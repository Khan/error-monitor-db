"""BigQuery-related routines for querying the AppEngine logs.

We already export all the AppEngine server logs to BigQuery once an hour,
into a dataset in the format "logs_hourly.requestlogs_YYYYMMDD_HH". On the
error monitor, we run a cron job that executes this file to scrape these logs
as they become available with a simple query (in `logs_from_bigquery`) that
just filters for log lines  of level ERROR or CRITICAL. The errors are then
stored in Redis by version and error key for later reference.

To see how errors are stored, see models.py.

There are two dependencies here that are not included in the repo for security
reasons:

  client_secrets.json - Contains the Google AppEngine "Client ID for native
      application". To get this, go here and click "Download JSON":

  https://console.developers.google.com/project/124072386181/apiui/credential

  secrets.py - Contains the HipChat token that alertlib relies on to send
      notifications.
"""
import datetime
import httplib2
import json
import logging
from optparse import OptionParser
import os
import pprint
import re
import sys

import apiclient.discovery
import apiclient.errors

import oauth2client.client
import oauth2client.client
import oauth2client.file
import oauth2client.tools

import alertlib
import models

# The AppEngine project number for 'khan' (NOT 'khan-academy' for historical
# reasons) where we store our BigQuery tables.
PROJECT_NUMBER = '124072386181'


class TableNotFoundError(Exception):
    pass


class UnknownBigQueryError(Exception):
    def __init__(self, error_json):
        self.error = error_json


class MissingBigQueryCredentialsError(Exception):
    pass


class BigQuery(object):
    def __init__(self):
        """Initialize the BigQuery client, making sure we are authorized."""
        dir = os.path.dirname(os.path.realpath(__file__))
        flow = oauth2client.client.flow_from_clientsecrets(
            '%s/client_secrets.json' % dir,
            scope='https://www.googleapis.com/auth/bigquery')
        storage = oauth2client.file.Storage(
            '%s/bigquery_credentials.dat' % dir)
        credentials = storage.get()

        if credentials is None or credentials.invalid:
            # Run oauth2 flow with default arguments.
            credentials = oauth2client.tools.run_flow(
                flow, storage,
                oauth2client.tools.argparser.parse_args(
                    ["--noauth_local_webserver"]))

        http = httplib2.Http()
        http = credentials.authorize(http)

        self.bigquery_service = apiclient.discovery.build(
            'bigquery', 'v2', http=http)

    def run_query(self, sql):
        """A utility to execute a query on BigQuery.

        Returns an array of records or None. The useful information in each
        record is the list of fields 'f', each of which has a value 'v':

            [
                # Record
                {
                    "f": [
                        {"v": <value of column 1>},
                        {"v": <value of column 2>},
                        ...
                    ]
                }
            ]
        """
        try:
            # Create a query statement and query request object
            query_data = {'query': sql}
            query_request = self.bigquery_service.jobs()

            # Make a call to the BigQuery API
            query_response = query_request.query(projectId=PROJECT_NUMBER,
                                                 body=query_data).execute()

        except apiclient.errors.HttpError as err:
            err_json = json.loads(err.content)
            # Traverse the unnecessarily complex JSON to check if the error is
            # simply that the table was not found.
            if ((err_json.get("error", {}).get("errors", []) or [{}])[0]
                    .get("reason", "") == "notFound"):
                raise TableNotFoundError()

            raise UnknownBigQueryError(err_json)

        except oauth2client.client.AccessTokenRefreshError:
            raise MissingBigQueryCredentialsError()

        return query_response['rows']

    def logs_from_bigquery(self, log_hour):
        """Retrieve logs for the specified hour from BigQuery.

        'log_hour' is the date portion of the request log dataset name, in the
        format YYYYMMDD_HH, in UTC time.

        If the logs have already been retrieved and the errors are in Redis,
        don't re-fetch the logs.

        Returns a set of all the unique error keys seen as well as a set of
        the unique *new* error keys (errors that were not seen before this
        hour in the logs). In case of an error, raises one of the exceptions
        at the top of the file.
        """
        if models.check_log_data_received(log_hour):
            return set(), set()

        lines = 0
        error_keys = set()
        new_errors = set()
        print "Fetching hourly logs for %s" % log_hour
        records = self.run_query(
            ('SELECT version_id, ip, resource, status, app_logs.level, '
             'app_logs.message, elog_url_route, module_id '
             'FROM [logs_hourly.requestlogs_%s] WHERE '
             'app_logs.level >= 3') % log_hour)

        for record in records:
            (
                version_id, ip, resource, status, level, message, route,
                module_id
            ) = [v['v'] for v in record['f']]

            # Never record errors for znd versions
            if not re.match(r'\d{4}-\d{4}-[0-9a-f]{12}', version_id):
                continue

            error_key, is_new = models.record_occurrence_from_logs(
                version_id, log_hour, status, level, resource, ip, route,
                module_id, message)

            if error_key:
                error_keys.add(error_key)

                if is_new:
                    new_errors.add(error_key)

            lines += 1

        print "Processed %d lines and found %d errors (%d new)." % (
            lines, len(error_keys), len(new_errors))
        models.record_log_data_received(log_hour)
        return error_keys, new_errors


if __name__ == "__main__":
    # This will be called from a cron job. It will try to fetch each hour's
    # worth of logs for a given day until it fails (because of error or
    # because that hour is not available in BigQuery yet) and then stops.
    # Since we won't re-fetch the same hour twice, this can be called as often
    # as we like.
    parser = OptionParser()
    parser.add_option("-d", "--date", dest="date_str",
                      default=datetime.datetime.utcnow().strftime("%Y%m%d"),
                      help="Date (in UTC) to import logs for, in format "
                            "YYYYMMDD. If omitted, use today's date.")
    parser.add_option("-H", "--hipchat", dest="hipchat",
                      help="Hipchat room to notify of new errors.")
    (options, args) = parser.parse_args()

    bq = BigQuery()
    for hour in xrange(0, 24):
        log_hour = "%s_%02d" % (options.date_str, hour)

        try:
            error_keys, new_errors = bq.logs_from_bigquery(log_hour)

        except TableNotFoundError:
            print "BigQuery table for %s is not available yet." % log_hour
            break

        except UnknownBigQueryError, e:
            print "BigQuery error:"
            pprint.pprint(e.error)
            # Return an error code so that cron will broadcast the message
            sys.exit(1)

        except MissingBigQueryCredentialsError:
            print ("Credentials have been revoked or expired, please re-run"
                   "the application to re-authorize")
            # Return an error code so that cron will broadcast the message
            sys.exit(1)

        if new_errors and options.hipchat:
            for error_key in new_errors:
                info = models.get_error_summary_info(error_key)
                alert = alertlib.Alert(
                    'New error found in app logs at hour %s: '
                    '%s (%s) %s\nFor details see https://www.khanacademy.org/'
                    'devadmin/errors/%s' % (
                        log_hour,
                        models.ERROR_LEVELS[int(info["error_def"]["level"])],
                        info["error_def"]["status"],
                        info["error_def"]["title"],
                        error_key
                    ), severity=logging.ERROR).send_to_hipchat(options.hipchat)

    print "Done fetching logs."


