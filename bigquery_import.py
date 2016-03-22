#!/usr/bin/env python

"""BigQuery-related routines for querying the AppEngine logs.

We already export all the AppEngine server logs to BigQuery once an hour,
into a dataset in the format "logs_hourly.requestlogs_YYYYMMDD_HH". On the
error monitor, we run a cron job that executes this file to scrape these logs
as they become available with a simple query (in `logs_from_bigquery`) that
just filters for log lines  of level ERROR or CRITICAL. The errors are then
stored in Redis by version and error key for later reference.

To see how errors are stored, see models.py.

There is a dependency here that are not included in the repo for security
reasons:

  client_secrets.json - Contains the Google AppEngine "Client ID for native
      application". To get this, go here and click "Download JSON":

  https://console.developers.google.com/project/124072386181/apiui/credential
"""
import datetime
import httplib2
import json
import logging
from optparse import OptionParser
import os
import pprint
import re
import time

import apiclient.discovery
import apiclient.errors

import oauth2client.client
import oauth2client.file
import oauth2client.tools

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
        # Try a few times because occasionally BigQuery returns early without
        # any response data
        for attempt in xrange(3):
            try:
                # Create a query statement and query request object
                query_data = {'query': sql}
                query_request = self.bigquery_service.jobs()

                # Make a call to the BigQuery API
                query_response = query_request.query(projectId=PROJECT_NUMBER,
                                                     body=query_data).execute()

            except apiclient.errors.HttpError as err:
                err_json = json.loads(err.content)
                # Traverse the unnecessarily complex JSON to check if the error
                # is simply that the table was not found.
                try:
                    code = err_json['error']['code']
                    message = err_json['error']['errors'][0]['message']
                    reason = err_json['error']['errors'][0]['reason']
                except (KeyError, IndexError):
                    raise UnknownBigQueryError(err_json)

                if reason == 'notFound':
                    raise TableNotFoundError()
                elif code >= 500:             # transient error, let's retry
                    pass
                elif (code == 403 and         # let's retry that too
                        message.startswith('Exceeded rate limits')):
                    pass                      # they *told* us to retry
                elif ('Please try again' in message or
                        'Retrying may solve the problem' in message or
                        'Backend Error' in message):
                    pass

                raise UnknownBigQueryError(err_json)

            except oauth2client.client.AccessTokenRefreshError:
                raise MissingBigQueryCredentialsError()

            if query_response.get('jobComplete', True) is False:
                # This happens occasionally with no additional information, so
                # for now just wait a bit and try again
                time.sleep(60)
                continue

            if 'rows' not in query_response:
                # Some other error happened that we didn't anticipate, so log
                # the response which hopefully includes some kind of error
                # message
                raise UnknownBigQueryError(query_response)

            return query_response['rows']

    def errors_from_bigquery(self, log_hour):
        """Retrieve logs for the specified hour from BigQuery.

        'log_hour' is the date portion of the request log dataset name, in the
        format YYYYMMDD_HH, in UTC time.

        If the logs have already been retrieved and the errors are in Redis,
        don't re-fetch the logs.

        In case of an error, raises one of the exceptions
        at the top of the file.

        Returns (keys of new errors, keys of continuing errors).
        If we've already processed these logs, returns (None, None).
        """
        new_keys = set()
        old_keys = set()
        lines = 0
        print "Fetching hourly errors for %s" % log_hour
        records = self.run_query(
            ('SELECT version_id, ip, resource, status, app_logs.level, '
             'app_logs.message, elog_url_route, module_id '
             'FROM [logs_hourly.requestlogs_%s]'
             'WHERE app_logs.level >= 3') % log_hour)

        for record in records:
            (
                version_id, ip, resource, status, level, message, route,
                module_id
            ) = [v['v'] for v in record['f']]

            # Never record errors for znd versions
            if not re.match(r'\d{6}-\d{4}-[0-9a-f]{12}', version_id):
                continue

            error_key, is_new = models.record_occurrence_from_errors(
                version_id, log_hour, status, level, resource, ip, route,
                module_id, message)

            if error_key:
                if is_new:
                    new_keys.add(error_key)
                else:
                    old_keys.add(error_key)

            lines += 1

        num_new = len(new_keys)
        num_old = len(old_keys)
        print ("Processed %d lines and found %d distinct errors: "
               "%d new, and %d continuing"
               % (lines, num_new + num_old, num_new, num_old))
        return (new_keys, old_keys - new_keys)

    def requests_from_bigquery(self, log_hour):
        print "Fetching hourly requests for %s" % log_hour
        records = self.run_query(
            ('SELECT COUNT(*), status, elog_url_route '
             'FROM [logs_hourly.requestlogs_%s] '
             'WHERE elog_url_route IS NOT NULL '
             'GROUP BY status, elog_url_route HAVING COUNT(*) > 1') % log_hour)

        for record in records:
            num_seen, status, route = [v['v'] for v in record['f']]
            models.record_occurrences_from_requests(log_hour, status,
                                                    route, num_seen)

        # Add any route/status combinations that didn't occur into the model.
        for route in models.get_routes():
            for status in models.get_statuses():
                if models.get_responses_count(route, status, log_hour) == 0:
                    models.record_occurrences_from_requests(log_hour, status,
                                                            route, 0)


def _urlize(error_key):
    return ('<a href="https://www.khanacademy.org/devadmin/errors/%s">%s</a>'
            % (error_key, error_key))


def import_logs(date_str):
    bq = BigQuery()
    for hour in xrange(0, 24):
        log_hour = "%s_%02d" % (date_str, hour)

        try:
            if models.check_log_data_received(log_hour):
                continue

            bq.requests_from_bigquery(log_hour)
            # bg.errors_from_bigquery(log_hour)
            models.record_log_data_received(log_hour)

        except TableNotFoundError:
            # Not really an error, so we won't emit it to stderr.
            print "BigQuery table for %s is not available yet." % log_hour
            break

        except UnknownBigQueryError, e:
            logging.fatal("BigQuery error: %s" % pprint.pformat(e.error))

        except MissingBigQueryCredentialsError:
            logging.fatal("Credentials have been revoked or expired, "
                          "please re-run the application manually to "
                          "re-authorize")

    print "Done fetching logs."


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
    (options, args) = parser.parse_args()

    import_logs(options.date_str)
