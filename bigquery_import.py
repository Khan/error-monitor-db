#!/usr/bin/env python
# TODO(colin): fix these lint errors (http://pep8.readthedocs.io/en/release-1.7.x/intro.html#error-codes)
# pep8-disable:E131

"""BigQuery-related routines for querying the AppEngine logs.

We already export all the AppEngine server logs to BigQuery once an hour,
into a dataset in the format "logs_hourly.requestlogs_YYYYMMDD_HH". On the
error monitor, we run a cron job that executes this file to scrape these logs
as they become available with a simple query (in `errors_from_bigquery`) that
just filters for log lines  of level ERROR or CRITICAL. The errors are then
stored in Redis by version and error key for later reference.

We also save the number of HTTP response codes received on specific routes
to be used in the detection of anomalies. We first try to pull in hourly logs
(`requests_from_bigquery`), and if those aren't available we try to pull in
daily logs (`daily_requests_from_bigquery`).

To see how errors and requests are stored, see models.py.

This script authenticates using a service account. You need service account
credentials to run it: https://phabricator.khanacademy.org/K283
When you've downloaded that json file, pass the credentials file location as an
env variable:
GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json bigquery_import.py ...
"""
import calendar
import datetime
import httplib2
import json
import logging
from optparse import OptionParser
import pprint
import re

from google.cloud import bigquery
from google.cloud import exceptions

import models

# The AppEngine project id for 'khan' (NOT 'khan-academy' for historical
# reasons) where we store our BigQuery tables.
PROJECT_ID = 'khanacademy.org:deductive-jet-827'
LOG_COMPLETION_URL_BASE = (
    'https://www.khanacademy.org/api/internal/logs/completed')


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
        self.bigquery_service = bigquery.Client(project=PROJECT_ID)

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
            config = bigquery.job.QueryJobConfig()
            config.use_legacy_sql = True
            query_job = self.bigquery_service.query(
                sql, job_config=config)
            # This will block and wait for the job to complete
            rows = query_job.result()

        except exceptions.GoogleCloudError as err:
            # TODO(colin): it's not clear to me what the format of this
            # error message is from the documentation; do more
            # sophisticated parsing here?
            raise UnknownBigQueryError('%s: %s' % (err.message, err.errors))
        return rows

    def errors_from_bigquery(self, log_hour):
        """Retrieve errors for the specified hour from BigQuery.

        'log_hour' is the date portion of the request log dataset name, in the
        format YYYYMMDD_HH, in UTC time.

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
            # Never record errors for znd versions.
            # `version_id` may also be None for some logs from the service
            # bridge on managed VMs.  We ignore these because most of the other
            # fields are None too, and we can't do much with them.
            if (record.version_id is None or
                    not re.match(r'\d{6}-\d{4}-[0-9a-f]{12}',
                                 record.version_id)):
                continue

            error_key, is_new = models.record_occurrence_from_errors(
                record.version_id, log_hour, record.status,
                record.app_logs_level, record.resource, record.ip,
                record.elog_url_route, record.module_id,
                record.app_logs_message)

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
        """Retrieve requests for the specified hour from BigQuery.

        'log_hour' is the date portion of the request log dataset name, in the
        format YYYYMMDD_HH, in UTC time.
        """
        print "Fetching hourly requests for %s" % log_hour
        records = self.run_query(
            ('SELECT COUNT(*) AS num_seen, status, elog_url_route '
             'FROM [logs_hourly.requestlogs_%s] '
             'WHERE elog_url_route IS NOT NULL '
             'GROUP BY status, elog_url_route HAVING COUNT(*) > 0') % log_hour)

        for record in records:
            models.record_occurrences_from_requests(
                log_hour, record.status, record.elog_url_route,
                record.num_seen)

    def daily_requests_from_bigquery(self, date):
        """Retrieve requests for the specified day from BigQuery.

        'date' is the date portion of the request log dataset name, in the
        format YYYYMMDD, in UTC time.
        """
        print "Fetching daily requests for %s" % date
        records = self.run_query(
            ('SELECT COUNT(*) AS num_seen, '
             'HOUR(start_time_timestamp) AS log_hour, '
             'status, elog_url_route '
             'FROM [logs.requestlogs_%s] '
             'WHERE elog_url_route IS NOT NULL '
             'GROUP BY log_hour, status, elog_url_route '
             'HAVING COUNT(*) > 0') % date)

        for record in records:
            log_hour = "%s_%02d" % (date, int(record.log_hour))
            models.record_occurrences_from_requests(
                log_hour, record.status, record.elog_url_route,
                record.num_seen)


def _urlize(error_key):
    return ('<a href="https://www.khanacademy.org/devadmin/errors/%s">%s</a>'
            % (error_key, error_key))


def _log_hour_is_complete(log_hour):
    """Check with the webapp log completion API if the logs are completed.

    Since logs stream directly into the hourly tables, we use a heuristic to
    check if we're pretty sure the logs for the hour have all arrived.

    If they're not complete, we hold off on ingesting this hour.
    """
    # Add an hour because the completion timestamps we need to supply
    # correspond to the end of the interval.
    log_dt = (
        datetime.datetime.strptime(log_hour, '%Y%m%d_%H') +
        datetime.timedelta(hours=1))
    timestamp = calendar.timegm(log_dt.utctimetuple())
    completion_url = '%s?end_time=%s' % (
        LOG_COMPLETION_URL_BASE,
        timestamp)
    resp, content = httplib2.Http().request(completion_url, method='GET')
    status = resp['status']
    if status != '200':
        logging.error(
            'Unexpectedly got status %s when fetching log completion '
            'from %s.  Check the appengine logs for more info.' % (
                status, completion_url))
        return False
    else:
        return json.loads(content)


def import_logs(date_str):
    """Import both the request and error logs from bigquery.

    If the logs have already been retrieved and the is in Redis,
    don't re-fetch the logs.
    """
    bq = BigQuery()

    for hour in xrange(0, 24):
        log_hour = "%s_%02d" % (date_str, hour)

        try:
            if models.check_log_data_received(log_hour):
                continue

            if not _log_hour_is_complete(log_hour):
                print "BigQuery table for %s is not complete yet." % log_hour
                continue

            bq.requests_from_bigquery(log_hour)
            bq.errors_from_bigquery(log_hour)
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


def import_daily_logs(date_str):
    """Like import_logs, but use daily instead of hourly tables.

    Useful if you're importing historical data from further in the past than
    we keep hourly log tables.

    Note that we don't check that this day's logs are complete using webapp's
    log completion API; this is intended for manual backfilling use, and we
    assume you know what you're doing.
    """
    bq = BigQuery()
    if not models.check_log_data_received(date_str + "_00"):
        print "Trying to fetch daily logs."
        # We aren't partially through fetching hourly request logs so try
        # to fetch all daily request logs in one go.
        try:
            # We don't fetch error logs since this case should only happen
            # when logs are too old to be counted by the error monitoring.
            bq.daily_requests_from_bigquery(date_str)
            for hour in xrange(24):
                # Record successful receipt of all hours that day.
                log_hour = "%s_%02d" % (date_str, hour)
                models.record_log_data_received(log_hour)

        except TableNotFoundError:
            print "BigQuery table for %s is not available yet." % date_str

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
    parser.add_option("--use-daily-tables", dest="use_daily",
                      default=False, action="store_true",
                      help="Use the daily log tables to import logs, instead "
                      "of the hourly ones. This will happen automatically if "
                      "we're loading a date more than 7 days ago. (Within 7 "
                      "days, you might want to use this to backfill data "
                      "quickly.)")
    (options, args) = parser.parse_args()

    # If we're loading logs for more than 7 days ago, we won't have hourly
    # tables, so use daily ones instead.
    days_ago = (datetime.datetime.utcnow() -
                datetime.datetime.strptime(options.date_str, '%Y%m%d')).days

    if options.use_daily or days_ago > 7:
        import_daily_logs(options.date_str)
    else:
        import_logs(options.date_str)
