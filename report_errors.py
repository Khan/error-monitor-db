#!/usr/bin/env python

"""A tool to send error information from the error-monitor-db to Slack/logs.

This tool is purposefully written not to use any of the
error-monitor-db internals, to show that it can be run from any
anywhere.
"""

import cgi
import collections
import datetime
import json
import logging
import re
import urllib2

import alertlib


# A list of error message to ignore (that is, to not alert about).
# These are errors that are either:
# a) beyond our control to fix (OOM errors); or
# b) known-broken and we want to fix them one day but don't know how to
#    fix them yet (problem out of order).
# Instead of alerting about such errors as they occur, we alert once
# a day with a summary of how often these errors are occurring.
#
# Each entry is either a string or a regexp.  A log-line matches if
# the full logline text contains any of the below as a substring.
_ALERT_BLACKLIST = [
    # OOM's, caused by really big ndb queries maybe?
    'Exceeded soft private memory limit',

    # We'd like to solve these, but don't have a good way how, so there's
    # no point in alerting about them.
    # TODO(csilvers): make sure we have good monitoring for them, instead.
    'The datastore operation timed out,',
    'The transaction could not be committed. Please try again',
    'Request was aborted after waiting too long to attempt to service',
    'server is not responding',

    # We see this a lot when trying to send UDP packets to graphite.
    # cf. https://enterprise.google.com/supportcenter/managecases#Case/0016000000QWp9w/4095721
    'ApplicationError: 4 Unknown error',

    # One day we'll figure out what causes this!
    'Problem out of order',
    'SAT problem out of order',

    # And likewise this.  Tom thinks it's just a user having multiple
    # browser windows open, but I think it's happening too often to be
    # just that.
    re.compile(r'client mastery task \(\d+\) is OLDER than server'),
]


def _matches_blacklist(logline):
    for b in _ALERT_BLACKLIST:
        if isinstance(b, basestring) and b in logline:
            return True
        if hasattr(b, 'search') and b.search(logline):     # regexp
            return True
    return False


def _fetch_error_json(hostport):
    url = 'http://%s/recent_errors' % hostport
    return json.load(urllib2.urlopen(url))


def _send_alert_to_bugtracker(error_info):
    error_name = 'Top Daily Error: ' + str(error_info.title)
    error_notes = ('*Frequent error (%d occurrences recently):*\n'
                   'https://www.khanacademy.org/devadmin/errors/%s\n'
                   '%s (%s)\n This is currently the most frequent error seen'
                   ' in the webapp logs' %
                   (error_info.count, error_info.key,
                    error_info.title, error_info.status))

    project_name = 'Learning Platform'
    alertlib.Alert(error_notes, summary=error_name, severity=logging.WARNING
                   ).send_to_bugtracker(project_name=project_name)


def _send_alerts_to_slack(slack_attachments, slack_channel):
    if slack_channel:
        (alertlib.Alert('', severity=logging.ERROR)
         .send_to_slack(slack_channel, attachments=slack_attachments))
    # We always send the alert to stdout as well
    for a in slack_attachments:
        print a['fallback']
    print '-' * 40 + '\n'


_ErrorInfo = collections.namedtuple("_ErrorInfo",
                                    ["key", "title", "status", "count",
                                     "dates_seen", "first_date_seen"])


def _slack_error_link(error_info):
    """Return a brief link to an error suitable for use in slack."""
    return ('<https://www.khanacademy.org/devadmin/errors/%s|%s> (%d)' %
            (error_info.key, error_info.key, error_info.count))


def _plaintext_error_link(error_info):
    """Return a brief plaintext link to an error."""
    return ('<https://www.khanacademy.org/devadmin/errors/%s> (%d)' %
            (error_info.key, error_info.count))


def _slack_error_attachment(error_info):
    """Return an attachment field with error information for use in slack."""
    return {
        'text': ('*Frequent error (%d occurrences in this date range):*\n'
                 '<https://www.khanacademy.org/devadmin/errors/%s|%s> (%s)' %
                 (error_info.count, error_info.key,
                  cgi.escape(error_info.title), error_info.status)),
        'fallback': ('Frequent error (%d occurrences in this date range):\n%s '
                     '<https://www.khanacademy.org/devadmin/errors/%s> (%s)' %
                     (error_info.count, cgi.escape(error_info.title),
                      error_info.key, error_info.status)),
        'color': 'danger',
        'mrkdwn_in': ['text'],
    }


def _slack_error_list(error_infos, error_type):
    """Return a slack attachment field with a list of errors.

    error_infos should be a list of errors.  error_type should be the type of
    errors we are displaying (e.g. "long-running errors" or "new errors since
    <date>").
    """
    title = ('%s %s (with frequency)' % (len(error_infos), error_type))
    return {
        'text': '*%s:*\n%s' % (
            title, ' ~ '.join(_slack_error_link(e) for e in error_infos)),
        'fallback': '%s:\n%s' % (
            title, ' ~ '.join(_plaintext_error_link(e) for e in error_infos)),
        'color': 'danger',
        'mrkdwn_in': ['text'],
    }


def _parse_error_info(error_dict, start_date, end_date):
    """Given a dict returned by the server, extract useful info from it.

    The return value is an _ErrorInfo.

    We include counts in [start_date, end_date): that is, errors in the
    end-date hour are *not* included.  This is so if the end-date is
    "now", we don't include counts from an incomplete hour, which could
    throw off analysis.  (That's not an issue now where errors are only
    imported to the server once an hour, but it could be eventually.)

    Note that first_date_seen is not just min(dates_seen): it is the
    first time this error was seen even if it was before start_date,
    while dates_seen is only dates between start_date and end_date.

    The start_date and end_date should be YYYYMMDD_HH.
    """
    dates_seen = set()     # to figure out the *actual* time range covered
    count = 0
    for record in error_dict['by_hour_and_version']:
        # Luckily we can compare YYYYMMDD_HH entries lexicographically!
        if start_date <= record['hour'] < end_date:
            count += int(record['count'])
            dates_seen.add(record['hour'])

    return _ErrorInfo(key=error_dict['error_def']['key'],
                      title=error_dict['error_def']['title'],
                      status=int(error_dict['error_def'].get('status') or 0),
                      count=count,
                      dates_seen=dates_seen,
                      first_date_seen=error_dict['first_seen'])


def _categorize_errors(errors, start_date, end_date):
    """Return a map from 'new/old/blacklist/all/whitelist' to _ErrorInfo."""
    # We break up all errors into 'new' (first seen since start_time),
    # 'old' (first seen before start_time), and 'blacklist'
    # (matches _ALERT_BLACKLIST).  We also have a few summary categories:
    # 'all' and 'whitelist' (not blacklist).
    categories = {'new': [], 'old': [], 'blacklist': [],
                  # These are summary categories: supersets of the ones above
                  'all': [], 'whitelist': []}

    for error in errors:
        error_info = _parse_error_info(error, start_date, end_date)
        # Ignore errors that we haven't seen in the last 24 hours.
        if error_info.count == 0:
            continue

        if _matches_blacklist(error_info.title):
            categories['blacklist'].append(error_info)
            categories['all'].append(error_info)
        elif error_info.first_date_seen >= start_date:
            categories['new'].append(error_info)
            categories['whitelist'].append(error_info)
            categories['all'].append(error_info)
        else:
            categories['old'].append(error_info)
            categories['whitelist'].append(error_info)
            categories['all'].append(error_info)

    # Now sort each of the categories by count (frequency).
    for error_list in categories.itervalues():
        error_list.sort(key=lambda e: e.count, reverse=True)

    return categories


def send_alerts_for_errors(hostport,
                           start_date, end_date, num_errors_to_highlight,
                           new_only,
                           slack_channel):
    """Process the error logs between start and end date and send a report.

    We always send the report to stdout.  If slack_channel is not None, we
    send the report there as well. We also send the top error to alertlib's
    bugtracker as a new task if it does not already exist.

    Arguments:
        hostport: the host:port where the error-monitor server is running
        start_date: when to start processing error info, as YYYMMDD_HH, in UTC
        end_date: when to end processing error info, as YYYMMDD_HH, in UTC
        num_errors_to_highlight: how many errors to print more detailed
            error information about.  For all other errors, we just print
            a brief (one-word) summary.  We always print the detailed info
            about the most frequent errors.
        new_only: if set, only report new errors: those whose first
            occurrence is on or after start_date
        slack_channel: the name of the slack room to send the error report to.
            Can be None, in which case we won't send to Slack.
    """
    error_json = _fetch_error_json(hostport)
    errors = error_json['errors']
    categories = _categorize_errors(errors, start_date, end_date)

    # Get the full summary information.
    full_count = sum(e.count for e in categories['all'])
    all_dates_seen = reduce(lambda x, y: x | y.dates_seen, categories['all'],
                            set())

    slack_pretext = ('Found *%d* unique errors (*%d* total) '
                     'between %s and %s (UTC)'
                     % (len(categories['all']), full_count,
                        min(all_dates_seen), max(all_dates_seen)))
    if min(all_dates_seen) != start_date:
        slack_pretext = (slack_pretext + '\n*WARNING:* requested error info '
                         'since %s, but we only have error info from %s' %
                         (start_date, min(all_dates_seen)))
    # TODO(csilvers): do a similar warning for end_date?  Harder since it's
    # non-inclusive.

    if new_only:
        categories['old'] = []
        categories['whitelist'] = categories['new']

    slack_attachments = []

    highlighted_errors = categories['whitelist'][:num_errors_to_highlight]
    for error_info in highlighted_errors:
        slack_attachments.append(_slack_error_attachment(error_info))

    continuing_msgs = [e for e in categories['old']
                       if e not in highlighted_errors]
    if continuing_msgs:
        error_type = "long-running errors"
        slack_attachments.append(_slack_error_list(continuing_msgs,
                                                   error_type))

    new_msgs = [e for e in categories['new']
                if e not in highlighted_errors]
    if new_msgs:
        error_type = "new errors since %s" % start_date
        slack_attachments.append(_slack_error_list(new_msgs, error_type))

    slack_attachments[0]['pretext'] = slack_pretext
    slack_attachments[0]['mrkdwn_in'].append('pretext')
    _send_alerts_to_slack(slack_attachments, slack_channel)

    # Send the top error to bugtracker as a new task. No new task will be
    # created if an unfinished one with the same name already exists (so
    # that multiple tasks for the same error are not created).
    if highlighted_errors:
        _send_alert_to_bugtracker(highlighted_errors[0])


if __name__ == "__main__":
    import argparse

    now = datetime.datetime.utcnow()
    one_day_ago = datetime.datetime.utcnow() - datetime.timedelta(days=1)
    now_str = now.strftime("%Y%m%d_%H")
    one_day_ago_str = one_day_ago.strftime("%Y%m%d_%H")

    parser = argparse.ArgumentParser()
    # TODO(csilvers): warn if start_date is more than 7 days ago.
    parser.add_argument("-d", "--start-date",
                        default=one_day_ago_str,
                        help=("Date (in UTC) to start processing error info, "
                              "as YYYYMMDD_HH. All output counts will include "
                              "errors from this hour. Default: %(default)s"))
    parser.add_argument("-e", "--end-date",
                        default=now_str,
                        help=("Date (in UTC) to stop processing error info, "
                              "as YYYYMMDD_HH. All output counts will include "
                              "errors before this hour. Default: %(default)s"))
    parser.add_argument("--host",
                        default="localhost:9340",
                        help=("Host where the error-monitor-db lives. "
                              "May include a port too. Default: %(default)s"))
    parser.add_argument("-S", "--slack", dest="slack",
                        help="Slack channel to send the error report to.")
    parser.add_argument('-n', '--num-errors-to-highlight', type=int,
                        default=3,
                        help=("How many errors to emit detailed error info "
                              "in our report, vs just summary info"))
    parser.add_argument('--new-only', action='store_true',
                        help=("If set, only report errors that appeared "
                              "for the first time since --start-date"))
    args = parser.parse_args()

    send_alerts_for_errors(args.host, args.start_date, args.end_date,
                           args.num_errors_to_highlight, args.new_only,
                           args.slack)
