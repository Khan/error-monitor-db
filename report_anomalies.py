#!/usr/bin/env python

"""A tool to send anomaly information from the error-monitor-db to Slack/logs.

This tool is purposefully written not to use any of the
error-monitor-db internals, to show that it can be run from any
anywhere.
"""

import datetime
import json
import logging
import urllib2

import alertlib


def _fetch_anomaly_json(hostport, date):
    url = 'http://%s/anomalies/%s' % (hostport, date)
    return json.load(urllib2.urlopen(url))["anomalies"]


def _send_alerts(slack_attachments, slack_channel):
    """Send alerts to Slack."""
    if slack_channel:
        (alertlib.Alert('', severity=logging.ERROR)
         .send_to_slack(slack_channel, attachments=slack_attachments))
    # We always send the alert to stdout as well
    for a in slack_attachments:
        print a['fallback']
    print '-' * 40 + '\n'


def _slack_anomaly_attachment(anomaly_info):
    """Return an attachment field with anomaly information for use in slack."""
    return {
        'text': ('*Anomaly on route %s with status code %d*\n'
                 'Got %d requests which gave an anomaly score of %lf.' %
                 (anomaly_info["route"], anomaly_info["status"],
                  anomaly_info["count"], anomaly_info["anomaly_score"])),
        'fallback': ('Anomaly on route %s with status code %d\n'
                     'Got %d requests which gave an anomaly score of %lf.' %
                     (anomaly_info["route"], anomaly_info["status"],
                      anomaly_info["count"], anomaly_info["anomaly_score"])),
        'color': 'danger',
        'mrkdwn_in': ['text'],
    }


def send_alerts_for_anomalies(hostport, date, slack_channel):
    anomalies = _fetch_anomaly_json(hostport, date)
    if not anomalies:
        print "No anomalies found at %s UTC." % date
        return

    slack_pretext = 'Found %d anomalies at %s UTC.\n' % (len(anomalies), date)

    slack_attachments = []
    for anomaly in anomalies:
        slack_attachments.append(_slack_anomaly_attachment(anomaly))

    slack_attachments[0]['pretext'] = slack_pretext
    slack_attachments[0]['mrkdwn_in'].append('pretext')
    _send_alerts(slack_attachments, slack_channel)


if __name__ == "__main__":
    import argparse

    now = datetime.datetime.utcnow()
    one_day_ago = datetime.datetime.utcnow() - datetime.timedelta(days=1)
    now_str = now.strftime("%Y%m%d_%H")
    one_day_ago_str = one_day_ago.strftime("%Y%m%d_%H")

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--date",
                        default=now_str,
                        help=("Date (in UTC) to start processing error info, "
                              "as YYYYMMDD_HH. All output counts will include "
                              "errors from this hour. Default: %(default)s"))
    parser.add_argument("--host",
                        default="localhost:9340",
                        help=("Host where the error-monitor-db lives. "
                              "May include a port too. Default: %(default)s"))
    parser.add_argument("-S", "--slack", dest="slack",
                        help="Slack channel to send the error report to.")
    args = parser.parse_args()

    send_alerts_for_anomalies(args.host, args.date, args.slack)
