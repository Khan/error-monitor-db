# TODO(None): fix these lint errors (http://pep8.readthedocs.io/en/release-1.7.x/intro.html#error-codes)
# pep8-disable:E128
"""A server that stores & retrieves error information from app logs."""
import argparse
import decimal
import json
import re

import flask
import logging
import logging.handlers
import math
import numpy
import redis

import models

app = flask.Flask("Khan Academy Error Monitor")

r = redis.StrictRedis(host='localhost', port=6379, db=0)

HTTP_OK_CODE = 200

# A list of tuples (error, threshold).  We blacklist these errors unless the
# number of errors per minute is greater than the threshold.  This lets us
# blacklist errors that we don't care about a somewhat heightened level of, but
# still get alerted if we see a massive spike.  This is separate from
# report_errors.py's list because here we only care about errors that may be
# heightened on deploy, rather than errors we see a lot in general, and we may
# want to blacklist errors that are bad in general but not caused by deploys.
# The thresholds are selected somewhat arbitrarily.
# TODO(benkraft): consider switching to a threshold that will scale on the
# previous error rate or the number of requests.  It would be trickier to get
# right but might be more accurate.
_ERROR_BLACKLIST_THRESHOLDS = [
    # OOMs, which often spike to 10-20/min on deploys.  A massive spike could
    # still mean a problem, such as if someone did `range(10**12)` in
    # `main.py`.
    ('Exceeded soft private memory limit', 100),
    # Requests that timed out before getting sent to an instance.  These are
    # bad, but they are probably a matter of insufficient priming or an
    # unrelated issue and not the deployer's fault, unless there are very large
    # numbers of them.
    ('Request was aborted after waiting too long', 800),
    # Failed send to graphite.  Not sure why these spike on deploy sometimes,
    # but they do.  They're a 200 and in the worst case just mean we didn't
    # send logs to graphite.
    ('ApplicationError: 4 Unknown error', 100),
    # Memcache set errors; these likely indicate some contention, and mean
    # we're wasting work, but are 200s.  They tend to increase a bit on
    # deploys.  See https://app.asana.com/0/31965416896056/182669804238387.
    (re.compile(r'google\.appengine\.api\.memcache set failed on chunk for '
                r'[^ ]* user_models.get_students_data'), 100),
    # Request response timeout mostly coming from /_ah/queue/deferred; Like
    # the request aborted above, these are increased significantly due to the
    # deploy process routing traffic and are not likely due to the change.
    ('Process terminated because the request deadline was exceeded', 100),
    # Request response timeouts; Similar to above, these spike during deploy
    # but are not the result of the change but of routing traffic. These
    # errors usually spike to < 50, so more than that may be an issue.
    ('DeadlineExceededError: The overall deadline for responding', 50),
]


def poisson_cdf(actual, mean):
    """Return p(draw <= actual) when drawing from a poisson distribution.

    That is, we return the probability that we see actual or anything
    smaller in a random measurement of a variable with a poisson
    distribution with mean mean.

    (Stolen from statistics_util.py in webapp to avoid a scipy dependency.)

    Arguments:
       mean: a float.
    """
    if actual < 0:
        return 0.0

    # We use Decimal so that long periods and high numbers of
    # reports work -- a mean of 746 or higher would cause a zero
    # to propagate and make us report a probability of 0 even
    # if the actual probability was almost 1.
    mean = decimal.Decimal(mean)

    cum_prob = decimal.Decimal(0)

    p = (-mean).exp()
    cum_prob += p
    for i in xrange(actual):
        # We calculate the probability of each lesser value
        # individually, and sum as we go.
        p *= mean
        p /= i + 1
        cum_prob += p

    return float(cum_prob)


def _matches_blacklist(logline, count):
    for error, threshold in _ERROR_BLACKLIST_THRESHOLDS:
        if isinstance(error, basestring):
            if error in logline and count <= threshold:
                return True
        else:  # it's a regex
            if error.search(logline) and count <= threshold:
                return True
    return False


def _version_sort_key(version):
    """HACK: sort versions so that Dec '14 sorts before Jan '15.

    This is necessary because we don't yet store the year in the version name
    so 1231-* needs to be hard-coded to sort before 0101-*.
    TODO(tom): Take this out once we store the 2-digit year in the version
    name.
    """
    return '14' + version if version.startswith('12') else '15' + version


def _count_is_elevated_probability(historical_counts, recent_count):
    """Give the probability recent_count is elevated over the norm.

    We are given a collection of recent counts, each over a 1-minute time
    frame, and must decide how likely the new count is to be within a normal
    distribution represented by the historical counts.

    Arguments:
       historical_count: a list of the number of errors seen in each time
           window in 'the past'.
       recent_count: the number of errors seen in 'the present'.

    Returns:
       A pair: the expected number of errors we would have seen this period,
          and the probability that the number of errors we actually saw
          is actually higher than the expected number, both as floats.
    """
    if not historical_counts:
        # We don't have any history, so we can't make any guesses
        return (0, 0)

    counts = numpy.array(historical_counts)
    mean = numpy.mean(counts)

    if recent_count < mean:
        # If the error count went down, we don't care about the probability
        return (mean, 0)

    mean = max(mean, 1)
    cdf = poisson_cdf(int(math.floor(recent_count)), mean)
    return (mean, cdf)


@app.route("/monitor", methods=["post"])
def monitor():
    """Accept a snapshot of AppEngine error logs and record them in Redis.

    Each snapshot contains error logs that were emitted during a particular
    60-second time window after monitoring begins, so we can do an
    apples-to-apples comparison with the same time window after a different
    deploy. We may receive data for the same minute multiple times, for
    instance in 3 updates of 20-second time slices for a single minute. We
    append the error instances to the same database each time.

    Takes a JSON object with the following required fields:

    version: The identifier of the version we are monitoring, e.g.
             '0810-0511-eef125daa7'

    minute:  The number of minutes that have elapsed since we started
             monitoring, so we can do an apples-apples comparison between
             subsequently monitored versions

    logs:    A list of log records from a short time window (< 1 min) that
             occurred on the version we're monitoring.
    """
    # TODO(tom) Secret key for security?
    # Fetch the request parameters
    params = flask.request.get_json()
    if not params:
        return "Invalid parameters", 400

    error_logs = params['logs']
    minute = params['minute']
    version = params['version']

    if error_logs is None or minute is None or version is None:
        return "Invalid parameters", 400

    for log in error_logs:
        models.record_occurrence_during_monitoring(version, minute,
            str(log['status']), str(log['level']), log['resource'], log['ip'],
            log['route'], log['module_id'], log['message'])

    # Track that we've seen at least some logs from this GAE version and minute
    models.record_monitoring_data_received(version, minute)

    return "OK"


@app.route("/errors/<version_id>/monitor/<int:minute>", methods=["get"])
def monitor_results(version_id, minute):
    """Fetch monitoring results for one minute of monitoring.

    This handler assumes that errors logs for the specified minute have been
    posted to the /monitor handler previously. The error counts for that
    timeframe are compared with the same timeframe in the specified prior
    versions (assuming we have data for them) to determine whether any new
    errors have appeared or whether existing errors are occurring more
    frequently in the new version.

    verify_versions: A comma-separated list of GAE version names to search when
        looking for prior instances of errors in the current version. These
        are only meaningful if we have previously recorded monitoring data
        under those version names using the /monitor route.
    """
    # TODO(tom) Secret key for security?
    verify_versions = flask.request.args.get('verify_versions')
    if not verify_versions:
        return "Invalid parameters", 400

    # Parse verify_versions and skip any versions we haven't actually received
    # log data for
    orig_versions = verify_versions.split(",")
    verify_versions = [
            v for v in orig_versions
            if models.check_monitoring_data_received(v, minute)]

    ignored_versions = set(orig_versions) - set(verify_versions)
    if ignored_versions:
        logging.warning("Ignoring versions with no data for minute %d: %s" %
                (minute, ignored_versions))

    # Get all the previous error counts in the versions we're verifying against
    version_counts_by_key = {}
    for version in verify_versions:
        version_counts_by_key[version] = {
            error["key"]: count
            for error, count in (
                models.get_monitoring_errors(version, minute))
        }

    # Track significant (new or unexpectedly frequent) errors
    significant_errors = []
    errors = models.get_monitoring_errors(version_id, minute)

    for error, monitor_count in errors:
        # Warn about the error even if it's blacklisted.
        logging.warning("MONITORING ERROR IN %s: %s (%d)" % (
                version_id, error["title"], monitor_count))

        if _matches_blacklist(error["title"], monitor_count):
            continue

        # Get the counts for this error in the same minute of the reference
        # version monitoring histories
        version_counts = [
            version_counts_by_key[version].get(error["key"], 0)
            for version in verify_versions]

        # Calculate the likelihood the current count is significantly above the
        # expected amount based on the history
        (expected_count, probability) = _count_is_elevated_probability(
                version_counts, monitor_count)

        if probability >= 0.9995:
            if monitor_count == 1:
                # An error that only occurs once is probably a fluctuation
                # in the space-time continuum.  Just ignore it.
                logging.warning("Not reporting error; only occurs once.")
                continue

            if monitor_count < 5:
                # Special-case for really infrequent errors! Only error if we
                # haven't seen this error before in *any* minute of a previous
                # deploy or in the BigQuery logs for one of the known good
                # versions. Otherwise this is a known low-frequency error and
                # will just look like spam
                error_info = models.get_error_summary_info(error["key"])
                error_versions = error_info["versions"].keys()

                if any((version in error_versions or
                        ("MON_%s" % version) in error_versions)
                       for version in orig_versions):
                    # Don't error on low-frequency errors we've seen before
                    logging.warning("Not reporting error; too infrequent.")
                    continue

            significant_errors.append({
                "key": error["key"],
                "status": int(error["status"]),
                "level": models.ERROR_LEVELS[int(error["level"])],
                "message": error["title"],
                "minute": minute,
                "monitor_count": monitor_count,
                "expected_count": expected_count,
                "probability": probability
            })

    return json.dumps({
        "errors": significant_errors
    })


@app.route("/recent_errors", methods=["get"])
def view_recent_errors():
    """Summary information for all errors seen in the past week.

    See `get_error_summary_info` for more information.
    """
    errors = sorted(
        [models.get_error_summary_info(error_key)
         for error_key in models.get_error_keys()],
        key=lambda error: error["count"],
        reverse=True)

    return json.dumps({
        "errors": errors
    })


@app.route("/version_errors/<version>", methods=["get"])
def view_version_errors(version):
    """Summary information for all errors seen in the specified version.

    See `get_error_summary_info` for more information.
    """
    errors = sorted(
        filter(
          lambda x: x is not None,
          [models.get_error_summary_info(error_key)
           for error_key in models.get_error_keys_by_version(version)]),
        key=lambda error: error["count"],
        reverse=True)

    return json.dumps({
        "errors": errors
    })


@app.route("/error/<error_key>", methods=["get"])
def view_error(error_key):
    """Summary information for a single error.

    See `get_error_summary_info` and `get_error_extended_information` for more
    information. Extended error information is only retrieved for the latest
    GAE version this error occurred on.
    """
    info = models.get_error_summary_info(error_key)
    if not info:
        return "Error not found", 404

    # Get latest version and return route/stack information for that version
    version = sorted(info["versions"].keys(), key=_version_sort_key)[-1]
    info["routes"] = models.get_error_extended_information(version, error_key)

    return json.dumps(info)


@app.route('/ping')
def ping():
    """Simple handler used to check if the server is running.

    This will return an error if we cannot connect to Redis.
    """
    if not models.can_connect():
        return "ERROR Cannot connect to Redis instance.", 500
    return flask.Response('pong', mimetype='text/plain')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Serve the error-monitor-db.')

    parser.add_argument('--port', type=int, default=9340,
        help='HTTP listen port.')

    parser.add_argument('--debug', action='store_true', default=False,
        help='Enable debug mode.')

    args = parser.parse_args()

    # Start the server running
    app.debug = args.debug

    if not app.debug:
        file_handler = logging.handlers.RotatingFileHandler(
            '/var/log/error-monitor-db-app.log',
            maxBytes=1024 * 1024, backupCount=5)
        file_handler.setLevel(logging.WARNING)
        app.logger.addHandler(file_handler)

    app.run(host="0.0.0.0", port=args.port)
