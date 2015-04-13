"""A server that stores & retrieves error information from app logs."""
import argparse
import json

import flask
import logging
import logging.handlers
import numpy
import redis
import scipy.stats

import models

app = flask.Flask("Khan Academy Error Monitor")

r = redis.StrictRedis(host='localhost', port=6379, db=0)


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

    if len(historical_counts) == 1:
        # We only have one data point, so do a simple threshold check
        return (historical_counts[0],
                1 if recent_count > historical_counts[0] else 0)

    counts = numpy.array(historical_counts)
    mean = numpy.mean(counts)

    if recent_count < mean:
        # If the error count went down, we don't care about the probability
        return (mean, 0)

    # Run a simple z-test by calculating the standard deviation and z-score
    stdev = numpy.std(counts)

    if stdev < 1:
        # Avoid a division by zero error
        return (mean, 1 if recent_count > mean else 0)

    pvalue = (recent_count - mean) / stdev
    zscore = scipy.stats.norm.cdf(pvalue)

    return (mean, zscore)


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
        logging.warning("MONITORING ERROR IN %s: %s (%d)" % (
                version_id, error["title"], monitor_count))

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


if not app.debug:
    file_handler = logging.handlers.RotatingFileHandler(
        '/home/ubuntu/logs/error-monitor-db-app.log',
        maxBytes=1024 * 1024, backupCount=5)
    file_handler.setLevel(logging.WARNING)
    app.logger.addHandler(file_handler)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Serve the error-monitor-db.')

    parser.add_argument('--port', type=int, default=9340,
        help='HTTP listen port.')

    parser.add_argument('--debug', action='store_true', default=False,
        help='Enable debug mode.')

    args = parser.parse_args()

    # Start the server running
    app.debug = args.debug
    app.run(host="0.0.0.0", port=args.port)
