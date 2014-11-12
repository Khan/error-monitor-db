"""A server that stores & retrieves error information from app logs."""
import json

import flask
import logging
import numpy
import redis
import scipy.stats

import models

ERROR_LEVELS = ["", "", "", "ERROR", "CRITICAL"]

app = flask.Flask("Khan Academy Error Monitor")
app.debug = True  # TODO(tom) STOPSHIP Take out before deploying to production

r = redis.StrictRedis(host='localhost', port=6379, db=0)


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
    verify_versions = verify_versions.split(",")

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
            significant_errors.append({
                "key": error["key"],
                "status": int(error["status"]),
                "level": ERROR_LEVELS[int(error["level"])],
                "message": error["title"],
                "minute": minute,
                "monitor_count": monitor_count,
                "expected_count": expected_count,
                "probability": probability
            })

    return json.dumps({
        "errors": significant_errors
    })


@app.route('/ping')
def ping():
    """Simple handler used to check if the server is running."""
    return flask.Response('pong', mimetype='text/plain')


if __name__ == "__main__":
    # Start the server running
    app.run(host="0.0.0.0", port=9340)
