"""R translation functions for anomaly detection.

This module is little more than a set of functions to allow multiprocess
access to the R robust anomaly detection (RAD) code by Netflix.

More details on the implementation of RAD can be found at the following
Github repository:
https://github.com/Netflix/Surus

The following paper provides details on the method underlying RAD, entitled
robust principle component analysis (RPCA):
https://statweb.stanford.edu/~candes/papers/RobustPCA.pdf
"""

import multiprocessing

import rpy2.rinterface
import rpy2.robjects as robjects

import models
import report_anomalies

robjects.r('suppressMessages(require(RAD))')

# See Netflix/Surus/resources/R/RAD/R/anomaly_detection.R for
# details on the R RPCA function.
r_rpca = robjects.r('AnomalyDetection.rpca')

NUM_HOURS_PER_WEEK = 7 * 24


def rpca(time_series, frequency):
    """A simple function that calls into the RPCA R code.

    Returns a list of sparse outliers.
    """
    # Convert our time series to work with R.
    time_series = robjects.FloatVector(time_series)

    try:
        anomalies = r_rpca(time_series, frequency=frequency)[2]
    except rpy2.rinterface.RRuntimeError:
        # The time series was too small to detect anomalies.
        anomalies = len(time_series) * [0.0]

    return list(anomalies)


def find_anomalies_on_routes(log_hour, routes):
    """Find any unusual 200 request counts on the given routes at an hour.

    'log_hour' is a timestamp formatted as "YYYYMMDD_HH" in UTC.
    'routes' is a list of all routes that are to be inspected.

    The function returns an array of floats where a positive number indicates
    an unusually high number of 200 responses and a negative number represents
    an unusually low number. The higher the magnitude, the more pronounced the
    anomaly.
    """
    anomalies = multiprocessing.Array('d', len(routes), lock=True)

    def check_anomaly(index):
        hours_seen, responses_count = models.get_hourly_responses_count(
            routes[index], report_anomalies.HTTP_OK_CODE)

        # Ensure that the time series is divisible by the frequency by
        # cutting off the first few elements.
        hours_seen = hours_seen[len(hours_seen) %
                                NUM_HOURS_PER_WEEK:]
        responses_count = responses_count[len(responses_count) %
                                          NUM_HOURS_PER_WEEK:]

        log_hour_index = -1
        for i in xrange(len(hours_seen)):
            if hours_seen[i] == log_hour:
                log_hour_index = i

        if log_hour_index == -1 or log_hour_index == 0:
            # Either the log hour wasn't recorded or it was the first
            # request so just skip it.
            anomalies[index] = 0
            return

        anomaly_scores = rpca(responses_count, NUM_HOURS_PER_WEEK)
        if anomaly_scores[log_hour_index - 1] != 0:
            anomalies[index] = anomaly_scores[log_hour_index - 1]

    # We fork new processes for each run of RAD instead of threads due to
    # a limitation of rpy2 which forbids multi-threaded access of the R env.
    indices = [i for i in xrange(len(routes))]
    multiprocess_map(check_anomaly, indices)

    return anomalies


def multiprocess_wrapper(func, input_queue, output_queue):
    """A simple wrapper function used by multiprocess_map.

    Taken from: http://stackoverflow.com/a/16071616"""
    while True:
        index, args = input_queue.get()
        if index is None:
            break

        output_queue.put((index, func(args)))


def multiprocess_map(func, args_list,
                     num_processes=multiprocessing.cpu_count()):
    """Fork multiple processes and map a function across the processes.

    'func' is some arbitrary function that takes any element in args_list
    as its paremeter.
    'args_list' is a list of parameters to be given to func.
    'num_processes' is the number of processes to fork.

    Taken from: http://stackoverflow.com/a/16071616"""
    input_queue = multiprocessing.Queue(1)
    output_queue = multiprocessing.Queue()
    processes = [multiprocessing.Process(
        target=multiprocess_wrapper, args=(func, input_queue, output_queue))
               for _ in xrange(num_processes)]

    for process in processes:
        process.daemon = True
        process.start()

    sent = [input_queue.put((index, args))
            for index, args in enumerate(args_list)]
    [input_queue.put((None, None)) for _ in xrange(num_processes)]
    res = [output_queue.get() for _ in xrange(len(sent))]

    [process.join() for process in processes]

    return [output for index, output in sorted(res)]
