import multiprocessing

import rpy2.rinterface
import rpy2.robjects as robjects

import models
import server

robjects.r('suppressMessages(require(RAD))')
rpca = robjects.r('AnomalyDetection.rpca')

NUM_HOURS_PER_WEEK = 7 * 24


def get_anomalies(time_series, frequency):
    # Convert our time series to work with R.
    time_series = robjects.FloatVector(time_series)

    try:
        anomalies = rpca(time_series, frequency=frequency)[2]
    except rpy2.rinterface.RRuntimeError:
        # The time series was too small to detect anomalies.
        anomalies = len(time_series) * [0.0]

    return list(anomalies)


def find_anomalies_on_routes(log_hour, routes):
    anomalies = multiprocessing.Array('d', len(routes), lock=True)

    def check_anomaly(index):
        hours_seen, responses_count = models.get_hourly_responses_count(
            routes[index], server.HTTP_OK_CODE)

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

        anomaly_scores = get_anomalies(responses_count, NUM_HOURS_PER_WEEK)
        if anomaly_scores[log_hour_index - 1] != 0:
            anomalies[index] = anomaly_scores[log_hour_index - 1]

    # We fork new processes for each run of RAD instead of threads due to
    # a limitation of rpy2.
    indices = [i for i in xrange(len(routes))]
    multiprocess_map(check_anomaly, indices)

    return anomalies


def multiprocess_wrapper(func, input_queue, output_queue):
    """Taken from: http://stackoverflow.com/a/16071616"""
    while True:
        index, args = input_queue.get()
        if index is None:
            break

        output_queue.put((index, func(args)))


def multiprocess_map(func, args_list,
                     num_processes=multiprocessing.cpu_count()):
    """Taken from: http://stackoverflow.com/a/16071616"""
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
