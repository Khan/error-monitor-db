import rpy2.rinterface
import rpy2.robjects as robjects
robjects.r('suppressMessages(require(RAD))')


rpca = robjects.r('AnomalyDetection.rpca')


def get_anomalies(time_series, frequency=168):
    # Convert our time series to work with R.
    time_series = robjects.FloatVector(time_series)

    try:
        anomalies = rpca(time_series, frequency=frequency)[2]
    except rpy2.rinterface.RRuntimeError:
        # The time series was too small to detect anomalies.
        anomalies = len(time_series) * [0.0]

    return list(anomalies)
