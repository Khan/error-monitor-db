"""Logic for tracking error classes & occurrences in Redis.

Errors are filtered entries from AppEngine's server logs having an error level
of ERROR or CRITICAL, which are emitted either by our own code in webapp or by
exceptions thrown in any code running on AppEngine. The version we receive here
is roughly a dictionary version of the fields in AppEngine's original
RequestLog class:

    https://cloud.google.com/appengine/docs/python/logs/requestlogclass

Each error we receive from the logs is considered an /occurrence/ of some
more general error /class/ that we want to track the frequency of by grouping
together the occurrences while ignoring specifics such as user IP, the exact
stack trace, etc.

The `message` field is either a string we emit in `logging.error` or
`logging.critical` calls, or an Exception's message followed by the stack
trace. Thus the message is the most important component we use to group errors,
but we can't do simple comparisons because errors may have numbers names or
other variables interpolated into them, so we have to be clever about how we
derive identifiers from the message.

There are several derived identifiers that we compare errors by and any _one_
match will cause the errors to be grouped together, so occurrence A can match
occurrence B, which in turn matches occurrence C, even if A and C don't match.
This results in a grouping that is dependent on the order the occurrences are
observed, but in practice this should be rare.

An error /class/ is represented by an error definition (error_def) which
contains the following identifying information:

    key:         A hash of the status, level, and various IDs that uniquely
                 identifies the error.

    title:       The first line of the error message, which is used as a
                 human-readable identifier.

    status:      The response status of the handler that reported the error,
                 e.g. 400, 404, 500, etc.

    level:       The error level from AppEngine, always either 3 (ERROR) or 4
                 (CRITICAL).

    id0/.../id3: Various identifiers extracted from the error message. If a new
                 error instance matches any of these identifiers, we group it
                 with this class instead of creating a new class for it.

See `_parse_message` for an example error_def.

For each error class we track the following data per /occurrence/:

    ip:          The source IP address for the request.

    resource:    The request URI.

    route:       The route identifier of the handler that handled the request.

    module_id:   The GAE module that handled the request.

    stack:       A parsed version of the stack trace (if any) extracted from
                 the message, including a filename, line number, and method
                 name.

    stack_key:   A hash of the stack trace, for coalescing similar stacks.

We write to the following Redis keys:

    (NOTE: Sorted sets in Redis are maps of {string: int} ordered by value, and
     in this case the value is the incidence count.)

    // Error definition (static for each error class)

    error:<key> - JSONified error_def for the error class identified by 'key'

    errordef:id0 ... errordef:id3 - Hashtables of identifier -> error key for
        various identifier types


    // Error occurrence information by version

    (NOTE: When an error occurs during monitoring, the version is prefixed with
     "MON_" to keep monitoring errors separate from errors scraped from the
     BigQuery logs.)

    ver:<version>:errors - Sorted set of error keys seen on this GAE version

    ver:<version>:error:<key>:ips - Sorted set of IPs associated with the error

    ver:<version>:error:<key>:stacks:msgs - Hashtable of stack key ->
        JSONified stack trace for all stacks associated with this error

    ver:<version>:error:<key>:stacks:counts - Sorted set of stack keys
        associated with the error

    ver:<version>:error:<key>:routes - Sorted set of routes associated with the
        error

    ver:<version>:error:<key>:uris - Sorted set of URIs associated with the
        error

    ver:<version>:error:<key>:modules - Sorted set of GAE modules associated
        with the error


    // Monitoring only

    ver:<version>:errors_by_minute:<minute> - Sorted set of error keys seen
        during a 60-second interval 'minute' minutes after monitoring has begun


    // BigQuery logs only

    first_seen:<key> - The first log hour when this error appeared in the logs

    last_seen:<key> - The latest log hour when this error appeared in the logs

    ver:<version>:error:<key>:hours_seen - A dictionary of each log hour when
        this error appeared in this version's logs and the occurrence count
        for that hour
    

"""

import json
import md5
import re
import redis

# GAE uses numbers internally to denote error level. We only care about levels
# 3 and 4.
ERROR_LEVELS = ["", "", "", "ERROR", "CRITICAL"]

# URIs to ignore because they spam irrelevant errors
URI_BLACKLIST = [
    '/api/internal/translate/lint_poentry'
    ]

# Time delay until we expire keys (one week)
KEY_EXPIRY_SECONDS = 60 * 60 * 24 * 7

# webapp appends a cache-busting query param e.g. _=131231 for API calls
# made from the JS code. We want to ignore them because a different cache
# busting param doesn't indicate anything semantic about the API call
_CACHE_BUST_QUERY_PARAM_RE = re.compile(r'(?<=[?&])_=\d+')

# Each different version has a different file path. When we're deciding
# whether two stacks are unique or not, we don't care about those, so
# get rid of them.
#
# The original file paths start like this:
# /base/data/home/apps/s~khan-academy/1029-2305-f48a12e2b9ba.379742046073152437/api/errors.py @Nolint
_VERSION_PATH_PREFIX_RE = re.compile(r'^.*\d{4}-\d{4}-[a-f0-9]{12}\.\d+/')

# Keys in the error def that represent identifiers
_ERROR_ID_KEYS = ["id0", "id1", "id2", "id3"]

r = redis.StrictRedis(host='localhost', port=6379, db=0)


####
## Caches for error data from Redis.
####

# A cache of the error def (status, level, title, IDs, etc.)
# This cache is not invalidated when the error count changes since the
# data is completely static after creation
# TODO(tom) Allow entries to expire here like they do in Redis (get expiry
# information from Redis)
_error_def_cache = {}

# A cache of error ID -> error key dictionaries, one for each identifier we
# want to be able to look up errors with
_error_id_cache = {key: {} for key in _ERROR_ID_KEYS}


def _get_cached_error_def(error_key):
    """Retrieve the error def information from cache or Redis."""
    if error_key in _error_def_cache:
        return _error_def_cache[error_key]

    err = r.get("error:%s" % error_key)
    if not err:
        return None

    err = json.loads(err)
    _error_def_cache[error_key] = err
    for id in _ERROR_ID_KEYS:
        _error_id_cache[id][err[id]] = error_key
    return err

# TODO(tom) Cache summary statistics and drill-down information


####
## General-purpose error tracking methods
####


def _find_error_by_def(error_def):
    """Find an existing error by the identifying information in error_def.

    If successful, return the error key. Otherwise return None.
    """
    # Try to match by hash
    if (error_def['key'] in _error_def_cache or
            r.get("error:%s" % error_def['key'])):
        return error_def['key']

    # Try to match by each ID in turn

    # Look up in in-memory cache first
    for id in _ERROR_ID_KEYS:
        if not error_def[id]:
            continue
        error_key = _error_id_cache[id].get(error_def[id])
        if error_key:
            return error_key

    # Fall back to Redis
    for id in _ERROR_ID_KEYS:
        if not error_def[id]:
            continue
        error_key = r.hget("errordef:%s" % id, error_def[id])
        if error_key:
            return error_key

    return None


def _find_or_create_error(error_def, expiry):
    """Write identifying error info to Redis if it isn't there already.

    'error_def' is the error information dict returned by _parse_message.

    'expiry' is the timeout in seconds until all these keys expire (or -1
    for no expiration). Existing errors will have their expiration lease
    renewed.
    
    Returns the identifier key of the existing or new error.
    """

    # Attempt to match an existing error
    error_key = _find_error_by_def(error_def)
    if not error_key:
        error_key = error_def['key']

        # Store the error def information as one key
        r.set("error:%s" % error_key, json.dumps(error_def))

        # Store the IDs in the lookup tables
        # TODO(tom) Since these are all in big hashtables, we can't expire
        # them automatically. We could however rebuild the hashtables from
        # all the unexpired errors.
        for id in ["id0", "id1", "id2", "id3"]:
            if error_def[id]:
                r.hset("errordef:%s" % id, error_def[id], error_key)

    # Bump the expiry time for the error information
    r.expire("error:%s" % error_key, expiry)

    return error_key


def _parse_message(message, status, level):
    """Splits message into title, identifying information, and stack trace.

    'message' is the multiline string error message to parse.
    'status' and 'level' are included in the identifying information.

    Returns a tuple with an error def, a stack trace and stack key, e.g.:
        ({
            'key': "a2b099f3",
            'title': "Error on line 214: File not found",
            'status': "200",
            'level': "3",
            'id0': "200 3 Error on line %%: File not found",
            'id1': "200 3 Error on line",
            'id2': "200 3 File not found",
            'id3': None
        }, [{
            'filename': "/home/webapp/main.c",
            'lineno': 214,
            'function': "parse_file"
        }],
        1234092)
    """
    error_def = {}
    msg_lines = message.split("\n")

    # The first line of the message is the visible error message
    error_def['title'] = msg_lines[0].encode('utf-8')

    # Copy over status & level
    error_def['status'] = status
    error_def['level'] = level

    # Various identifying traits
    #
    # The title with numbers removed
    id_prefix = str("%s %s " % (status, level))
    error_def['id0'] = (
            id_prefix + re.sub(r'\d+', '%%', error_def['title']))

    # The first 3 words of the title
    error_def['id1'] = (
            id_prefix + " ".join(error_def['id0'].split(" ")[:3]))

    # The last 3 words of the title
    error_def['id2'] = (
            id_prefix + " ".join(error_def['id0'].split(" ")[-3:]))

    # Special-cases
    error_def['id3'] = None
    if error_def['title'].startswith("{'report_url':"):
        # TODO(tom) Not sure where this error (which is just a bit of JSON
        # without context) is being emitted. The most likely data structure
        # this could be is the serializable version of UserAssessment in
        # assessment/models.py. Would be nice to track this down so we can
        # remove the special-case.
        error_def['id3'] = id_prefix + 'report_url'

    # Build a hash of the identifiers to serve as a single unique
    # identifier for the error
    h = md5.md5()
    id_str = "%(id0)s%(id1)s%(id2)s%(id3)s" % error_def
    h.update(id_str)
    error_def['key'] = h.hexdigest()[:8]

    # Parse the rest of the message, which we expect to be a stack trace
    stack = []

    for line in msg_lines[1:]:
        if line.startswith("Traceback"):
            continue
        if line.startswith("  File "):
            match = re.match(
                r'  File "([^"]*)", line (\d*), in (.*)', line)
            if not match:
                # This case happens often because of a truncated line when the
                # error message becomes too long.
                continue
            (filename, lineno, function) = match.groups()
            filename = _VERSION_PATH_PREFIX_RE.sub('', filename)
            stack.append({
                "filename": filename,
                "lineno": lineno,
                "function": function
            })

    # We want to de-duplicate storage of stacks, but be resilient to line
    # number changes, so we ignore line numbers when doing the hashing.
    h = md5.md5()
    h.update("|".join("%(filename)s:%(function)s" % s
                for s in stack))
    stack_key = h.hexdigest()

    return (error_def, stack, stack_key)


def get_error_summary_info(error_key):
    """Retrieve error summary information from Redis.
    
    The format of the summary information is:

        "error_def" - See _parse_message for the error def format

        "versions" - A dictionary storing occurrence counts per GAE version.
            Occurrences during monitoring are returned separately from errors
            from BigQuery and the versions for those are prefixed with "MON_".

        "first_seen" - The log hour in which this error was first observed (not
            including logs observed while monitoring)

        "last_seen" - The log hour in which this error was last observed (not
            including logs observed while monitoring)

        "by_hour_and_version" - A list of structs, one for each "version" and
            "hour" that we observed the error on, and the occurrence "count"
            (not including errors observed while monitoring)

    """
    error_def = _get_cached_error_def(error_key)
    if not error_def:
        return None

    versions = r.zrevrange("%s:versions" % error_key, 0, -1, withscores=True)

    by_hour_and_version = []
    for version, _ in versions:
        hours_seen = r.hgetall("ver:%s:error:%s:hours_seen" % (
            version, error_key))
        if hours_seen:
            for hour, count in hours_seen.iteritems():
                by_hour_and_version.append({
                    "hour": hour,
                    "version": version,
                    "count": count
                })
    
    error_info = {
        "error_def": error_def,
        "versions": dict(versions),
        "first_seen": r.get("first_seen:%s" % error_key),
        "last_seen": r.get("last_seen:%s" % error_key),
        "by_hour_and_version": by_hour_and_version
    }

    return error_info


####
## Common error-tracking methods
####

def _update_error_details(version, status, level, resource, ip, route,
                              module, message):
    """Store a new error instance which was seen while monitoring a deploy.

    All the Redis keys for the data is prefixed with the version so they
    can be queried and expired separately from the other versions. Errors seen
    during monitoring are recorded with "MON_" prepended to the version in
    order to avoid double-counting those errors.

    'version' is the GAE version name this error was logged on.

    'status' is the HTTP status code for the request (an integer).

    'level' is either 3 for ERROR or 4 for CRITICAL.

    'resource' is the URI the request was serving.

    'ip' is the client IP for the request. Internal requests (e.g. task
    queues) are all going to have the same internal system IP.

    'route' is the route specifier which represents the handler function that
    handled the request.

    'module' is the GAE module that the instance handling the request was
    running.

    'message' is the recorded error message, including the stack in the case
    of an exception (in which case it contains multiple lines).
    """
    if any(resource.startswith(uri) for uri in URI_BLACKLIST):
        # Ignore particularly spammy URIs
        return None

    # Parse the message to extract the title, identifying information, and
    # parsed stack trace
    error_def, stack, stack_key = _parse_message(message, status, level)

    # Look up the identifying information to see if we already have an
    # error that matches, in which case we use that error's key. If not,
    # write the new error to Redis.
    error_key = _find_or_create_error(error_def, KEY_EXPIRY_SECONDS)

    # All the occurrence-statistic Redis keys share a common prefix to keep
    # them separate from other error classes and versions
    key_prefix = "ver:%s:error:%s" % (version, error_key)

    # webapp appends a cache-busting query param e.g. _=131231 for API calls
    # made from the JS code. We want to ignore them because a different cache
    # busting param doesn't indicate anything semantic about the API call
    resource = _CACHE_BUST_QUERY_PARAM_RE.sub('', resource)

    # Record how many unique IPs have hit this endpoint, and also how many
    # times each of them hit the error.
    r.zincrby("%s:ips" % key_prefix, ip)
    r.expire("%s:ips" % key_prefix, KEY_EXPIRY_SECONDS)

    # Record all the unique stack traces we get, and count how many times each
    # of them is hit. The stack IDs are stored by route so we can show them
    # grouped that way in the UI.
    r.hset("%s:stacks:msgs" % key_prefix, stack_key, json.dumps(stack))
    r.expire("%s:stacks:msgs" % key_prefix, KEY_EXPIRY_SECONDS)
    r.zincrby("%s:stacks:%s:counts" % (key_prefix, route), stack_key)
    r.expire("%s:stacks:%s:counts" % (key_prefix, route), KEY_EXPIRY_SECONDS)

    # Record all of the routes causing this error, and also how many times
    # each of them is being hit.
    r.zincrby("%s:routes" % key_prefix, route)
    r.expire("%s:routes" % key_prefix, KEY_EXPIRY_SECONDS)

    # Record hits for a specific URL. We classify URLs hierarchically under
    # routes.
    r.zincrby("%s:uris:%s" % (key_prefix, route), resource)
    r.expire("%s:uris:%s" % (key_prefix, route), KEY_EXPIRY_SECONDS)

    # Record a hit for a specific module id
    r.zincrby("%s:modules" % key_prefix, module)
    r.expire("%s:modules" % key_prefix, KEY_EXPIRY_SECONDS)

    # Track how many times each error has been seen by version overall
    # and by the time elapsed since we started monitoring
    r.zincrby("ver:%s:errors" % version, error_key)
    r.expire("ver:%s:errors" % version, KEY_EXPIRY_SECONDS)

    # Record a hit for the version
    r.zincrby("%s:versions" % error_key, version)
    r.expire("%s:versions" % error_key, KEY_EXPIRY_SECONDS)

    return error_key


####
## Methods specific to deploy-time monitoring
####


def get_monitoring_errors(version, minute):
    """Return a list of the top errors found in a specific version.

    'minute' identifies a slice of time some number of minutes after monitoring
    started that we are fetching errors for, so 0 is the first 60 seconds after
    monitoring, 1 is the next 60 seconds, etc.

    The returned list will be ordered by the number of occurrences in the
    requested version and each entry is a tuple with format
    (error_def, count) where error_def is a dictionary and count is a number.
    """
    keys = r.zrevrange("ver:MON_%s:errors_by_minute:%d" % (version, minute),
            0, 1000, withscores=True)
    return [(_get_cached_error_def(k), count) for k, count in keys]


def lookup_monitoring_error(version, minute, error_key):
    """Check if an error was reported by the GAE version at this minute.

    'minute' identifies a slice of time some number of minutes after monitoring
    started that we are fetching errors for, so 0 is the first 60 seconds after
    monitoring, 1 is the next 60 seconds, etc.

    Returns True if we have monitoring data for this time slice and the error
    with the key 'error_key' was included in that data.
    """
    return (r.zscore(
        "ver:MON_%s:errors_by_minute:%d" % (version, minute), error_key)
        is not None)


def record_monitoring_data_received(version, minute):
    """Track that we've received log data for the GAE version and minute.

    We use these "seen" flags to determine whether we have data for this
    version to compare future versions against.
    """
    r.hset("ver:MON_%s:seen" % version, minute, 1)
    r.expire("ver:MON_%s:seen" % version, KEY_EXPIRY_SECONDS)


def check_monitoring_data_received(version, minute):
    """Check that we have received log data for the GAE version and minute."""
    return r.hget("ver:MON_%s:seen" % version, minute) is not None


def record_occurrence_during_monitoring(version, minute, status, level,
                                        resource, ip, route, module, message):
    """Store error details for an occurrence seen while monitoring GAE logs.

    'version', 'status', 'level', 'resource', 'ip', 'route', 'module', and
    'message' are all documented in `_update_error_details`.

    'minute' identifies a slice of time some number of minutes after monitoring
    started that we are fetching errors for, so 0 is the first 60 seconds after
    monitoring, 1 is the next 60 seconds, etc.
    """
    error_key = _update_error_details(
        "MON_%s" % version, status, level, resource, ip, route, module,
        message)

    if error_key:
        r.zincrby("ver:MON_%s:errors_by_minute:%d" % (version, minute),
                  error_key)
        r.expire("ver:MON_%s:errors_by_minute:%d" % (version, minute),
                 KEY_EXPIRY_SECONDS)


####
## Log scraping from BigQuery
####


def record_log_data_received(log_hour):
    """Track that we've received error data from the GAE logs (via BigQuery)."""
    r.set("available_logs:%s" % log_hour, 1)
    r.expire("available_logs:%s" % log_hour, KEY_EXPIRY_SECONDS)


def check_log_data_received(log_hour):
    """Check whether we have error data from BigQuery for the given hour."""
    return r.get("available_logs:%s" % log_hour) is not None


def record_occurrence_from_logs(version, log_hour, status, level, resource, ip,
                                route, module, message):
    """Store error details for an occurrence seen while scraping GAE app logs.

    'version', 'status', 'level', 'resource', 'ip', 'route', 'module', and
    'message' are all documented in `_update_error_details`.

    'log_hour' is the suffix of the BigQuery dataset name, which is a string
    in the format 'YYYYMMDD_HH', for example '20141120_10'. This is
    convenient because string ordering is chronological.
    """

    error_key = _update_error_details(
        version, status, level, resource, ip, route, module, message)
    is_new = False

    if error_key:
        # Record a hit for a specific hour on a specific version, to get more
        # granular time stats

        r.hincrby("ver:%s:error:%s:hours_seen" % (version, error_key),
                log_hour, 1)
        r.expire("ver:%s:error:%s:hours_seen" % (version, error_key),
                KEY_EXPIRY_SECONDS)

        # Record the first and last time we've seen the error.

        # Since log_hour is a string, use max char in case the key is missing
        if log_hour < (r.get("first_seen:%s" % error_key) or "\xff"):
            r.set("first_seen:%s" % error_key, log_hour)
            r.expire("first_seen:%s" % error_key, KEY_EXPIRY_SECONDS)
            # This will be set incorrectly if we process logs in
            # non-chronological order.
            # Currently we only use this for alerting in HipChat so hopefully
            # this won't be a huge issue.
            is_new = True

        if log_hour > r.get("last_seen:%s" % error_key):
            r.set("last_seen:%s" % error_key, log_hour)
            r.expire("last_seen:%s" % error_key, KEY_EXPIRY_SECONDS)

    return error_key, is_new
