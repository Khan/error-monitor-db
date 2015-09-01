error-monitor-db
================

Error monitoring service for errors from the Khan Academy webapp monitoring / logging infrastructure.

This is deployed on internal-services.khanacademy.org, and may be restarted by running `sudo restart error-monitor-db`.  The scripts `bigquery_import.py` and `report_errors.py` are invoked by cron (see aws-config/internal-services/crontab).

There are two kinds of error monitoring we want to implement:

Deploy-time error monitoring
----------------------------

At deploy time, we scrape the logs for the currently deployed version every 20 seconds for the first 10 minutes after setting it default. This is done by polling an internal API call that fetches the logs and forwarding them to this service at the /monitor endpoint. The errors are saved in a cache per version in Redis and compared to a list (supplied by the deploy script) of recent successful deploys to highlight A) new errors and B) errors which are occurring at a much higher rate than expected.

Continuous monitoring from BigQuery
-----------------------------------

Every hour we back up the application logs to BigQuery. Subsequently a CRON job will run as part of this service to query just those logs that represent errors and add them to a Redis database of all previously-seen errors. This allows us to track precisely how many instances of each error occured in any given hour and on any given version, along with relevant details such as stack traces, routes, and IPs.
