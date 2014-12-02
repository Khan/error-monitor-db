"""Unit tests for the endpoints in server.py."""
import fakeredis
import json
import unittest

import bigquery_import
import models
import server


class ErrorMonitorTest(unittest.TestCase):
    def setUp(self):
        # Mock out the Redis instance we are talking to so we don't trash
        # the production db
        self.old_r = models.r
        models.r = fakeredis.FakeStrictRedis()

        # Get a test app we can make requests against
        self.app = server.app.test_client()

    def tearDown(self):
        # Restore mocked Redis
        models.r = self.old_r

    def test_monitor_logs(self):
        # First we monitor a "perfect" build with no errors
        monitor_data = {
            'logs': [],
            'minute': 0,
            'version': 'v000'
        }
        rv = self.app.post('/monitor',
                data=json.dumps(monitor_data),
                headers={"Content-type": "application/json"})
        assert rv.status_code == 200

        # Monitoring is kind of useless here since we have no history, so
        # no errors will be returned
        rv = self.app.get('/errors/v000/monitor/0?verify_versions=x')
        ret = json.loads(rv.data)
        assert 'errors' in ret
        assert ret['errors'] == []

        # Now add some actual errors
        monitor_data = {
            'logs': [
                # A unique error!
                {"status": 500, "level": 4, "resource": "/test",
                    "ip": "1.1.1.1", "route": "/test", "module_id": "default",
                    "message": "Error while parsing directive 1"},

                # This error should be grouped with the previous one
                {"status": 500, "level": 4, "resource": "/test",
                    "ip": "1.1.1.1", "route": "/test", "module_id": "default",
                    "message": "Error while parsing directive 2"},

                # This error will be ignored because the URI is blacklisted
                {"status": 500, "level": 4,
                    "resource": "/api/internal/translate/lint_poentry",
                    "ip": "1.1.1.1", "route": "/test", "module_id": "default",
                    "message": "This URI is blacklisted."},

                # A second unique error
                {"status": 500, "level": 4, "resource": "/leia",
                    "ip": "1.1.1.1", "route": "/leia", "module_id": "default",
                    "message": "Help me, Obi Wan Kenobi. You're my only hope"},
            ],
            'minute': 0,
            'version': 'v001'
        }
        rv = self.app.post('/monitor',
                data=json.dumps(monitor_data),
                headers={"Content-type": "application/json"})
        assert rv.status_code == 200

        # Now the monitor results should show some new errors
        rv = self.app.get('/errors/v001/monitor/0?verify_versions=v000')
        assert 'directive 1' in rv.data
        assert 'directive 2' not in rv.data
        assert 'blacklisted' not in rv.data
        assert 'Obi Wan' in rv.data
        ret = json.loads(rv.data)
        assert 'errors' in ret
        assert len(ret['errors']) == 2

        # Extract the error keys for the new errors and look them up by the
        # first word in the message
        error_keys = {
            e['message'].split(" ")[0]: e['key']
            for e in ret['errors']
        }

        # Request for summary info for a garbage error fails
        rv = self.app.get("/error/GARBAGE")
        assert rv.status_code == 404

        # Request an actual error, see that it is reported for the monitoring
        # version
        rv = self.app.get("/error/%s" % error_keys["Help"])
        assert rv.status_code == 200
        assert '"MON_v001": 1' in rv.data

        # The same analysis with an invalid version would *not* report any
        # errors, because we have no data for the supposedly successful
        # previous version
        rv = self.app.get('/errors/v001/monitor/0?verify_versions=vINVALID')
        ret = json.loads(rv.data)
        assert 'errors' in ret
        assert ret['errors'] == []

        # Now add some actual errors
        monitor_data = {
            'logs': [
                # This is the same error from before, but it is only happening
                # once so no need to panic
                {"status": 500, "level": 4, "resource": "/test",
                    "ip": "1.1.1.1", "route": "/test", "module_id": "default",
                    "message": "Error while parsing directive 500"},

                # This is also a familiar error, but it's happening more
                # frequently now so we need to know about it
                {"status": 500, "level": 4, "resource": "/leia",
                    "ip": "1.1.1.1", "route": "/leia", "module_id": "default",
                    "message": "Help me, Obi Wan Kenobi. You're my only hope"},
                {"status": 500, "level": 4, "resource": "/leia",
                    "ip": "1.1.1.1", "route": "/leia", "module_id": "default",
                    "message": "Help me, Obi Wan Kenobi. You're my only hope"},
                {"status": 500, "level": 4, "resource": "/leia",
                    "ip": "1.1.1.1", "route": "/leia", "module_id": "default",
                    "message": "Help me, Obi Wan Kenobi. You're my only hope"},
                {"status": 500, "level": 4, "resource": "/leia",
                    "ip": "1.1.1.1", "route": "/leia", "module_id": "default",
                    "message": "Help me, Obi Wan Kenobi. You're my only hope"},
                {"status": 500, "level": 4, "resource": "/leia",
                    "ip": "1.1.1.1", "route": "/leia", "module_id": "default",
                    "message": "Help me, Obi Wan Kenobi. You're my only hope"},
                {"status": 500, "level": 4, "resource": "/leia",
                    "ip": "1.1.1.1", "route": "/leia", "module_id": "default",
                    "message": "Help me, Obi Wan Kenobi. You're my only hope"},

                # This is a brand new error. We want to know about it even
                # though it only happened once
                {"status": 404, "level": 4, "resource": "/home",
                    "ip": "1.1.1.1", "route": "/home", "module_id": "default",
                    "message": "There's no place like home."},
            ],
            'minute': 0,
            'version': 'v002'
        }
        rv = self.app.post('/monitor',
                data=json.dumps(monitor_data),
                headers={"Content-type": "application/json"})
        assert rv.status_code == 200

        # Now the monitor results should show some new errors
        rv = self.app.get('/errors/v002/monitor/0?verify_versions=v000,v001')
        assert 'directive' not in rv.data
        assert 'Obi Wan' in rv.data
        ret = json.loads(rv.data)
        assert 'errors' in ret
        assert len(ret['errors']) == 2

        # Request the previous error again to see the new version added
        rv = self.app.get("/error/%s" % error_keys["Help"])
        assert rv.status_code == 200
        assert '"MON_v001": 1' in rv.data
        assert '"MON_v002": 6' in rv.data

    def test_logs_from_bigquery(self):
        # TODO(tom) Once BigQuery scraping is implemented, call that and mock
        # out the relevant query functions

        # Mock out the actual BigQuery query mechanism
        bigquery_import.BigQuery.__init__ = lambda self: None
        bigquery_import.BigQuery.run_query = lambda self, sql: query_response
        bq = bigquery_import.BigQuery()

        # Record an error a few different times over a few different hours
        query_response = [{
            "f": [
                {"v": "0000-0000-0123456789ab"},
                {"v": "2.2.2.2"},
                {"v": "/omg"},
                {"v": 500},
                {"v": 4},
                {"v": "You can't handle the truth!"},
                {"v": "/omg"},
                {"v": "default"}
            ]
        } for i in xrange(5)]
        errors_4, new_errors_4 = bq.logs_from_bigquery("20141110_0400")

        # There is only one error here, and it is new
        assert len(errors_4) == 1
        assert len(new_errors_4) == 1

        query_response = [{
            "f": [
                {"v": "0000-0000-0123456789ab"},
                {"v": "2.2.2.2"},
                {"v": "/omg"},
                {"v": 500},
                {"v": 4},
                {"v": "You can't handle the truth!"},
                {"v": "/omg"},
                {"v": "default"}
            ]
        } for i in xrange(7)]
        errors_5, new_errors_5 = bq.logs_from_bigquery("20141110_0500")

        # There is only one error here, and it is not new
        assert len(errors_5) == 1
        assert len(new_errors_5) == 0

        # Validate we stored all the correct data
        rv = self.app.get("/error/%s" % list(errors_4)[0])
        assert rv.status_code == 200

        # Check error def is stored correctly
        assert '"status": 500' in rv.data
        assert '"level": 4' in rv.data
        assert '"title": "You can\'t handle the truth!"' in rv.data

        # Check version data is stored correctly
        assert '"0000-0000-0123456789ab": 12' in rv.data
        assert '"last_seen": "20141110_0500"' in rv.data
        assert '"first_seen": "20141110_0400"' in rv.data
        assert (
            '"count": 5, "version": "0000-0000-0123456789ab", '
            '"hour": "20141110_0400"' in rv.data)
        assert (
            '"count": 7, "version": "0000-0000-0123456789ab", '
            '"hour": "20141110_0500"' in rv.data)

        # Now we somehow get through a perfect monitoring session for a new
        # version even though there is this intermittent bug
        monitor_data = {
            'logs': [],
            'minute': 0,
            'version': '0000-1111-0123456789ab'
        }
        rv = self.app.post('/monitor',
                           data=json.dumps(monitor_data),
                           headers={"Content-type": "application/json"})
        assert rv.status_code == 200

        # Now we see the same error during monitoring of a new version
        monitor_data = {
            'logs': [
                # This is the same error from before, but it is only happening
                # once so no need to panic
                {"status": 500, "level": 4, "resource": "/wut",
                 "ip": "1.1.1.1", "route": "/wut", "module_id": "default",
                 "message": "You can't handle the truth!"},
            ],
            'minute': 0,
            'version': '0000-2222-0123456789ab'
        }
        rv = self.app.post('/monitor',
                           data=json.dumps(monitor_data),
                           headers={"Content-type": "application/json"})
        assert rv.status_code == 200

        # Now the monitor results should show some new errors
        rv = self.app.get(
            '/errors/0000-2222-0123456789ab/monitor/0?verify_versions='
            '0000-0000-0123456789ab,0000-1111-0123456789ab')
        ret = json.loads(rv.data)
        assert 'errors' in ret
        assert len(ret['errors']) == 0


if __name__ == '__main__':
    unittest.main()
