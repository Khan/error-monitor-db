"""Unit tests for the endpoints in server.py."""
import fakeredis
import json
import unittest

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

        # Record an error a few different times over a few different hours
        for i in xrange(5):
            error_key = models.record_occurrence_from_logs(
                version="v001", log_hour="20141110_0400", status=500, level=4,
                resource="/omg", ip="2.2.2.2", route="/omg", module="default",
                message="You can't handle the truth!")

        for i in xrange(7):
            models.record_occurrence_from_logs(
                version="v001", log_hour="20141110_0500", status=500, level=4,
                resource="/omg", ip="2.2.2.2", route="/omg", module="default",
                message="You can't handle the truth!")

        # Validate we stored all the correct data
        rv = self.app.get("/error/%s" % error_key)
        assert rv.status_code == 200

        # Check error def is stored correctly
        assert '"status": 500' in rv.data
        assert '"level": 4' in rv.data
        assert '"title": "You can\'t handle the truth!"' in rv.data

        # Check version data is stored correctly
        assert '"v001": 12' in rv.data
        assert '"last_seen": "20141110_0500"' in rv.data
        assert '"first_seen": "20141110_0400"' in rv.data
        assert (
            '"count": 5, "version": "v001", "hour": "20141110_0400"' in rv.data
            )
        assert (
            '"count": 7, "version": "v001", "hour": "20141110_0500"' in rv.data
            )


if __name__ == '__main__':
    unittest.main()
