#!/usr/bin/env python

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
        models.r.flushall()       # clear the redis db for the new test
        models._reset_caches()

        # Simple implementation of 'scan', since it's missing from
        # `FakeStrictRedis`
        models.r.scan = lambda cursor, match, count: (
                (0, models.r.keys(match)))

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
                # (Only the first word matches the previous error, but we need
                # 3 to consider them the same)
                {"status": 500, "level": 4, "resource": "/leia",
                    "ip": "1.1.1.1", "route": "/leia", "module_id": "default",
                    "message": "Error Help me, Obi Wan Kenobi. You're my only "
                        "hope"},
                {"status": 500, "level": 4, "resource": "/leia",
                    "ip": "1.1.1.1", "route": "/luke", "module_id": "default",
                    "message": "Error Help me, Obi Wan Kenobi. Train me in "
                        "ways of the force"},

                # A third unique error, but it's ignored because
                # singleton errors are ignored.
                {"status": 500, "level": 4, "resource": "/test",
                    "ip": "1.1.1.1", "route": "/test", "module_id": "default",
                    "message": "This error only occurs once."},
            ],
            'minute': 0,
            'version': 'v001'
        }
        rv = self.app.post('/monitor',
                data=json.dumps(monitor_data),
                headers={"Content-type": "application/json"})
        assert rv.status_code == 200

        # The monitor results should also show some new errors.
        rv = self.app.get('/errors/v001/monitor/0?verify_versions=v000')
        # This should show the most recent of the 'directive' errors.
        assert 'directive 2' in rv.data
        assert 'directive 1' not in rv.data
        assert 'blacklisted' not in rv.data
        assert 'Obi Wan' in rv.data
        ret = json.loads(rv.data)
        assert 'errors' in ret
        assert len(ret['errors']) == 2

        # Request for summary info for a garbage error fails
        rv = self.app.get("/error/GARBAGE")
        assert rv.status_code == 404

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
                # though it only happened twice.
                {"status": 404, "level": 4, "resource": "/home",
                    "ip": "1.1.1.1", "route": "/home", "module_id": "default",
                    "message": "There's no place like home."},
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
                {"v": "000000-0000-0123456789ab"},
                {"v": "2.2.2.2"},
                {"v": "/omg"},
                {"v": 500},
                {"v": 4},
                {"v": "You can't handle the truth!"},
                {"v": "/omg"},
                {"v": "default"}
            ]
        } for i in xrange(5)]
        errors_4, new_errors_4 = bq.errors_from_bigquery("20141110_0400")

        # There is only one error here, and it is new
        assert len(new_errors_4) == 1
        assert len(old_errors_4) == 0

        query_response = [{
            "f": [
                {"v": "000000-0000-0123456789ab"},
                {"v": "2.2.2.2"},
                {"v": "/omg"},
                {"v": 500},
                {"v": 4},
                {"v": "You can't handle the truth!"},
                {"v": "/omg"},
                {"v": "default"}
            ]
        } for i in xrange(7)]
        errors_5, new_errors_5 = bq.errors_from_bigquery("20141110_0500")

        # There is only one error here, and it is not new
        assert len(new_errors_5) == 0
        assert len(old_errors_5) == 1

        # Validate we stored all the correct data
        rv = self.app.get("/error/%s" % list(new_errors_4)[0])
        assert rv.status_code == 200

        # Check error def is stored correctly
        assert '"status": 500' in rv.data
        assert '"level": 4' in rv.data
        assert '"title": "You can\'t handle the truth!"' in rv.data

        parsed_data = json.loads(rv.data)
        assert "routes" in parsed_data
        assert "count" in parsed_data["routes"][0]
        assert parsed_data["routes"][0]["count"] == 12
        assert "route" in parsed_data["routes"][0]
        assert parsed_data["routes"][0]["route"] == "/omg"

        # Check version data is stored correctly
        assert '"000000-0000-0123456789ab": 12' in rv.data
        assert '"last_seen": "20141110_0500"' in rv.data
        assert '"first_seen": "20141110_0400"' in rv.data
        assert (
            '"count": 5, "version": "000000-0000-0123456789ab", '
            '"hour": "20141110_0400"' in rv.data)
        assert (
            '"count": 7, "version": "000000-0000-0123456789ab", '
            '"hour": "20141110_0500"' in rv.data)

        # Now we somehow get through a perfect monitoring session for a new
        # version even though there is this intermittent bug
        monitor_data = {
            'logs': [],
            'minute': 0,
            'version': '000000-1111-0123456789ab'
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
            'version': '000000-2222-0123456789ab'
        }
        rv = self.app.post('/monitor',
                           data=json.dumps(monitor_data),
                           headers={"Content-type": "application/json"})
        assert rv.status_code == 200

        # Now the monitor results should show some new errors
        rv = self.app.get(
            '/errors/000000-2222-0123456789ab/monitor/0?verify_versions='
            '000000-0000-0123456789ab,000000-1111-0123456789ab')
        ret = json.loads(rv.data)
        assert 'errors' in ret
        assert len(ret['errors']) == 0

    def test_fetch_errors(self):
        # Add an error to the database
        monitor_data = {
            'logs': [
                # A unique error!
                {"status": 500, "level": 4, "resource": "/test",
                    "ip": "1.1.1.1", "route": "/test", "module_id": "default",
                    "message": "Error while parsing directive 1"}
            ],
            'minute': 0,
            'version': 'vx001'
        }
        rv = self.app.post('/monitor',
                data=json.dumps(monitor_data),
                headers={"Content-type": "application/json"})
        assert rv.status_code == 200

        # Add a new error to the database for a new version
        monitor_data = {
            'logs': [
                # A unique error!
                {"status": 500, "level": 4, "resource": "/leia",
                    "ip": "1.1.1.1", "route": "/leia", "module_id": "default",
                    "message": "Help me, Obi Wan Kenobi. You're my only hope"}
            ],
            'minute': 0,
            'version': 'vx002'
        }
        rv = self.app.post('/monitor',
                data=json.dumps(monitor_data),
                headers={"Content-type": "application/json"})
        assert rv.status_code == 200

        # Check that "recent errors" includes both errors
        rv = self.app.get('/recent_errors')
        ret = json.loads(rv.data)
        assert "errors" in ret
        assert len(ret["errors"]) == 2

        # Check that requesting errors from one version filters correctly
        rv = self.app.get('/version_errors/MON_vx001')
        ret = json.loads(rv.data)
        assert "errors" in ret
        assert len(ret["errors"]) == 1

        # Errors from monitoring don't "leak" to the non-monitoring version
        rv = self.app.get('/version_errors/vx001')
        ret = json.loads(rv.data)
        assert "errors" in ret
        assert len(ret["errors"]) == 0


class RequestMonitorTest(unittest.TestCase):
    def setUp(self):
        # Mock out the Redis instance we are talking to so we don't trash
        # the production db
        self.old_r = models.r
        models.r = fakeredis.FakeStrictRedis()

        # Simple implementation of 'scan', since it's missing from
        # `FakeStrictRedis`
        models.r.scan = lambda cursor, match, count: (
                (0, models.r.keys(match)))

        # Get a test app we can make requests against
        self.app = server.app.test_client()
        self.app.debug = True

        # Mock out the actual BigQuery query mechanism
        self.query_response = None
        bigquery_import.BigQuery.__init__ = lambda *args: None
        bigquery_import.BigQuery.run_query = lambda *args: self.query_response
        self.bq = bigquery_import.BigQuery()

    def tearDown(self):
        # Restore mocked Redis
        models.r.flushall()
        models.r = self.old_r

    def test_single_request(self):
        # Record a single request.
        self.query_response = [{
            "f": [
                {"v": 1},
                {"v": 200},
                {"v": "/path"},
            ]
        }]
        self.bq.requests_from_bigquery("20100101_01")

        self.app.get("/update_thresholds")
        ret = self.app.get("/anomalies/20100101_01")
        ret = json.loads(ret.data)

        assert len(ret["anomalies"]) == 0

    def test_single_anomaly(self):
        # Record multiple consistent requests over multiple dates.
        for i in xrange(1, 11):
            self.query_response = [{
                "f": [
                    {"v": 100},
                    {"v": 200},
                    {"v": "/path"},
                ]
            }]
            self.bq.requests_from_bigquery("201001%02d_01" % i)

        self.app.get("/update_thresholds")

        # Record a sudden steep drop in requests.
        self.query_response = [{
            "f": [
                {"v": 1},
                {"v": 200},
                {"v": "/path"},
            ]
        }]
        self.bq.requests_from_bigquery("20100120_01")

        ret = self.app.get("/anomalies/20100120_01")
        ret = json.loads(ret.data)

        assert len(ret["anomalies"]) == 1
        anomaly = ret["anomalies"][0]
        assert anomaly["route"] == "/path"
        assert anomaly["status"] == 200
        assert anomaly["count"] == 1

    def test_multiple_anomalies(self):
        # Record multiple consistent requests over multiple dates.
        for i in xrange(1, 11):
            for j in xrange(1, 11):
                self.query_response = [{
                    "f": [
                        {"v": 1000 + i + j},
                        {"v": 200},
                        {"v": "/path"},
                    ]
                }]
                self.bq.requests_from_bigquery("2010%02d%02d_01" % (i, j))

        # Record multiple relatively steep drops in requests.
        for i in xrange(1, 4):
            self.query_response = [{
                "f": [
                    {"v": 800 + i},
                    {"v": 200},
                    {"v": "/path"},
                ]
            }]
            self.bq.requests_from_bigquery("201101%02d_01" % i)

        self.app.get("/update_thresholds")
        for i in xrange(1, 4):
            ret = self.app.get("/anomalies/201101%02d_01" % i)
            ret = json.loads(ret.data)

            assert len(ret["anomalies"]) == 1
            anomaly = ret["anomalies"][0]
            assert anomaly["route"] == "/path"
            assert anomaly["status"] == 200
            assert anomaly["count"] == 800 + i

    def test_increasing_requests(self):
        # Simulate a path that gets more requests over time.
        for i in xrange(1, 11):
            for j in xrange(1, 11):
                self.query_response = [{
                    "f": [
                        {"v": 100 * (10 * i + j)},
                        {"v": 200},
                        {"v": "/path"},
                    ]
                }]
                self.bq.requests_from_bigquery("2010%02d%02d_01" % (i, j))

        self.app.get("/update_thresholds")

        # Now record a day with slightly fewer requests. This shouldn't raise
        # an anomaly
        self.query_response = [{
            "f": [
                {"v": 10800},
                {"v": 200},
                {"v": "/path"},
            ]
        }]
        self.bq.requests_from_bigquery("20110101_01")
        ret = self.app.get("/anomalies/20110101_01")
        ret = json.loads(ret.data)

        assert len(ret["anomalies"]) == 0

        # Finally record a day with way fewer requests. This should raise
        # an anomaly.
        self.query_response = [{
            "f": [
                {"v": 1000},
                {"v": 200},
                {"v": "/path"},
            ]
        }]
        self.bq.requests_from_bigquery("20110102_01")
        ret = self.app.get("/anomalies/20110102_01")
        ret = json.loads(ret.data)
        print ret, "\n\n\n\n"
        assert len(ret["anomalies"]) == 1


if __name__ == '__main__':
    unittest.main()
