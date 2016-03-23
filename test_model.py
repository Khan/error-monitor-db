import fakeredis
import unittest

import models


class ModelTest(unittest.TestCase):
    def setUp(self):
        # Mock out the Redis instance we are talking to so we don't trash
        # the production db
        self.old_r = models.r
        models.r = fakeredis.FakeStrictRedis()

    def tearDown(self):
        # Restore mocked Redis
        models.r.flushall()
        models.r = self.old_r

    def test_no_request(self):
        self.assertEquals(len(models.get_routes()), 0)
        self.assertEquals(models.get_responses_count(
            "/route1", 200, "20200101_11"), 0)
        self.assertEquals(
            len(models.get_hourly_responses_count("/route1", 200, 11)), 0)

    def test_single_request(self):
        models.record_occurrences_from_requests("20200101_11", 200,
                                                "/route1", 10)
        routes = models.get_routes()
        responses_count = models.get_responses_count("/route1", 200,
                                                     "20200101_11")

        self.assertEquals(len(routes), 1)
        self.assertEquals(routes[0], "/route1")
        self.assertEquals(responses_count, 10)

    def test_multiple_request(self):
        # Only the first request should go through to the database.
        models.record_occurrences_from_requests("20200101_11", 200,
                                                "/route1", 3)
        models.record_occurrences_from_requests("20200101_11", 200,
                                                "/route1", 3)
        models.record_occurrences_from_requests("20200101_11", 200,
                                                "/route1", 3)
        routes = models.get_routes()
        responses_count = models.get_responses_count("/route1", 200,
                                                     "20200101_11")

        self.assertEquals(len(routes), 1)
        self.assertEquals(routes[0], "/route1")
        self.assertEquals(responses_count, 3)

    def test_wrong_hour(self):
        models.record_occurrences_from_requests("20200101_11", 200,
                                                "/route1", 20)
        self.assertEquals(models.get_responses_count(
            "/route1", 200, "20200101_10"), 0)
        self.assertEquals(len(models.get_hourly_responses_count(
            "/route1", 200, 10)), 0)

    def test_wrong_status(self):
        models.record_occurrences_from_requests("20200101_11", 200,
                                                "/route1", 100)
        self.assertEquals(models.get_responses_count(
            "/route1", 400, "20200101_11"), 0)
        self.assertEquals(len(models.get_hourly_responses_count(
            "/route1", 400, 11)), 0)

    def test_wrong_route(self):
        models.record_occurrences_from_requests("20200101_11", 200,
                                                "/route1", 15)
        self.assertEquals(models.get_responses_count(
            "/route2", 200, "20200101_11"), 0)
        self.assertEquals(len(models.get_hourly_responses_count(
            "/route2", 200, 11)), 0)

    def test_multiple_day_requests_same_hour(self):
        models.record_occurrences_from_requests("20200101_11", 200,
                                                "/route1", 3)
        models.record_log_data_received("20200101_11")
        models.record_occurrences_from_requests("20200103_11", 200,
                                                "/route1", 2)
        models.record_log_data_received("20200103_11")
        models.record_occurrences_from_requests("20210111_11", 200,
                                                "/route1", 1)
        models.record_log_data_received("20210111_11")

        hourly_count = models.get_hourly_responses_count("/route1", 200, 11)
        self.assertEquals(len(hourly_count), 3)
        # We expect the counts to be ordered from earliest to latest date.
        self.assertEquals(hourly_count[0], 3)
        self.assertEquals(hourly_count[1], 2)
        self.assertEquals(hourly_count[2], 1)


if __name__ == '__main__':
    unittest.main()
