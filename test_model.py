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

        count, hours_seen = models.get_hourly_responses_count("/route1", 200)
        self.assertEquals(len(count), 0)
        self.assertEquals(len(hours_seen), 0)

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

    def test_wrong_status(self):
        models.record_occurrences_from_requests("20200101_11", 200,
                                                "/route1", 100)
        models.record_log_data_received("20200101_11")

        self.assertEquals(models.get_responses_count(
            "/route1", 400, "20200101_11"), 0)
        models.get_hourly_responses_count("/route1", 400)

        hours_seen, count = models.get_hourly_responses_count("/route1", 400)
        self.assertEquals(len(hours_seen), 0)
        self.assertEquals(len(count), 0)

    def test_wrong_route(self):
        models.record_occurrences_from_requests("20200101_11", 200,
                                                "/route1", 15)
        models.record_log_data_received("20200101_11")

        self.assertEquals(models.get_responses_count(
            "/route2", 200, "20200101_11"), 0)

        hours_seen, count = models.get_hourly_responses_count("/route2", 200)
        self.assertEquals(len(hours_seen), 0)
        self.assertEquals(len(count), 0)

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

        hours_seen, count = models.get_hourly_responses_count("/route1", 200)
        self.assertEquals(len(hours_seen), 3)
        self.assertEquals(len(count), 3)

        # We expect the counts to be ordered from earliest to latest date.
        self.assertEquals(count[0], 3)
        self.assertEquals(count[1], 2)
        self.assertEquals(count[2], 1)


class TestParseMessage(unittest.TestCase):
    def test_simple(self):
        # TODO(benkraft): Test stacktrace parsing.
        error_def, _, __ = models._parse_message(
            'Error on line 214: File not found', '200', '3')
        error_def.pop('key')  # We don't check the key.
        self.assertEqual(
            error_def, {
                'title': 'Error on line 214: File not found',
                'status': '200',
                'level': '3',
                'id0': '200 3 Error on line %%: File not found',
                'id1': '200 3 Error on line',
                'id2': '200 3 File not found',
                'id3': None,
            })

    def test_no_attribute(self):
        error_def, _, __ = models._parse_message(
            "'NoneType' object has no attribute 'some_attribute'", '500', '3')
        error_def.pop('key')
        self.assertEqual(
            error_def, {
                'title': "'NoneType' object has no attribute 'some_attribute'",
                'status': '500',
                'level': '3',
                'id0': ("500 3 'NoneType' object has no attribute "
                        "'some_attribute'"),
                'id1': None,
                'id2': None,
                'id3': None,
            })

        error_def, _, __ = models._parse_message(
            "'User' object has no attribute '_User__email'", '500', '3')
        error_def.pop('key')
        self.assertEqual(
            error_def, {
                'title': "'User' object has no attribute '_User__email'",
                'status': '500',
                'level': '3',
                'id0': ("500 3 'User' object has no attribute '_User__email'"),
                'id1': None,
                'id2': None,
                'id3': None,
            })

    def test_sig_error(self):
        error_def, _, __ = models._parse_message(
            "Error in signature for "
            "/api/internal/user/progress_summary[methods=['GET']]. "
            "Reverting to jsonify.", '200', '3')
        error_def.pop('key')
        self.assertEqual(
            error_def, {
                'title': ("Error in signature for "
                          "/api/internal/user/progress_summary"
                          "[methods=['GET']]. Reverting to jsonify."),
                'status': '200',
                'level': '3',
                'id0': ("200 3 Error in signature for "
                        "/api/internal/user/progress_summary"
                        "[methods=['GET']]. Reverting to jsonify."),
                'id1': None,
                'id2': None,
                'id3': None,
            })

    def test_memcache_set_error(self):
        # Function with no args
        error_def, _, __ = models._parse_message(
            "Memcache set failed for content.exercise_models._name_table",
            '200', '3')
        error_def.pop('key')
        self.assertEqual(
            error_def, {
                'title': ("Memcache set failed for "
                          "content.exercise_models._name_table"),
                'status': '200',
                'level': '3',
                'id0': ("200 3 Memcache set failed for "
                          "content.exercise_models._name_table"),
                'id1': None,
                'id2': None,
                'id3': ("Memcache set failed for "
                        "content.exercise_models._name_table"),
            })

        # Function with args
        error_def, _, __ = models._parse_message(
            "Memcache set failed for "
            "prediction.models.get_model_for_job_and_exercise."
            "('23aaed', u'some-topic-slug')",
            '200', '3')
        error_def.pop('key')
        self.assertEqual(
            error_def, {
                'title': ("Memcache set failed for "
                          "prediction.models.get_model_for_job_and_exercise."
                          "('23aaed', u'some-topic-slug')"),
                'status': '200',
                'level': '3',
                'id0': ("200 3 Memcache set failed for "
                        "prediction.models.get_model_for_job_and_exercise."
                        "('%%aaed', u'some-topic-slug')"),
                'id1': None,
                'id2': None,
                'id3': ("Memcache set failed for "
                        "prediction.models.get_model_for_job_and_exercise."),
            })
        # Function with single string key param
        error_def, _, __ = models._parse_message(
            "Memcache set failed for "
            "bibliotron.bibliotron_util.count_practiceable_content.'ag5zfm'",
            '200', '3')
        error_def.pop('key')
        self.assertEqual(
            error_def, {
                'title': ("Memcache set failed for "
                          "bibliotron.bibliotron_util."
                          "count_practiceable_content.'ag5zfm'"),
                'status': '200',
                'level': '3',
                'id0': ("200 3 Memcache set failed for "
                        "bibliotron.bibliotron_util."
                        "count_practiceable_content.'ag%%zfm'"),
                'id1': None,
                'id2': None,
                'id3': ("Memcache set failed for "
                        "bibliotron.bibliotron_util."
                        "count_practiceable_content."),
            })


if __name__ == '__main__':
    unittest.main()
