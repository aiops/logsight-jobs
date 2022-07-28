import unittest
from unittest import mock
from unittest.mock import MagicMock

from logsight.connectors.connectors.elasticsearch import ElasticsearchConfigProperties
from logsight_jobs.jobs.delete_es_data_job import DeleteESDataJob
from logsight.services.elasticsearch_service.elasticsearch_service import ElasticsearchService
from logsight.services.service_provider import ServiceProvider


class CalculateIncidentsTest(unittest.TestCase):
    def test__execute(self):
        cleanup_age = "now-1y"
        es_config = ElasticsearchConfigProperties(scheme="scheme", host="host", port=9201, username="user",
                                                  password="password")
        es = ElasticsearchService(es_config)
        es.connect = MagicMock()
        es._connect = MagicMock()
        es.delete_by_ingest_timestamp = MagicMock()
        ServiceProvider.provide_elasticsearch = MagicMock(return_value=es)

        job = DeleteESDataJob()
        job.execute()
        self.assertEqual(job.cleanup_age, cleanup_age)
        calls = [mock.call("*_pipeline", start_time="now-15y", end_time=cleanup_age),
                 mock.call("*_incidents", start_time="now-15y", end_time=cleanup_age)]
        es.delete_by_ingest_timestamp.assert_has_calls(calls, any_order=False)
        self.assertEqual(es.delete_by_ingest_timestamp.call_count, 2)
