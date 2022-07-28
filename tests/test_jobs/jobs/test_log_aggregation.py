import unittest
from unittest.mock import MagicMock
from datetime import datetime

from logsight.connectors.connectors.elasticsearch import ElasticsearchConfigProperties
from logsight_jobs.jobs.log_aggregation import CalculateLogAggregationJob
from logsight_jobs.persistence.dto import IndexInterval
from logsight.services.elasticsearch_service.elasticsearch_service import ElasticsearchService
from tests.inputs import agg_results, processed_logs


class CalculateLogAggregationTest(unittest.TestCase):
    def test__calculate(self):
        es_config = ElasticsearchConfigProperties(scheme="scheme", host="host", port=9201, username="user",
                                                  password="password")
        es = ElasticsearchService(es_config)
        es.connect = MagicMock()
        es._connect = MagicMock()
        es.get_all_logs_for_index = MagicMock(return_value=processed_logs)

        job = CalculateLogAggregationJob(IndexInterval("log_agg", datetime.min))
        result = job._calculate(es.get_all_logs_for_index("test"))

        self.assertEqual(agg_results, result)
