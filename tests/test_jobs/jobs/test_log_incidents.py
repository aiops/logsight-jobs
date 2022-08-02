import unittest
from unittest.mock import MagicMock
from datetime import datetime

from logsight.connectors.connectors.elasticsearch import ElasticsearchConfigProperties
from logsight_jobs.jobs.incidents import CalculateIncidentJob
from logsight_jobs.persistence.dto import IndexInterval
from logsight.services.elasticsearch_service.elasticsearch_service import ElasticsearchService
from tests.inputs import expected_incident_result, processed_logs


class CalculateIncidentsTest(unittest.TestCase):
    def test__calculate(self):
        es_config = ElasticsearchConfigProperties(scheme="scheme", host="host", port=9201, username="user",
                                                  password="password")
        es = ElasticsearchService(es_config)
        es.connect = MagicMock()
        es._connect = MagicMock()
        es.get_all_logs_for_index = MagicMock(return_value=processed_logs, side_effect=[processed_logs])
        job = CalculateIncidentJob(IndexInterval("incidents", datetime.min))
        result = job._calculate(es.get_all_logs_for_index("test"))

        self.assertEqual(expected_incident_result, result)
