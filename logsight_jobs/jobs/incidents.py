import logging.config
from typing import List

from logsight.analytics_core.modules.incidents import IncidentDetector
from logsight_jobs.common.index_job import IndexJob
from logsight_jobs.persistence.dto import IndexInterval

logger = logging.getLogger("logsight." + __name__)


class CalculateIncidentJob(IndexJob):
    def __init__(self, index_interval: IndexInterval, **kwargs):
        super().__init__(index_interval, index_ext="incidents", **kwargs)
        self.incident_detector = IncidentDetector()

    def _calculate(self, logs) -> List:
        return self.incident_detector.calculate_incidents(logs)
