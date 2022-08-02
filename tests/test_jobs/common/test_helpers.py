from datetime import datetime
import pytest

from logsight_jobs.common.index_job import IndexJobResult
from logsight_jobs.persistence.dto import IndexInterval


@pytest.fixture
def job_result():
    return IndexJobResult(IndexInterval("index", datetime.min), "table")
