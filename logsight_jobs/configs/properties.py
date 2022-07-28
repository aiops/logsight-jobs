from os.path import dirname, realpath
from pathlib import Path
from typing import Optional

from pydantic import BaseModel
from logsight.configs.properties import ConfigProperties


class IncidentJobConfigProperties(BaseModel):
    interval: Optional[int] = 60
    n_jobs: Optional[int] = 2


class ESCleanupConfigProperties(BaseModel):
    cleanup_age: Optional[str] = "now-1y"
    interval: Optional[int] = 60


@ConfigProperties(prefix="jobs",
                  path=Path(dirname(realpath(__file__))) / "configuration.cfg")
class JobConfigProperties(BaseModel):
    incident_job: Optional[IncidentJobConfigProperties]
    es_cleanup_job: Optional[ESCleanupConfigProperties]
