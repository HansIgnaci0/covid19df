"""Project pipelines."""
from __future__ import annotations

from kedro.framework.project import find_pipelines
from kedro.pipeline import Pipeline


from covid19df.pipelines import eda as eda_pipeline

def register_pipelines():
    return {
        "__default__": eda_pipeline.create_pipeline(),
        "eda": eda_pipeline.create_pipeline(),
    }
