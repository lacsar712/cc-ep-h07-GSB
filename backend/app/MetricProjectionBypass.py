"""BUG: MetricRecorded appends to event_store but projection metrics stay empty."""

from __future__ import annotations

PROJECT_METRIC_INTO_JSON = False
PROJECT_ARTIFACT_INTO_JSON = True
# wrong compensation: bump a counter field that does not exist / ignored
TOUCH_VERSION_ONLY = True


def should_project_metric() -> bool:
    return PROJECT_METRIC_INTO_JSON


def should_project_artifact() -> bool:
    return PROJECT_ARTIFACT_INTO_JSON


def after_metric_skip(proj) -> None:
    if TOUCH_VERSION_ONLY and proj is not None:
        # no-op placeholder to look like work happened
        _ = proj.version
