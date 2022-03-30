# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import time
import uuid
from unittest.mock import patch

from openlineage.client.constants import DEFAULT_NAMESPACE_NAME
from openlineage.client.run import RunEvent, RunState, Run, Job

from openlineage.dagster.adapter import OpenLineageAdapter
from .conftest import PRODUCER


@patch("openlineage.dagster.adapter.to_utc_iso_8601")
@patch("openlineage.dagster.adapter.OpenLineageClient.emit")
def test_start_pipeline_run(mock_client_emit, mock_to_utc_iso_8601):
    event_time = "2022-01-01T00:00:00.000000Z"
    mock_to_utc_iso_8601.return_value = event_time

    pipeline_name = "a_pipeline"
    pipeline_run_id = str(uuid.uuid4())
    timestamp = time.time()

    adapter = OpenLineageAdapter()
    adapter.start_pipeline(pipeline_name, pipeline_run_id, timestamp)

    mock_to_utc_iso_8601.assert_called_once_with(timestamp)
    mock_client_emit.assert_called_once_with(
        RunEvent(
            eventType=RunState.START,
            eventTime=event_time,
            run=Run(
                runId=pipeline_run_id,
                facets={}
            ),
            job=Job(
                namespace=DEFAULT_NAMESPACE_NAME,
                name=pipeline_name,
                facets={}
            ),
            producer=PRODUCER,
            inputs=[],
            outputs=[]
        )
    )


@patch("openlineage.dagster.adapter.to_utc_iso_8601")
@patch("openlineage.dagster.adapter.OpenLineageClient.emit")
def test_complete_pipeline_run(mock_client_emit, mock_to_utc_iso_8601):
    event_time = "2022-01-01T00:00:00.000000Z"
    mock_to_utc_iso_8601.return_value = event_time

    pipeline_name = "a_pipeline"
    pipeline_run_id = str(uuid.uuid4())
    timestamp = time.time()

    adapter = OpenLineageAdapter()
    adapter.complete_pipeline(pipeline_name, pipeline_run_id, timestamp)

    mock_to_utc_iso_8601.assert_called_once_with(timestamp)
    mock_client_emit.assert_called_once_with(
        RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=event_time,
            run=Run(
                runId=pipeline_run_id,
                facets={}
            ),
            job=Job(
                namespace=DEFAULT_NAMESPACE_NAME,
                name=pipeline_name,
                facets={}
            ),
            producer=PRODUCER,
            inputs=[],
            outputs=[]
        )
    )


@patch("openlineage.dagster.adapter.to_utc_iso_8601")
@patch("openlineage.dagster.adapter.OpenLineageClient.emit")
def test_fail_pipeline_run(mock_client_emit, mock_to_utc_iso_8601):
    event_time = "2022-01-01T00:00:00.000000Z"
    mock_to_utc_iso_8601.return_value = event_time

    pipeline_name = "a_pipeline"
    pipeline_run_id = str(uuid.uuid4())
    timestamp = time.time()

    adapter = OpenLineageAdapter()
    adapter.fail_pipeline(pipeline_name, pipeline_run_id, timestamp)

    mock_to_utc_iso_8601.assert_called_once_with(timestamp)
    mock_client_emit.assert_called_once_with(
        RunEvent(
            eventType=RunState.FAIL,
            eventTime=event_time,
            run=Run(
                runId=pipeline_run_id,
                facets={}
            ),
            job=Job(
                namespace=DEFAULT_NAMESPACE_NAME,
                name=pipeline_name,
                facets={}
            ),
            producer=PRODUCER,
            inputs=[],
            outputs=[]
        )
    )


@patch("openlineage.dagster.adapter.to_utc_iso_8601")
@patch("openlineage.dagster.adapter.OpenLineageClient.emit")
def test_cancel_pipeline_run(mock_client_emit, mock_to_utc_iso_8601):
    event_time = "2022-01-01T00:00:00.000000Z"
    mock_to_utc_iso_8601.return_value = event_time

    pipeline_name = "a_pipeline"
    pipeline_run_id = str(uuid.uuid4())
    timestamp = time.time()

    adapter = OpenLineageAdapter()
    adapter.cancel_pipeline(pipeline_name, pipeline_run_id, timestamp)

    mock_to_utc_iso_8601.assert_called_once_with(timestamp)
    mock_client_emit.assert_called_once_with(
        RunEvent(
            eventType=RunState.ABORT,
            eventTime=event_time,
            run=Run(
                runId=pipeline_run_id,
                facets={}
            ),
            job=Job(
                namespace=DEFAULT_NAMESPACE_NAME,
                name=pipeline_name,
                facets={}
            ),
            producer=PRODUCER,
            inputs=[],
            outputs=[]
        )
    )
