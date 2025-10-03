"""ナレッジベースのインジェストジョブをトリガーするためのヘルパー関数。"""

from __future__ import annotations

import time
import uuid
from typing import TYPE_CHECKING, Any, Literal

import boto3

if TYPE_CHECKING:
    from collections.abc import Callable

from .config import Settings

settings = Settings()

IngestionStatus = Literal[
    "STARTING",
    "IN_PROGRESS",
    "COMPLETE",
    "FAILED",
    "STOPPING",
    "STOPPED",
]


def start_sync(knowledge_base_id: str, data_source_id: str) -> str:
    """インジェストジョブを開始し、その識別子を返す。"""
    client = boto3.client("bedrock-agent", region_name=settings.AWS_REGION)
    response = client.start_ingestion_job(
        knowledgeBaseId=knowledge_base_id,
        dataSourceId=data_source_id,
        clientToken=str(uuid.uuid4()),
    )
    return response["ingestionJob"]["ingestionJobId"]


def wait_for_sync(
    knowledge_base_id: str,
    data_source_id: str,
    ingestion_job_id: str,
    *,
    poll_seconds: float = 20.0,
    timeout_seconds: float = 3600.0,
    on_update: Callable[[IngestionStatus, dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """インジェストジョブが完了またはタイムアウトするまでポーリングする。"""
    client = boto3.client("bedrock-agent", region_name=settings.AWS_REGION)
    waited = 0.0

    while waited <= timeout_seconds:
        response = client.get_ingestion_job(
            knowledgeBaseId=knowledge_base_id,
            dataSourceId=data_source_id,
            ingestionJobId=ingestion_job_id,
        )
        job = response["ingestionJob"]
        status: IngestionStatus = job["status"]
        if on_update:
            on_update(status, job)
        if status in {"COMPLETE", "FAILED", "STOPPED"}:
            return job
        time.sleep(poll_seconds)
        waited += poll_seconds

    msg = f"Ingestion job {ingestion_job_id} did not finish within {timeout_seconds} seconds"
    raise TimeoutError(
        msg,
    )


def sync_data_source(knowledge_base_id: str, data_source_id: str) -> dict[str, Any]:
    """インジェストジョブを開始し、完了を待つ。"""
    ingestion_job_id = start_sync(knowledge_base_id, data_source_id)
    return wait_for_sync(knowledge_base_id, data_source_id, ingestion_job_id)


def _format_stat(value: int | None) -> str:
    """統計値をフォーマットする。値がNoneの場合は'?'を返す。"""
    return "?" if value is None else str(value)


def main() -> None:
    """メインエントリーポイント。インジェストジョブを開始し、完了まで監視する。"""
    knowledge_base_id = settings.KNOWLEDGE_BASE_ID
    data_source_id = settings.DATA_SOURCE_ID

    if not knowledge_base_id or not data_source_id:
        raise SystemExit(
            "Knowledge base ID and data source ID must be configured in Settings before running sync.",
        )

    print(
        f"Starting ingestion job for knowledge base {knowledge_base_id} and data source {data_source_id}…",
        flush=True,
    )
    ingestion_job_id = start_sync(knowledge_base_id, data_source_id)
    print(
        f"Started ingestion job {ingestion_job_id}. Polling every 20.0 seconds…",
        flush=True,
    )

    last_status: IngestionStatus | None = None

    def on_update(status: IngestionStatus, job: dict[str, Any]) -> None:
        nonlocal last_status
        if status != last_status:
            stats = job.get("statistics", {})
            scanned = _format_stat(stats.get("numberOfDocumentsScanned"))
            failed = _format_stat(stats.get("numberOfDocumentsFailed"))
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            print(
                f"[{timestamp}] status={status} scanned={scanned} failed={failed}",
                flush=True,
            )
            last_status = status

    try:
        final_job = wait_for_sync(
            knowledge_base_id,
            data_source_id,
            ingestion_job_id,
            on_update=on_update,
        )
    except TimeoutError as exc:
        print(str(exc), flush=True)
        raise SystemExit(1) from exc

    status: IngestionStatus = final_job["status"]
    stats = final_job.get("statistics", {})
    failures = final_job.get("failureReasons", []) or []

    print("Ingestion summary:")
    print(f"  status: {status}")
    print(
        "  documents: "
        f"scanned={_format_stat(stats.get('numberOfDocumentsScanned'))} "
        f"indexed_new={_format_stat(stats.get('numberOfNewDocumentsIndexed'))} "
        f"indexed_modified={_format_stat(stats.get('numberOfModifiedDocumentsIndexed'))} "
        f"failed={_format_stat(stats.get('numberOfDocumentsFailed'))}"
    )

    exit_code = 0 if status == "COMPLETE" and not failures else 1
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
