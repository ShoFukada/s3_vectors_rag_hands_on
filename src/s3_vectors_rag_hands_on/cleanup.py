from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING

import boto3
from botocore.exceptions import ClientError

from .config import Settings

if TYPE_CHECKING:
    from collections.abc import Iterable

    from botocore.client import BaseClient

settings = Settings()


def _client(service_name: str) -> BaseClient:
    """設定されたリージョンを使用してboto3クライアントを作成する。"""
    return boto3.client(service_name, region_name=settings.AWS_REGION)


@dataclass
class CleanupSummary:
    """最終レポートと終了コードのための結果フラグを収集する。"""

    knowledge_base_deleted: bool | None = None
    data_source_deleted: bool | None = None
    vector_index_deleted: bool | None = None
    vector_bucket_deleted: bool | None = None
    documents_deleted: int = 0
    document_bucket_deleted: bool | None = None
    iam_role_deleted: bool | None = None

    def failures(self) -> list[str]:
        """失敗を表すキーのリストを返す。"""
        failure_keys: list[str] = []
        for key, value in self.__dict__.items():
            if key == "documents_deleted":
                continue
            if value is False:
                failure_keys.append(key)
        return failure_keys


@dataclass
class DataSourceInfo:
    """更新/削除呼び出しに必要なデータソースの情報。"""

    knowledge_base_id: str
    data_source_id: str
    name: str
    configuration: dict
    description: str | None = None


def resolve_knowledge_base_id() -> str | None:
    """設定で提供されたKnowledge Base IDが存在する場合はそれを返す。"""
    kb_id = settings.KNOWLEDGE_BASE_ID
    if not kb_id:
        raise ValueError("Settings.KNOWLEDGE_BASE_ID が未設定です。config.py または .env に ID を記載してください。")

    client = _client("bedrock-agent")
    try:
        client.get_knowledge_base(knowledgeBaseId=kb_id)
    except client.exceptions.ResourceNotFoundException:
        print("[cleanup] WARN  設定済みの Knowledge Base ID は既に削除済みのようです")
        return None

    print(f"[cleanup] 設定値の Knowledge Base ID を利用します: {kb_id}")
    return kb_id


def resolve_data_source(knowledge_base_id: str) -> DataSourceInfo | None:
    """設定のIDを使用してデータソースメタデータを返す(存在する場合)。"""
    data_source_id = settings.DATA_SOURCE_ID
    if not data_source_id:
        raise ValueError("Settings.DATA_SOURCE_ID が未設定です。config.py または .env に ID を記載してください。")

    client = _client("bedrock-agent")
    try:
        response = client.get_data_source(
            knowledgeBaseId=knowledge_base_id,
            dataSourceId=data_source_id,
        )
    except client.exceptions.ResourceNotFoundException:
        print("[cleanup] WARN  設定済みの Data Source ID は既に削除済みのようです")
        return None

    data = response["dataSource"]
    info = DataSourceInfo(
        knowledge_base_id=knowledge_base_id,
        data_source_id=data_source_id,
        name=data["name"],
        configuration=data["dataSourceConfiguration"],
        description=data.get("description"),
    )
    print(f"[cleanup] Data Source を特定しました: {info.data_source_id} (name={info.name})")
    return info


def delete_data_source(info: DataSourceInfo) -> bool | None:
    """ベクトルデータをRETAINモードに設定した後、データソースを削除する。"""
    client = _client("bedrock-agent")
    print(
        f"[cleanup] データソース削除を開始します "
        f"(knowledge_base_id={info.knowledge_base_id}, data_source_id={info.data_source_id})",
        flush=True,
    )

    # Bedrockはデフォルトでベクトルストアをパージしようとする。
    # S3 Vectorsリソースが既に存在しない場合でも削除が成功するようにRETAINに上書きする。
    update_kwargs = {
        "knowledgeBaseId": info.knowledge_base_id,
        "dataSourceId": info.data_source_id,
        "name": info.name,
        "dataSourceConfiguration": info.configuration,
        "dataDeletionPolicy": "RETAIN",
    }
    if info.description is not None:
        update_kwargs["description"] = info.description

    try:
        client.update_data_source(**update_kwargs)
        print("[cleanup] dataDeletionPolicy を RETAIN に更新しました")
    except client.exceptions.ResourceNotFoundException:
        print("[cleanup] WARN  データソースが見つからなかったため更新をスキップします")
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code")
        print(f"[cleanup] WARN  dataDeletionPolicy の更新に失敗しましたが削除を試みます: {code}")
        print(f"[cleanup] WARN  {error}")

    try:
        client.delete_data_source(
            knowledgeBaseId=info.knowledge_base_id,
            dataSourceId=info.data_source_id,
        )
    except client.exceptions.ResourceNotFoundException:
        print("[cleanup] WARN  データソースは既に削除済みでした")
        return None
    except client.exceptions.ConflictException:
        print("[cleanup] WARN  データソースの削除処理が既に進行中です")
        return True
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code")
        print(f"[cleanup] ERROR データソース削除に失敗しました: {code}")
        print(f"[cleanup] ERROR {error}")
        return False
    else:
        print("[cleanup] データソース削除完了")
        return True


def delete_knowledge_base(knowledge_base_id: str) -> bool | None:
    """Knowledge Baseを削除する。"""
    client = _client("bedrock-agent")
    print(f"[cleanup] Knowledge Base 削除を開始します (id={knowledge_base_id})")
    try:
        client.delete_knowledge_base(knowledgeBaseId=knowledge_base_id)
    except client.exceptions.ResourceNotFoundException:
        print("[cleanup] WARN  Knowledge Base は既に削除済みでした")
        return None
    except client.exceptions.ConflictException:
        print("[cleanup] WARN  Knowledge Base の削除処理が既に進行中です")
        return True
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code")
        print(f"[cleanup] ERROR Knowledge Base 削除に失敗しました: {code}")
        print(f"[cleanup] ERROR {error!s}")
        return False
    else:
        print("[cleanup] Knowledge Base 削除完了")
        return True


def delete_vector_index() -> bool | None:
    """S3 Vectorsインデックスを削除する。"""
    client = _client("s3vectors")
    print(
        f"[cleanup] S3 Vectors インデックス削除を開始します "
        f"(bucket={settings.VECTOR_BUCKET_NAME}, index={settings.VECTOR_INDEX_NAME})",
        flush=True,
    )
    try:
        client.delete_index(
            vectorBucketName=settings.VECTOR_BUCKET_NAME,
            indexName=settings.VECTOR_INDEX_NAME,
        )
    except client.exceptions.NotFoundException:
        print("[cleanup] WARN  S3 Vectors インデックスは既に存在しませんでした")
        return None
    except client.exceptions.ConflictException:
        print("[cleanup] WARN  S3 Vectors インデックス削除が競合しました (再試行で解消する場合があります)")
        return False
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code")
        print(f"[cleanup] ERROR S3 Vectors インデックス削除に失敗しました: {code}")
        print(f"[cleanup] ERROR {error!s}")
        return False
    else:
        print("[cleanup] S3 Vectors インデックス削除完了")
        return True


def delete_vector_bucket() -> bool | None:
    """S3 Vectorsバケットを削除する。"""
    client = _client("s3vectors")
    print(f"[cleanup] S3 Vectors バケット削除を開始します (name={settings.VECTOR_BUCKET_NAME})")
    try:
        client.delete_vector_bucket(vectorBucketName=settings.VECTOR_BUCKET_NAME)
    except client.exceptions.NotFoundException:
        print("[cleanup] WARN  S3 Vectors バケットは既に存在しませんでした")
        return None
    except client.exceptions.ConflictException:
        print("[cleanup] WARN  S3 Vectors バケット削除が競合しました。残っているインデックスがないか確認してください")
        return False
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code")
        print(f"[cleanup] ERROR S3 Vectors バケット削除に失敗しました: {code}")
        print(f"[cleanup] ERROR {error!s}")
        return False
    else:
        print("[cleanup] S3 Vectors バケット削除完了")
        return True


def _chunk_delete(keys: Iterable[str]) -> None:
    """S3オブジェクトを一括削除する。"""
    s3 = _client("s3")
    objects = [{"Key": key} for key in keys]
    if objects:
        s3.delete_objects(Bucket=settings.DOCUMENT_S3_BUCKET, Delete={"Objects": objects})


def empty_document_bucket() -> int:
    """ドキュメントバケット内の全オブジェクトを削除する。"""
    s3 = _client("s3")
    print(f"[cleanup] S3 ドキュメントバケット内のオブジェクトを削除します: {settings.DOCUMENT_S3_BUCKET}")

    deleted = 0
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=settings.DOCUMENT_S3_BUCKET):
            keys = [obj["Key"] for obj in page.get("Contents", [])]
            if not keys:
                continue
            _chunk_delete(keys)
            deleted += len(keys)
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code")
        if code == "NoSuchBucket":
            print("[cleanup] WARN  S3 ドキュメントバケットは既に存在しませんでした")
            return 0
        print(f"[cleanup] ERROR S3 ドキュメントバケット内の削除に失敗しました: {code}")
        print(f"[cleanup] ERROR {error!s}")
        return deleted

    if deleted == 0:
        print("[cleanup] 削除対象のオブジェクトはありませんでした")
    else:
        print(f"[cleanup] {deleted} 件のオブジェクトを削除しました")
    return deleted


def delete_document_bucket() -> bool | None:
    """ドキュメントバケットを削除する。"""
    s3 = _client("s3")
    print(f"[cleanup] S3 ドキュメントバケット削除を開始します: {settings.DOCUMENT_S3_BUCKET}")
    try:
        s3.delete_bucket(Bucket=settings.DOCUMENT_S3_BUCKET)
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code")
        if code == "NoSuchBucket":
            print("[cleanup] WARN  S3 ドキュメントバケットは既に存在しませんでした")
            return None
        if code == "BucketNotEmpty":
            print("[cleanup] WARN  S3 ドキュメントバケットにオブジェクトが残っているため削除できませんでした")
            return False
        print(f"[cleanup] ERROR S3 ドキュメントバケット削除に失敗しました: {code}")
        print(f"[cleanup] ERROR {error!s}")
        return False
    else:
        print("[cleanup] S3 ドキュメントバケット削除完了")
        return True


def delete_iam_role() -> bool | None:
    """Bedrock Knowledge Base用のIAMロールを削除する。"""
    iam = _client("iam")
    role_name = settings.BEDROCK_ROLE_NAME
    print(f"[cleanup] IAMロール削除を開始します (role={role_name})")

    try:
        # まず、インラインポリシーを全て削除する
        try:
            policy_response = iam.list_role_policies(RoleName=role_name)
            for policy_name in policy_response.get("PolicyNames", []):
                iam.delete_role_policy(RoleName=role_name, PolicyName=policy_name)
                print(f"[cleanup] インラインポリシー削除: {policy_name}")
        except iam.exceptions.NoSuchEntityException:
            print("[cleanup] WARN  IAMロールは既に削除済みでした")
            return None

        # 次に、アタッチされたマネージドポリシーを全てデタッチする
        attached_policies = iam.list_attached_role_policies(RoleName=role_name)
        for policy in attached_policies.get("AttachedPolicies", []):
            iam.detach_role_policy(RoleName=role_name, PolicyArn=policy["PolicyArn"])
            print(f"[cleanup] マネージドポリシーをデタッチ: {policy['PolicyName']}")

        # 最後にロールを削除する
        iam.delete_role(RoleName=role_name)
    except iam.exceptions.NoSuchEntityException:
        print("[cleanup] WARN  IAMロールは既に削除済みでした")
        return None
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code")
        print(f"[cleanup] ERROR IAMロール削除に失敗しました: {code}")
        print(f"[cleanup] ERROR {error!s}")
        return False
    else:
        print("[cleanup] IAMロール削除完了")
        return True


def cleanup_all() -> CleanupSummary:
    """全てのリソースをクリーンアップする。"""
    print("[cleanup] クリーンアップを開始します (全リソース削除モード / ドキュメント含む)")

    summary = CleanupSummary()

    knowledge_base_id: str | None
    try:
        knowledge_base_id = resolve_knowledge_base_id()
    except ValueError as exc:
        print(f"[cleanup] ERROR {exc!s}")
        knowledge_base_id = None
        summary.knowledge_base_deleted = False

    if knowledge_base_id:
        data_source_info: DataSourceInfo | None
        try:
            data_source_info = resolve_data_source(knowledge_base_id)
        except ValueError as exc:
            print(f"[cleanup] ERROR {exc!s}")
            data_source_info = None
            summary.data_source_deleted = False

        if data_source_info:
            summary.data_source_deleted = delete_data_source(data_source_info)
        else:
            print("[cleanup] Data Source: 設定された ID は既に削除済みとみなします")

        summary.knowledge_base_deleted = delete_knowledge_base(knowledge_base_id)
    else:
        print("[cleanup] Knowledge Base: 設定された ID は既に削除済みとみなします")

    summary.vector_index_deleted = delete_vector_index()
    summary.vector_bucket_deleted = delete_vector_bucket()
    summary.documents_deleted = empty_document_bucket()
    summary.document_bucket_deleted = delete_document_bucket()
    summary.iam_role_deleted = delete_iam_role()

    label_map = {
        "knowledge_base_deleted": "Knowledge Base",
        "data_source_deleted": "Data Source",
        "vector_index_deleted": "S3 Vectors インデックス",
        "vector_bucket_deleted": "S3 Vectors バケット",
        "document_bucket_deleted": "S3 ドキュメントバケット",
        "iam_role_deleted": "IAMロール",
    }

    for key, label in label_map.items():
        value = getattr(summary, key)
        if value is True:
            print(f"[cleanup] {label}: 削除済み")
        elif value is False:
            print(f"[cleanup] WARN  {label}: 削除に失敗しました")
        else:
            print(f"[cleanup] {label}: スキップまたは不要でした")

    print(f"[cleanup] 削除したドキュメント数: {summary.documents_deleted}")

    print("[cleanup] クリーンアップ完了")
    return summary


def main(argv: list[str]) -> int:  # noqa: ARG001
    """クリーンアップのメインエントリーポイント。"""
    summary = cleanup_all()

    failures = summary.failures()
    if failures:
        print("[cleanup] WARN  一部のリソースで削除が失敗しました: " + ", ".join(sorted(failures)))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
