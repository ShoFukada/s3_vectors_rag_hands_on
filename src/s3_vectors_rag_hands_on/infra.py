from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError

from .config import Settings

settings = Settings()


@dataclass(slots=True)
class KnowledgeBaseResources:
    knowledge_base_id: str
    data_source_id: str
    vector_bucket_arn: str
    vector_index_arn: str


def ensure_document_bucket() -> str:
    """ドキュメント用のS3バケットが存在しない場合は作成する。

    データソースの作成時に必要なバケットARNを返す。
    """
    s3 = boto3.client("s3", region_name=settings.AWS_REGION)
    try:
        s3.head_bucket(Bucket=settings.DOCUMENT_S3_BUCKET)
    except ClientError as error:
        error_code = error.response.get("Error", {}).get("Code")
        if error_code not in {"404", "NoSuchBucket", "NotFound"}:
            raise
        create_kwargs: dict[str, Any] = {"Bucket": settings.DOCUMENT_S3_BUCKET}
        if settings.AWS_REGION != "us-east-1":
            create_kwargs["CreateBucketConfiguration"] = {
                "LocationConstraint": settings.AWS_REGION,
            }
        s3.create_bucket(**create_kwargs)

    return f"arn:aws:s3:::{settings.DOCUMENT_S3_BUCKET}"


def upload_sample_documents() -> None:
    """ローカルのサンプルコーパス(ドキュメント + サイドカーメタデータファイル)をアップロードする。"""
    local_dir = Path(settings.LOCAL_DATA_DIR)
    if not local_dir.exists():
        msg = f"Local data directory not found: {local_dir}"
        raise FileNotFoundError(msg)

    s3 = boto3.client("s3", region_name=settings.AWS_REGION)
    for path in local_dir.rglob("*"):
        if not path.is_file():
            continue

        relative_key = path.relative_to(local_dir).as_posix()
        s3_key = f"{settings.DOCUMENT_S3_PREFIX}{relative_key}".replace("//", "/")
        s3.upload_file(str(path), settings.DOCUMENT_S3_BUCKET, s3_key)


def ensure_vector_bucket_and_index() -> tuple[str, str]:
    """S3 Vectorsバケットとインデックスが存在することを確認する。

    タプル ``(vector_bucket_arn, vector_index_arn)`` を返す。
    """
    vectors = boto3.client("s3vectors", region_name=settings.AWS_REGION)

    try:
        bucket_response = vectors.get_vector_bucket(
            vectorBucketName=settings.VECTOR_BUCKET_NAME,
        )
    except vectors.exceptions.NotFoundException:
        vectors.create_vector_bucket(
            vectorBucketName=settings.VECTOR_BUCKET_NAME,
        )
        bucket_response = vectors.get_vector_bucket(
            vectorBucketName=settings.VECTOR_BUCKET_NAME,
        )

    vector_bucket = bucket_response["vectorBucket"]
    vector_bucket_arn: str = vector_bucket["vectorBucketArn"]

    try:
        index_response = vectors.get_index(
            vectorBucketName=settings.VECTOR_BUCKET_NAME,
            indexName=settings.VECTOR_INDEX_NAME,
        )
    except vectors.exceptions.NotFoundException:
        vectors.create_index(
            vectorBucketName=settings.VECTOR_BUCKET_NAME,
            indexName=settings.VECTOR_INDEX_NAME,
            dataType="float32",
            dimension=settings.BEDROCK_EMBEDDING_DIMENSION,
            distanceMetric="cosine",
        )
        index_response = vectors.get_index(
            vectorBucketName=settings.VECTOR_BUCKET_NAME,
            indexName=settings.VECTOR_INDEX_NAME,
        )

    return vector_bucket_arn, index_response["index"]["indexArn"]


def ensure_bedrock_kb_role(
    document_bucket_arn: str,
    vector_bucket_arn: str,
    vector_index_arn: str,
) -> str:
    """Bedrock Knowledge Base用のIAMロールを作成または取得する。

    ロールを最新のポリシーに更新し、ARNを返す。
    """
    iam = boto3.client("iam")
    role_name = settings.BEDROCK_ROLE_NAME

    try:
        response = iam.get_role(RoleName=role_name)
        role_arn = response["Role"]["Arn"]
    except iam.exceptions.NoSuchEntityException:
        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "bedrock.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }

        role_response = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Role for Bedrock Knowledge Base to access S3 and models",
        )

        role_arn = role_response["Role"]["Arn"]

    s3_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3:GetObject", "s3:ListBucket"],
                "Resource": [
                    document_bucket_arn,
                    f"{document_bucket_arn}/*",
                ],
            }
        ],
    }
    iam.put_role_policy(
        RoleName=role_name,
        PolicyName="S3Access",
        PolicyDocument=json.dumps(s3_policy),
    )

    s3vectors_resources = [
        vector_bucket_arn,
        f"{vector_bucket_arn}/*",
    ]
    if vector_index_arn:
        s3vectors_resources.append(vector_index_arn)
        s3vectors_resources.append(f"{vector_bucket_arn}/index/*")

    s3vectors_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3vectors:*"],
                "Resource": s3vectors_resources,
            }
        ],
    }
    iam.put_role_policy(
        RoleName=role_name,
        PolicyName="S3VectorsAccess",
        PolicyDocument=json.dumps(s3vectors_policy),
    )

    bedrock_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["bedrock:InvokeModel"],
                "Resource": "*",
            }
        ],
    }
    iam.put_role_policy(
        RoleName=role_name,
        PolicyName="BedrockModelAccess",
        PolicyDocument=json.dumps(bedrock_policy),
    )

    return role_arn


def get_or_create_knowledge_base(
    vector_bucket_arn: str,
    vector_index_arn: str,
    role_arn: str,
) -> str:
    """ナレッジベースが存在しない場合は作成する。"""
    bedrock_agents = boto3.client("bedrock-agent", region_name=settings.AWS_REGION)

    paginator = bedrock_agents.get_paginator("list_knowledge_bases")
    for page in paginator.paginate():
        for kb in page.get("knowledgeBaseSummaries", []):
            if kb.get("name") == settings.KNOWLEDGE_BASE_NAME:
                return kb["knowledgeBaseId"]

    response = bedrock_agents.create_knowledge_base(
        name=settings.KNOWLEDGE_BASE_NAME,
        roleArn=role_arn,
        knowledgeBaseConfiguration={
            "type": "VECTOR",
            "vectorKnowledgeBaseConfiguration": {
                "embeddingModelArn": settings.BEDROCK_EMBEDDING_MODEL_ARN,
            },
        },
        storageConfiguration={
            "type": "S3_VECTORS",
            "s3VectorsConfiguration": {
                "vectorBucketArn": vector_bucket_arn,
                "indexArn": vector_index_arn,
            },
        },
    )
    return response["knowledgeBase"]["knowledgeBaseId"]


def get_or_create_data_source(knowledge_base_id: str, document_bucket_arn: str) -> str:
    """必要に応じてS3データソースをナレッジベースにアタッチする。"""
    bedrock_agents = boto3.client("bedrock-agent", region_name=settings.AWS_REGION)
    paginator = bedrock_agents.get_paginator("list_data_sources")
    for page in paginator.paginate(knowledgeBaseId=knowledge_base_id):
        for data_source in page.get("dataSourceSummaries", []):
            if data_source.get("name") == "s3-sample-documents":
                return data_source["dataSourceId"]

    response = bedrock_agents.create_data_source(
        knowledgeBaseId=knowledge_base_id,
        name="s3-sample-documents",
        description="Sample documents uploaded from data/input",
        dataSourceConfiguration={
            "type": "S3",
            "s3Configuration": {
                "bucketArn": document_bucket_arn,
                "inclusionPrefixes": [settings.DOCUMENT_S3_PREFIX],
            },
        },
    )
    return response["dataSource"]["dataSourceId"]


def _provision_document_bucket() -> str:
    try:
        document_bucket_arn = ensure_document_bucket()
    except Exception as exc:
        print(f"[FAIL] document bucket: {exc}")
        raise
    else:
        print("[SUCCESS] document bucket")
    return document_bucket_arn


def _provision_sample_documents() -> None:
    try:
        upload_sample_documents()
    except Exception as exc:
        print(f"[FAIL] sample document upload: {exc}")
        raise
    else:
        print("[SUCCESS] sample document upload")


def _provision_bedrock_kb_role(
    document_bucket_arn: str,
    vector_bucket_arn: str,
    vector_index_arn: str,
) -> str:
    try:
        role_arn = ensure_bedrock_kb_role(
            document_bucket_arn=document_bucket_arn,
            vector_bucket_arn=vector_bucket_arn,
            vector_index_arn=vector_index_arn,
        )
    except Exception as exc:
        print(f"[FAIL] Bedrock knowledge base role: {exc}")
        raise
    else:
        print("[SUCCESS] Bedrock knowledge base role")
    return role_arn


def _provision_vector_bucket_and_index() -> tuple[str, str]:
    try:
        vector_bucket_arn, vector_index_arn = ensure_vector_bucket_and_index()
    except Exception as exc:
        print(f"[FAIL] S3 Vectors bucket and index: {exc}")
        raise
    else:
        print("[SUCCESS] S3 Vectors bucket and index")
    return vector_bucket_arn, vector_index_arn


def _provision_knowledge_base(vector_bucket_arn: str, vector_index_arn: str, role_arn: str) -> str:
    try:
        knowledge_base_id = get_or_create_knowledge_base(
            vector_bucket_arn=vector_bucket_arn,
            vector_index_arn=vector_index_arn,
            role_arn=role_arn,
        )
    except Exception as exc:
        print(f"[FAIL] knowledge base: {exc}")
        raise
    else:
        print("[SUCCESS] knowledge base")
    return knowledge_base_id


def _provision_data_source(knowledge_base_id: str, document_bucket_arn: str) -> str:
    try:
        data_source_id = get_or_create_data_source(
            knowledge_base_id=knowledge_base_id,
            document_bucket_arn=document_bucket_arn,
        )
    except Exception as exc:
        print(f"[FAIL] data source: {exc}")
        raise
    else:
        print("[SUCCESS] data source")
    return data_source_id


def provision_all() -> KnowledgeBaseResources:
    document_bucket_arn = _provision_document_bucket()
    _provision_sample_documents()
    vector_bucket_arn, vector_index_arn = _provision_vector_bucket_and_index()
    role_arn = _provision_bedrock_kb_role(
        document_bucket_arn=document_bucket_arn,
        vector_bucket_arn=vector_bucket_arn,
        vector_index_arn=vector_index_arn,
    )
    knowledge_base_id = _provision_knowledge_base(vector_bucket_arn, vector_index_arn, role_arn)
    data_source_id = _provision_data_source(knowledge_base_id, document_bucket_arn)
    return KnowledgeBaseResources(
        knowledge_base_id=knowledge_base_id,
        data_source_id=data_source_id,
        vector_bucket_arn=vector_bucket_arn,
        vector_index_arn=vector_index_arn,
    )


if __name__ == "__main__":
    resources = provision_all()
    print(json.dumps(asdict(resources), indent=2))
