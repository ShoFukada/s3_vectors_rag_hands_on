import os
from pathlib import Path
from typing import Any

import boto3
from pydantic_settings import BaseSettings, SettingsConfigDict


def _get_aws_account_id() -> str:
    """Get AWS account ID from STS."""
    try:
        sts = boto3.client("sts")
        return sts.get_caller_identity()["Account"]
    except Exception:  # noqa: BLE001
        return "unknown"


def find_env_file() -> str | None:
    """プロジェクトルートの.envファイルを再帰的に探す"""
    current_path: Path = Path(__file__).resolve()

    for parent in current_path.parents:
        env_file: Path = parent / ".env"
        if env_file.exists():
            return str(env_file)

    return None


class Settings(BaseSettings):

    model_config = SettingsConfigDict(
        env_file=find_env_file(),
        env_file_encoding="utf-8",
    )

    AWS_REGION: str
    AWS_PROFILE: str | None = None
    AWS_ACCESS_KEY_ID: str | None = None
    AWS_SECRET_ACCESS_KEY: str | None = None
    AWS_SESSION_TOKEN: str | None = None
    DOCUMENT_S3_BUCKET: str | None = None
    DOCUMENT_S3_PREFIX: str = "knowledge-base/documents/"
    VECTOR_BUCKET_NAME: str | None = None
    VECTOR_INDEX_NAME: str | None = None
    BEDROCK_EMBEDDING_MODEL_ARN: str = "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-embed-text-v2:0"
    BEDROCK_EMBEDDING_DIMENSION: int = 1024
    KNOWLEDGE_BASE_NAME: str = "s3-vectors-rag-hands-on"
    BEDROCK_RESPONSE_MODEL_ARN: str = (
        "arn:aws:bedrock:us-east-1:239339588912:inference-profile/global.anthropic.claude-sonnet-4-5-20250929-v1:0"
    )
    LOCAL_DATA_DIR: str = "data/input"
    BEDROCK_ROLE_NAME: str = "BedrockKnowledgeBaseRole"
    KNOWLEDGE_BASE_ID: str | None  # infra.provision_all() の出力をenvから渡す
    DATA_SOURCE_ID: str | None  # infra.provision_all() の出力をenvから渡す

    def __init__(self, **data: Any) -> None:  # noqa: ANN401
        super().__init__(**data)

        account_id = _get_aws_account_id()
        if self.DOCUMENT_S3_BUCKET is None:
            self.DOCUMENT_S3_BUCKET = f"s3-vectors-rag-hands-on-documents-{account_id}"
        if self.VECTOR_BUCKET_NAME is None:
            self.VECTOR_BUCKET_NAME = f"s3-vectors-rag-hands-on-vectors-{account_id}"
        if self.VECTOR_INDEX_NAME is None:
            self.VECTOR_INDEX_NAME = f"s3-vectors-rag-hands-on-index-{account_id}"

        for field_name, field_value in self.model_dump().items():
            if field_value is not None:
                os.environ[field_name] = str(field_value)
