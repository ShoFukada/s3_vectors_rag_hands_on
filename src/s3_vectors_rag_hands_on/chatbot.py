from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import boto3

from .config import Settings

settings = Settings()


def equals_filter(key: str, value: str | float) -> dict[str, dict[str, str | bool | int | float]]:
    """Create an ``equals`` metadata filter clause for a knowledge base query."""
    return {"equals": {"key": key, "value": value}}


def equals_filter_bool(key: str, *, value: bool) -> dict[str, dict[str, str | bool | int | float]]:
    """Create an ``equals`` metadata filter clause with a boolean value."""
    return {"equals": {"key": key, "value": value}}


def greater_or_equals_filter(key: str, value: float) -> dict[str, dict[str, str | int | float]]:
    """Create a ``greaterThanOrEquals`` metadata filter clause."""
    return {"greaterThanOrEquals": {"key": key, "value": value}}


def and_all(*conditions: dict[str, Any]) -> dict[str, Any]:
    """Combine multiple conditions using ``andAll`` semantics."""
    return {"andAll": [condition for condition in conditions if condition]}


def ask_knowledge_base(
    question: str,
    knowledge_base_id: str,
    *,
    metadata_filter: dict[str, Any] | None = None,
    number_of_results: int = 4,
    search_type: str | None = None,
) -> dict[str, Any]:
    """Run a single RetrieveAndGenerate request and return the raw payload."""
    runtime = boto3.client("bedrock-agent-runtime", region_name=settings.AWS_REGION)

    vector_config: dict[str, Any] = {"numberOfResults": number_of_results}
    if metadata_filter:
        vector_config["filter"] = metadata_filter
    if search_type:
        vector_config["overrideSearchType"] = search_type

    retrieval_configuration = {"vectorSearchConfiguration": vector_config}

    return runtime.retrieve_and_generate(
        input={"text": question},
        retrieveAndGenerateConfiguration={
            "type": "KNOWLEDGE_BASE",
            "knowledgeBaseConfiguration": {
                "knowledgeBaseId": knowledge_base_id,
                "modelArn": settings.BEDROCK_RESPONSE_MODEL_ARN,
                "retrievalConfiguration": retrieval_configuration,
            },
        },
    )


MAX_REFERENCE_PREVIEW_LENGTH = 120


@dataclass(slots=True)
class QueryScenario:
    """Simple container for scripted RAG test cases."""

    label: str
    question: str
    metadata_filter: dict[str, Any] | None = None
    number_of_results: int = 4


def _print_response(label: str, response: dict[str, Any]) -> None:
    answer = response.get("output", {}).get("text", "(no answer)")
    print(f"=== {label} ===")
    print(answer)

    citations = response.get("citations", [])
    if not citations:
        print("(no citations returned)")
        print()
        return

    print("Sources:")
    for citation in citations:
        references = citation.get("retrievedReferences", [])
        for reference in references:
            ref = reference.get("content", {}).get("text", "")
            location = reference.get("location", {}).get("s3Location", {})
            uri = location.get("uri", "")
            preview = ref[:MAX_REFERENCE_PREVIEW_LENGTH]
            ellipsis = "…" if len(ref) > MAX_REFERENCE_PREVIEW_LENGTH else ""
            print(f"  - {uri}: {preview}{ellipsis}")
    print()


def run_scenarios(knowledge_base_id: str) -> None:
    """Execute a curated set of queries to validate metadata filtering."""
    base_filters = {
        "domain": equals_filter("domain", "auroradynamics.com"),
        "is_internal_true": equals_filter_bool("is_internal", value=True),
        "is_internal_false": equals_filter_bool("is_internal", value=False),
        "security_tags": equals_filter("tags", "security,governance"),
        "press_tags": equals_filter("tags", "press,announcement"),
        "published_after_2025": greater_or_equals_filter("published_at", 1_750_000_000),
        "metrics_tags": equals_filter("tags", "metrics,operations"),
        "catalog_tags": equals_filter("tags", "services,catalog"),
    }

    scenarios = [
        QueryScenario(
            label="No filter overview",
            question="Give me a broad overview of Aurora Dynamics as described in our knowledge base.",
        ),
        QueryScenario(
            label="Domain filter",
            question="Summarize the public description of Aurora Dynamics.",
            metadata_filter=base_filters["domain"],
        ),
        QueryScenario(
            label="Internal security docs",
            question="What security governance guidance is available?",
            metadata_filter=and_all(
                base_filters["is_internal_true"],
                base_filters["security_tags"],
            ),
        ),
        QueryScenario(
            label="Press announcement",
            question="What recent announcement did Aurora make?",
            metadata_filter=and_all(
                base_filters["is_internal_false"],
                base_filters["press_tags"],
                base_filters["published_after_2025"],
            ),
        ),
        QueryScenario(
            label="Metrics spotlight",
            question="Share key operational metrics for Aurora Dynamics.",
            metadata_filter=and_all(
                base_filters["is_internal_true"],
                base_filters["metrics_tags"],
            ),
        ),
        QueryScenario(
            label="Catalog lookup (hybrid search)",
            question="List the solution offerings Aurora provides to customers.",
            metadata_filter=base_filters["catalog_tags"],
        ),
    ]

    print("Running scripted knowledge base checks…", flush=True)

    for scenario in scenarios:
        try:
            response = ask_knowledge_base(
                scenario.question,
                knowledge_base_id,
                metadata_filter=scenario.metadata_filter,
                number_of_results=scenario.number_of_results,
            )
        except Exception as exc:  # noqa: BLE001
            print(f"=== {scenario.label} ===")
            print(f"Query failed: {exc}")
            if scenario.metadata_filter:
                print("Filter used:", json.dumps(scenario.metadata_filter, indent=2))
            print()
            continue

        _print_response(scenario.label, response)


if __name__ == "__main__":
    kb_id = settings.KNOWLEDGE_BASE_ID
    if not kb_id:
        raise SystemExit("Knowledge base ID must be configured in Settings before running scripted checks.")

    run_scenarios(kb_id)
