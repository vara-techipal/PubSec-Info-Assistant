import importlib
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import nltk

ROOT_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT_DIR))
sys.path.insert(0, str(ROOT_DIR / "functions"))

import pytest

MODULE_NAME = "functions.FileLayoutParsingOther.__init__"

ENV_VARS = {
    "BLOB_STORAGE_ACCOUNT": "testaccount",
    "BLOB_STORAGE_ACCOUNT_ENDPOINT": "https://test.blob.core.windows.net",
    "AZURE_QUEUE_STORAGE_ENDPOINT": "https://test.queue.core.windows.net",
    "BLOB_STORAGE_ACCOUNT_UPLOAD_CONTAINER_NAME": "upload",
    "BLOB_STORAGE_ACCOUNT_OUTPUT_CONTAINER_NAME": "content",
    "BLOB_STORAGE_ACCOUNT_LOG_CONTAINER_NAME": "log",
    "COSMOSDB_URL": "https://cosmos.local",
    "COSMOSDB_LOG_DATABASE_NAME": "statusdb",
    "COSMOSDB_LOG_CONTAINER_NAME": "statuscontainer",
    "NON_PDF_SUBMIT_QUEUE": "non-pdf",
    "PDF_POLLING_QUEUE": "pdf-poll",
    "PDF_SUBMIT_QUEUE": "pdf-submit",
    "TEXT_ENRICHMENT_QUEUE": "text-enrich",
    "CHUNK_TARGET_SIZE": "256",
    "LOCAL_DEBUG": "true",
    "AZURE_AI_CREDENTIAL_DOMAIN": "example.com",
    "AZURE_OPENAI_AUTHORITY_HOST": "AzurePublicCloud",
}


@pytest.fixture
def file_layout_module(monkeypatch):
    for key, value in ENV_VARS.items():
        monkeypatch.setenv(key, value)

    if MODULE_NAME in sys.modules:
        del sys.modules[MODULE_NAME]

    punkt_dir = Path(nltk.data.path[0]) / "tokenizers" / "punkt"
    punkt_dir.mkdir(parents=True, exist_ok=True)

    with patch("azure.identity.DefaultAzureCredential", return_value=MagicMock()), \
            patch("azure.identity.ManagedIdentityCredential", return_value=MagicMock()), \
            patch("nltk.download", return_value=True):
        module = importlib.import_module(MODULE_NAME)
    return module


def test_partition_file_json_html_content(file_layout_module):
    sample_payload = {
        "Title": "My Document",
        "Content": "<p>Welcome <strong>reader</strong></p>",
        "source_url": "https://contoso.example/source",
    }
    payload_bytes = json.dumps(sample_payload).encode("utf-8")

    class DummyResponse:
        def __init__(self, content: bytes):
            self.content = content

        def close(self) -> None:
            pass

    html_element = SimpleNamespace(text="Welcome reader")
    text_element = SimpleNamespace(text="Additional context")

    with patch.object(file_layout_module.requests, "get", return_value=DummyResponse(payload_bytes)), \
            patch("unstructured.partition.html.partition_html", return_value=[html_element]), \
            patch("unstructured.partition.text.partition_text", return_value=[text_element]):
        elements, metadata, source_url = file_layout_module.PartitionFile(".json", "https://example.com/test.json")

    element_texts = [getattr(element, "text", "") for element in elements if getattr(element, "text", "")]

    assert any("Welcome" in text for text in element_texts)
    assert "Title: My Document" in metadata
    assert source_url == "https://contoso.example/source"
