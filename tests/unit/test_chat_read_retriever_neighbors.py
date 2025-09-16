from pathlib import Path
from unittest.mock import MagicMock, patch
import sys

ROOT_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT_DIR / "app" / "backend"))

from approaches.chatreadretrieveread import ChatReadRetrieveReadApproach


class FakeSearchClient:
    def __init__(self, responses):
        self._responses = responses
        self.calls = []

    def search(self, search_text, **kwargs):
        filter_expr = kwargs.get("filter")
        self.calls.append(filter_expr)
        results = self._responses.get(filter_expr)
        if results is None:
            return iter([])
        return iter(results)


def build_doc(index, content):
    return {
        "file_name": "files/doc.json",
        "chunk_file": f"files/doc-{index}.json",
        "content": content,
        "pages": [index + 1],
        "chunk_index": index,
        "chunk_total": 3,
    }


def test_combine_chunk_family_fetches_neighbor_chunks():
    primary_doc = build_doc(1, "Main section text")
    previous_doc = build_doc(0, "Previous context")
    next_doc = build_doc(2, "Next context")

    responses = {
        "file_name eq 'files/doc.json' and chunk_index eq 0": [previous_doc],
        "file_name eq 'files/doc.json' and chunk_index eq 2": [next_doc],
    }

    fake_client = FakeSearchClient(responses)
    blob_client = MagicMock()
    blob_client.url = "https://storage.example.com/blob"

    with patch("approaches.chatreadretrieveread.AsyncAzureOpenAI", return_value=MagicMock()):
        approach = ChatReadRetrieveReadApproach(
            search_client=fake_client,
            oai_endpoint="https://openai",
            chatgpt_deployment="deployment",
            source_file_field="file_name",
            content_field="content",
            page_number_field="pages",
            chunk_file_field="chunk_file",
            content_storage_container="content",
            blob_client=blob_client,
            query_term_language="en",
            model_name="gpt-35-turbo",
            model_version="0613",
            target_embedding_model="embedding",
            enrichment_appservice_uri="https://embed",
            target_translation_language="en",
            azure_ai_endpoint="https://ai",
            azure_ai_location="eastus",
            azure_ai_token_provider="token",
            use_semantic_reranker=False,
        )

    combined_text, combined_docs = approach._combine_chunk_family(primary_doc)

    assert "Main section text" in combined_text
    assert "Previous context" in combined_text
    assert "Next context" in combined_text
    assert len(combined_docs) == 3
    assert any(call and "chunk_index eq 0" in call for call in fake_client.calls)
    assert any(call and "chunk_index eq 2" in call for call in fake_client.calls)
