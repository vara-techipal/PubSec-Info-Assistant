# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import logging
import os
import json
from io import BytesIO
from typing import Any, Dict, List, Tuple

import azure.functions as func
from azure.storage.blob import generate_blob_sas
from azure.storage.queue import QueueClient, TextBase64EncodePolicy
from azure.identity import ManagedIdentityCredential, AzureAuthorityHosts, DefaultAzureCredential, get_bearer_token_provider
from shared_code.status_log import StatusLog, State, StatusClassification
from shared_code.utilities import Utilities, MediaType

import requests
from bs4 import BeautifulSoup

azure_blob_storage_account = os.environ["BLOB_STORAGE_ACCOUNT"]
azure_blob_storage_endpoint = os.environ["BLOB_STORAGE_ACCOUNT_ENDPOINT"]
azure_queue_storage_endpoint = os.environ["AZURE_QUEUE_STORAGE_ENDPOINT"]
azure_blob_drop_storage_container = os.environ["BLOB_STORAGE_ACCOUNT_UPLOAD_CONTAINER_NAME"]
azure_blob_content_storage_container = os.environ["BLOB_STORAGE_ACCOUNT_OUTPUT_CONTAINER_NAME"]
azure_blob_log_storage_container = os.environ["BLOB_STORAGE_ACCOUNT_LOG_CONTAINER_NAME"]
cosmosdb_url = os.environ["COSMOSDB_URL"]
cosmosdb_log_database_name = os.environ["COSMOSDB_LOG_DATABASE_NAME"]
cosmosdb_log_container_name = os.environ["COSMOSDB_LOG_CONTAINER_NAME"]
non_pdf_submit_queue = os.environ["NON_PDF_SUBMIT_QUEUE"]
pdf_polling_queue = os.environ["PDF_POLLING_QUEUE"]
pdf_submit_queue = os.environ["PDF_SUBMIT_QUEUE"]
text_enrichment_queue = os.environ["TEXT_ENRICHMENT_QUEUE"]
CHUNK_TARGET_SIZE = int(os.environ["CHUNK_TARGET_SIZE"])
local_debug = os.environ["LOCAL_DEBUG"]
azure_ai_credential_domain = os.environ["AZURE_AI_CREDENTIAL_DOMAIN"]
azure_openai_authority_host = os.environ["AZURE_OPENAI_AUTHORITY_HOST"]

function_name = "FileLayoutParsingOther"

if azure_openai_authority_host == "AzureUSGovernment":
    AUTHORITY = AzureAuthorityHosts.AZURE_GOVERNMENT
else:
    AUTHORITY = AzureAuthorityHosts.AZURE_PUBLIC_CLOUD

# When debugging in VSCode, use the current user identity to authenticate with Azure OpenAI,
# Cognitive Search and Blob Storage (no secrets needed, just use 'az login' locally)
# Use managed identity when deployed on Azure.
# If you encounter a blocking error during a DefaultAzureCredntial resolution, you can exclude
# the problematic credential by using a parameter (ex. exclude_shared_token_cache_credential=True)
if local_debug == "true":
    azure_credential = DefaultAzureCredential(authority=AUTHORITY)
else:
    azure_credential = ManagedIdentityCredential(authority=AUTHORITY)

utilities = Utilities(azure_blob_storage_account, azure_blob_storage_endpoint, azure_blob_drop_storage_container, azure_blob_content_storage_container, azure_credential)

class UnstructuredError(Exception):
    pass


DEFAULT_CHUNK_OVERLAP_RATIO = 0.25
HEADER_CATEGORIES = {
    "Title",
    "Subtitle",
    "Section Header",
    "Header",
    "Heading",
    "SectionHeading",
}


def _looks_like_html(text: str) -> bool:
    if "<" not in text or ">" not in text:
        return False
    soup = BeautifulSoup(text, "html.parser")
    return bool(soup.find())


def _collect_json_fragments(value: Any) -> List[Tuple[str, str]]:
    fragments: List[Tuple[str, str]] = []

    def _walk(node: Any) -> None:
        if isinstance(node, dict):
            for child in node.values():
                _walk(child)
        elif isinstance(node, list):
            for child in node:
                _walk(child)
        elif isinstance(node, str):
            stripped = node.strip()
            if not stripped:
                return
            if _looks_like_html(stripped):
                soup = BeautifulSoup(stripped, "html.parser")
                fragments.append(("html", str(soup)))
            else:
                fragments.append(("text", stripped))

    _walk(value)
    return fragments


def _normalize_element_content(element) -> Tuple[str, str]:
    """Return display text and token text for an unstructured element."""

    text = getattr(element, "text", "") or ""
    text = text.strip()
    metadata = getattr(element, "metadata", None)
    html_text = getattr(metadata, "text_as_html", None)

    if html_text:
        soup = BeautifulSoup(html_text, "html.parser")
        display_text = str(soup)
        token_text = soup.get_text(" ").strip()
        return display_text, token_text

    if text and _looks_like_html(text):
        soup = BeautifulSoup(text, "html.parser")
        plain_text = soup.get_text(" ").strip()
        return plain_text, plain_text

    return text, text


def _semantic_chunk_elements(elements, chunk_target_size: int, overlap_ratio: float) -> List[Dict[str, Any]]:
    normalized_elements: List[Dict[str, Any]] = []
    current_section = ""

    for element in elements:
        display_text, token_text = _normalize_element_content(element)
        if not display_text:
            continue

        category = (getattr(element, "category", "") or "").strip()
        if category in HEADER_CATEGORIES and display_text:
            current_section = display_text.splitlines()[0].strip()

        normalized_elements.append(
            {
                "display_text": display_text,
                "token_text": token_text if token_text else display_text,
                "category": category,
                "section": current_section,
                "page_number": getattr(getattr(element, "metadata", None), "page_number", None),
            }
        )

    chunks: List[Dict[str, Any]] = []
    start_index = 0
    total_elements = len(normalized_elements)

    while start_index < total_elements:
        tokens_accumulated = 0
        end_index = start_index
        chunk_elements: List[Dict[str, Any]] = []

        while end_index < total_elements:
            candidate = normalized_elements[end_index]
            candidate_tokens = utilities.token_count(candidate["token_text"])
            if tokens_accumulated + candidate_tokens > chunk_target_size and chunk_elements:
                break

            chunk_elements.append(candidate)
            tokens_accumulated += candidate_tokens
            end_index += 1

            if tokens_accumulated >= chunk_target_size:
                break

        if not chunk_elements and start_index < total_elements:
            candidate = normalized_elements[start_index]
            chunk_elements.append(candidate)
            tokens_accumulated = utilities.token_count(candidate["token_text"])
            end_index = start_index + 1

        display_segments = [segment["display_text"] for segment in chunk_elements if segment["display_text"]]
        token_segments = [segment["token_text"] for segment in chunk_elements if segment["token_text"]]
        chunk_display_text = "\n\n".join(display_segments).strip()
        chunk_token_text = "\n\n".join(token_segments).strip()

        pages = sorted(
            {segment["page_number"] if segment["page_number"] is not None else 1 for segment in chunk_elements}
        )
        section_name = ""
        for segment in reversed(chunk_elements):
            if segment["section"]:
                section_name = segment["section"]
                break

        chunks.append(
            {
                "content": chunk_display_text,
                "token_text": chunk_token_text,
                "token_count": utilities.token_count(chunk_token_text) if chunk_token_text else 0,
                "pages": pages if pages else [1],
                "section": section_name,
            }
        )

        if end_index >= total_elements:
            break

        overlap_tokens = max(int(chunk_target_size * overlap_ratio), 1)
        overlap_accumulated = 0
        new_start = end_index
        while new_start > start_index and overlap_accumulated < overlap_tokens:
            new_start -= 1
            overlap_accumulated += utilities.token_count(
                normalized_elements[new_start]["token_text"]
            )

        if new_start == start_index:
            new_start += 1

        start_index = new_start

    return chunks

def PartitionFile(file_extension: str, file_url: str):      
    """ uses the unstructured.io libraries to analyse a document
    Returns:
        elements: A list of available models
    """  
    # Send a GET request to the URL to download the file
    response = requests.get(file_url)
    bytes_io = BytesIO(response.content)
    response.close()   
    metadata = [] 
    elements = None
    file_extension_lower = file_extension.lower()
    try:        
        if file_extension_lower == '.csv':
            from unstructured.partition.csv import partition_csv
            elements = partition_csv(file=bytes_io)               
                     
        elif file_extension_lower == '.doc':
            from unstructured.partition.doc import partition_doc
            elements = partition_doc(file=bytes_io) 
            
        elif file_extension_lower == '.docx':
            from unstructured.partition.docx import partition_docx
            elements = partition_docx(file=bytes_io)
            
        elif file_extension_lower == '.eml' or file_extension_lower == '.msg':
            if file_extension_lower == '.msg':
                from unstructured.partition.msg import partition_msg
                elements = partition_msg(file=bytes_io) 
            else:        
                from unstructured.partition.email import partition_email
                elements = partition_email(file=bytes_io)
            metadata.append(f'Subject: {elements[0].metadata.subject}')
            metadata.append(f'From: {elements[0].metadata.sent_from[0]}')
            sent_to_str = 'To: '
            for sent_to in elements[0].metadata.sent_to:
                sent_to_str = sent_to_str + " " + sent_to
            metadata.append(sent_to_str)
            
        elif file_extension_lower == '.html' or file_extension_lower == '.htm':  
            from unstructured.partition.html import partition_html
            elements = partition_html(file=bytes_io) 
            
        elif file_extension_lower == '.md':
            from unstructured.partition.md import partition_md
            elements = partition_md(file=bytes_io)
                       
        elif file_extension_lower == '.ppt':
            from unstructured.partition.ppt import partition_ppt
            elements = partition_ppt(file=bytes_io)
            
        elif file_extension_lower == '.pptx':
            from unstructured.partition.pptx import partition_pptx
            elements = partition_pptx(file=bytes_io)

        elif file_extension_lower == '.json':
            try:
                raw_json = bytes_io.getvalue().decode('utf-8')
                json_payload = json.loads(raw_json)
            except Exception as json_error:
                raise UnstructuredError(
                    f"An error occurred trying to parse the file: {str(json_error)}"
                ) from json_error

            fragments = _collect_json_fragments(json_payload)
            elements = []

            if fragments:
                from unstructured.partition.html import partition_html
                from unstructured.partition.text import partition_text

                for fragment_type, fragment_value in fragments:
                    try:
                        if fragment_type == 'html':
                            elements.extend(partition_html(text=fragment_value))
                        else:
                            elements.extend(partition_text(text=fragment_value))
                    except Exception:
                        elements.extend(partition_text(text=fragment_value))
            else:
                from unstructured.partition.text import partition_text
                elements = partition_text(text=json.dumps(json_payload))

            if isinstance(json_payload, dict):
                for metadata_key in ("Title", "Subject", "Summary"):
                    value = json_payload.get(metadata_key)
                    if isinstance(value, str) and value.strip():
                        metadata.append(f"{metadata_key}: {value.strip()}")

        elif file_extension_lower == '.txt':
            from unstructured.partition.text import partition_text
            elements = partition_text(file=bytes_io)

        elif file_extension_lower == '.xlsx':
            from unstructured.partition.xlsx import partition_xlsx
            elements = partition_xlsx(file=bytes_io)
            
        elif file_extension_lower == '.xml':
            from unstructured.partition.xml import partition_xml
            elements = partition_xml(file=bytes_io)
            
    except Exception as e:
        raise UnstructuredError(f"An error occurred trying to parse the file: {str(e)}") from e
         
    return elements, metadata
    
    

def main(msg: func.QueueMessage) -> None:
    try:
        statusLog = StatusLog(cosmosdb_url, azure_credential, cosmosdb_log_database_name, cosmosdb_log_container_name)
        logging.info('Python queue trigger function processed a queue item: %s',
                    msg.get_body().decode('utf-8'))

        # Receive message from the queue
        message_body = msg.get_body().decode('utf-8')
        message_json = json.loads(message_body)
        blob_name =  message_json['blob_name']
        blob_uri =  message_json['blob_uri']
        statusLog.upsert_document(blob_name, f'{function_name} - Starting to parse the non-PDF file', StatusClassification.INFO, State.PROCESSING)
        statusLog.upsert_document(blob_name, f'{function_name} - Message received from non-pdf submit queue', StatusClassification.DEBUG)

        # construct blob url
        blob_path_plus_sas = utilities.get_blob_and_sas(blob_name)
        statusLog.upsert_document(blob_name, f'{function_name} - SAS token generated to access the file', StatusClassification.DEBUG)

        file_name, file_extension, file_directory  = utilities.get_filename_and_extension(blob_name)

        response = requests.get(blob_path_plus_sas)
        response.raise_for_status()
              
        
        # Partition the file dependent on file extension
        elements, metadata = PartitionFile(file_extension, blob_path_plus_sas)
        metdata_text = ''
        for metadata_value in metadata:
            metdata_text += metadata_value + '\n'    
        statusLog.upsert_document(blob_name, f'{function_name} - partitioning complete', StatusClassification.DEBUG)
        
        title = ''
        # Capture the file title
        try:
            for i, element in enumerate(elements):
                if title == '' and element.category == 'Title':
                    # capture the first title
                    title = element.text
                    break
        except:
            # if this type of eleemnt does not include title, then process with emty value
            pass
        
        # Chunk the file using semantic chunking with overlap
        chunks = _semantic_chunk_elements(elements, CHUNK_TARGET_SIZE, DEFAULT_CHUNK_OVERLAP_RATIO)
        statusLog.upsert_document(blob_name, f'{function_name} - chunking complete. {len(chunks)} chunks created', StatusClassification.DEBUG)

        chunk_total = len(chunks)
        # Complete and write chunks
        for i, chunk in enumerate(chunks):
            page_list = chunk.get('pages', []) or [1]
            chunk_body = chunk.get('content', '')
            if not chunk_body:
                continue
            token_text = chunk.get('token_text', chunk_body)
            chunk_size = chunk.get('token_count', 0)
            if chunk_size == 0:
                chunk_size = utilities.token_count(token_text)

            # add filetype specific metadata as chunk text header
            chunk_text = f"{metdata_text}{chunk_body}" if metdata_text else chunk_body
            section_name = chunk.get('section', '')
            subtitle_name = section_name

            utilities.write_chunk(
                blob_name,
                blob_uri,
                f"{i}",
                chunk_size,
                chunk_text,
                page_list,
                section_name,
                title,
                subtitle_name,
                MediaType.TEXT,
                chunk_index=i,
                chunk_total=chunk_total,
            )
        
        statusLog.upsert_document(blob_name, f'{function_name} - chunking stored.', StatusClassification.DEBUG)   
        
        # submit message to the text enrichment queue to continue processing                
        queue_client = QueueClient(account_url=azure_queue_storage_endpoint,
                               queue_name=text_enrichment_queue,
                               credential=azure_credential,
                               message_encode_policy=TextBase64EncodePolicy())
        message_json["text_enrichment_queued_count"] = 1
        message_string = json.dumps(message_json)
        queue_client.send_message(message_string)
        statusLog.upsert_document(blob_name, f"{function_name} - message sent to enrichment queue", StatusClassification.DEBUG, State.QUEUED)    
             
    except Exception as e:
        statusLog.upsert_document(blob_name, f"{function_name} - An error occurred - {str(e)}", StatusClassification.ERROR, State.ERROR)

    statusLog.save_document(blob_name)
    