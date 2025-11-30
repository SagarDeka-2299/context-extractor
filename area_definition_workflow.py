import os
import io
import csv
import base64
import pdfplumber
import pandas as pd
import re
import sys
from sqlalchemy import create_engine, text
import requests
from typing import List, Set
from PIL import Image
from pathlib import Path
from dotenv import load_dotenv
from loguru import logger
from prefect import serve
import urllib
import pandas as pd

# Prefect Imports
from prefect import flow, task, serve
from prefect.states import State
from prefect.client.schemas.objects import FlowRun
from prefect.logging import get_run_logger as get_prefect_logger
from prefect.task_runners import ConcurrentTaskRunner

# LangChain & AI
from langchain_openai import ChatOpenAI
from langchain_openai import OpenAIEmbeddings
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.documents import Document
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_chroma import Chroma
from pydantic import BaseModel, Field

# --- IMPORT CONFIG ---
import area_definition_workflow_config as config

load_dotenv()

# ---------------------------------------------------------
# WEBHOOK HOOK
# ---------------------------------------------------------
def notify_area_hook(flow, flow_run: FlowRun, state: State):
    """
    Prefect hook to notify Django about the flow status.
    """
    status = "success" if state.is_completed() else "failed"
    payload = {"status": status}
    
    try:
        # Construct full URL using the base from config
        full_url = f"{config.WEBHOOK_URL}/area/update_status"
        requests.post(full_url, json=payload)
        print(f"🪝 Webhook sent: Area -> {status}")
    except Exception as e:
        print(f"⚠️ Webhook Error: {e}")

# ---------------------------------------------------------
# GLOBAL VARIABLES & LOCAL SETTINGS
# ---------------------------------------------------------

# Ensure Root Exists
config.OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

# 1. Internal Paths (Intermediate files)
IMAGE_OUTPUT_DIR = config.OUTPUT_ROOT / "extracted_images"
CSV_OUTPUT_FILE = config.OUTPUT_ROOT / "pdf_analysis_report.csv"
RULES_CSV_OUTPUT_FILE = config.OUTPUT_ROOT / "structured_rules.csv"

# 2. Database Connection
DB_USER = os.getenv("DB_USER","")
DB_PASSWORD = os.getenv("DB_PASSWORD","")
DB_HOST = os.getenv("DB_HOST","127.0.0.1")
DB_PORT = os.getenv("DB_PORT",'5432')
DB_NAME = os.getenv("DB_NAME")

safe_user = urllib.parse.quote_plus(DB_USER)
safe_password = urllib.parse.quote_plus(DB_PASSWORD)

SQL_CONNECTION_STRING = f"postgresql://{safe_user}:{safe_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

#=============
#test code: TODO: remove

import random
import time
from langchain_core.runnables import Runnable
from langchain_core.messages import AIMessage


#===============TEST========TODO:REMOVE

# class DummyChatOpenAI1(Runnable):
#     def invoke(self, input, config=None):
#         choices = ["YES", "NO"]
#         picked = random.choice(choices)
#         time.sleep(2)       # can be whatever delay you want
#         return AIMessage(content=picked)

# class RegulatoryRule(BaseModel):
#     rule_id: str = Field(description="Unique identifier (e.g., Q1, Q2, Q17)")
#     rule_summary: str = Field(description="A concise summary of the rule/clarification")
#     measurement_basis: str = Field(description="Key measurement principle and associated rule (e.g., 'middle of the external wall')")


# class DummyChatOpenAI2(Runnable):
#     def invoke(self, input, config=None):
#         # Fabricated sample rule entries to pick from
#         candidates = [
#             RegulatoryRule(
#                 rule_id="Q1",
#                 rule_summary="Clarifies that measurements should exclude decorative projections.",
#                 measurement_basis="Measure from the middle of the external wall face."
#             ),
#             RegulatoryRule(
#                 rule_id="Q7",
#                 rule_summary="Defines how to treat irregular building edges during assessment.",
#                 measurement_basis="Use perpendicular offset from the dominant structural line."
#             ),
#             RegulatoryRule(
#                 rule_id="Q17",
#                 rule_summary="Explains handling of partial obstructions in view analysis.",
#                 measurement_basis="Reference the nearest unobstructed structural surface."
#             ),
#         ]

#         picked = random.choice(candidates)

#         time.sleep(2)

#         # Return as JSON string in an AIMessage
#         return AIMessage(content=picked.model_dump_json())

# def get_llm_detector():
#     return DummyChatOpenAI1()
# def get_llm_extractor():
#     return DummyChatOpenAI2()

#===============TODO: REMOVE

#Get Embeddings Model
embeddings = OpenAIEmbeddings(
    model='text-embedding-3-large',
    api_key=os.getenv("OPENAI_API_KEY_SUPPORT_EMBEDDING"),
    base_url=os.getenv("OPENAI_BASE_URL_SUPPORT_EMBEDDING")
)
#=============
os.environ["OPENAI_API_KEY"]=os.environ["OPENAI_API_KEY_SUPPORT_IMG"]
os.environ["OPENAI_BASE_URL"]=os.environ["OPENAI_BASE_URL_SUPPORT_IMG"]
# 3. Model Initialization
def get_llm_detector():
    return ChatOpenAI(model="gpt-5-nano")
def get_llm_extractor():
    return ChatOpenAI(model="gpt-5")


# 4. Pydantic Model for Extraction
class RegulatoryRule(BaseModel):
    rule_id: str = Field(description="Unique identifier (e.g., Q1, Q2, Q17)")
    rule_summary: str = Field(description="A concise summary of the rule/clarification")
    measurement_basis: str = Field(description="Key measurement principle and associated rule (e.g., 'middle of the external wall')")

# ---------------------------------------------------------
# LOGURU CONFIGURATION
# ---------------------------------------------------------
logger.remove() 
logger.add(sys.stderr, format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{function}</cyan> - <level>{message}</level>")
logger.add(config.LOG_FILE, rotation="10 MB")

def prefect_sink(message):
    try:
        prefect_logger = get_prefect_logger()
        if hasattr(message, 'record'):
            record = message.record
            level_name = record["level"].name
            text = record["message"]
            if level_name == "SUCCESS":
                prefect_logger.info(f"[SUCCESS] {text}")
            else:
                getattr(prefect_logger, level_name.lower(), prefect_logger.info)(text)
    except Exception:
        pass

logger.add(prefect_sink, format="{message}")

# ---------------------------------------------------------
# TASKS
# ---------------------------------------------------------

@task(name="Encode Image to Base64", retries=3, retry_delay_seconds=2)
def encode_image_path_to_base64(image_path):
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode('utf-8')

@task(name="Split Pages", retries=3, retry_delay_seconds=2)
def split_pages_task(pdf_path: Path):
    if not IMAGE_OUTPUT_DIR.exists():
        IMAGE_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created image output directory at {IMAGE_OUTPUT_DIR}")
        
    prepared_pages = []
    
    logger.info(f"Opening PDF: {pdf_path}")
    if not pdf_path.exists():
        logger.error(f"PDF not found: {pdf_path}")
        return []

    with pdfplumber.open(pdf_path) as pdf:
        total_pages = len(pdf.pages)
        logger.info(f"PDF loaded successfully. Total pages to split: {total_pages}")
        
        for i, page in enumerate(pdf.pages):
            page_num = i + 1
            raw_text = page.extract_text() or ""
            image_filename = f"page_{page_num}_diagram.jpg"
            image_path = IMAGE_OUTPUT_DIR / image_filename
            
            # Save image
            plumber_image = page.to_image(resolution=150)
            plumber_image.original.save(image_path, format="JPEG", quality=85)
            
            prepared_pages.append({
                "page_num": page_num,
                "image_path": str(image_path),
                "raw_text": raw_text
            })
            
    logger.success(f"Successfully split all {total_pages} pages into images.")
    return prepared_pages

@task(name="Check Visuals", retries=5, retry_delay_seconds=3)
def check_visuals_task(image_path):
    base64_img = encode_image_path_to_base64(image_path)
    llm = get_llm_detector()
    system_prompt = "You are a document analysis assistant. Answer strictly 'YES' or 'NO'."
    user_prompt = "Does this page contain data tables, technical diagrams, flowcharts, or graphs? Ignore simple logos."
    message = HumanMessage(content=[
        {"type": "text", "text": user_prompt},
        {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{base64_img}"}},
    ])
    try:
        response = llm.invoke([SystemMessage(content=system_prompt), message])
        result = "YES" in response.content.strip().upper()
        return result
    except Exception as e:
        logger.error(f"Error checking visuals for {image_path}: {e}")
        raise e

@task(name="LLM Extract", retries=3, retry_delay_seconds=5)
def llm_extract_content_task(image_path):
    filename = os.path.basename(image_path)
    logger.info(f"Starting VLM extraction for: {filename}")
    base64_img = encode_image_path_to_base64(image_path)
    llm = get_llm_extractor()
    system_prompt = """
    You are a visual–language model that converts any page image (PDF scan, camera photo, diagram-rich document) into clean, structured, semantic markdown. You must interpret the page exactly as it visually appears: extract text, describe images, parse tables, and preserve ordering.

    Follow these strict rules:

    1. TEXT
    - Rewrite visible text cleanly in markdown.
    - Preserve exact reading order.
    - Ignore page numbers/headers unless meaningful.
    - Do not invent or paraphrase meaning.

    2. FIGURES
    Whenever any visual element appears (diagram, flowchart, schematic, picture, blueprint, comic drawing), insert:
    [figX image description: ...]
    Where:
    - X increases sequentially.
    - Describe shapes, labels, arrows, embedded text, layout, colors, and the purpose of the figure.

    Example (fun, memorable):
    [fig1 image description: A cartoon cat wearing sunglasses, pointing at a whiteboard labeled "Budget Plan" with arrows from "Snacks" → "Happiness".]
    =====

    3. TABLES
    Simple table → list of objects:
    {name: Luna, score: 98}, {name: Ben, score: 76}

    Complex table → row-by-row breakdown:
    row 1:
    --- column A
    * Text: "Item A"
    * [fig1 image description: Small icon of a banana lifting weights]
    ----
    --- column B
    * calories: 120
    * fiber: 3g
    =====

    4. SEMANTIC SPLIT
    Use exactly:
    =====
    between clearly different topics.

    Example:
    This section explains rainfall.
    =====
    This section explains why ducks stare at you sometimes.
    =====

    5. ORDER PRESERVATION
    Keep the original order of text, diagrams, tables, labels. Do not rearrange.

    Example:
    If a map appears between two paragraphs, keep it there.
    =====

    6. VISUAL + TEXT CONNECTION
    Place the figure immediately where it appears.

    Example:
    Step 2: mix the batter.
    [fig2 image description: Bowl with whisk mid-motion, small arrow showing clockwise mixing]
    =====

    7. NO HALLUCINATION
    Describe only what is visible.

    Example:
    If half the page is torn:
    label partially visible: "instruc..." (cut off)
    =====

    8. OUTPUT FORMAT
    Output only the final markdown. No comments or process explanation.
    """

    instruction_prompt = """
    Look at the provided page image. Extract every visible element—text, diagrams, tables—and rewrite them into clean semantic markdown following the system rules above. Maintain order, describe all visuals, convert tables properly, and separate distinct topics with =====.
    """

    message = HumanMessage(content=[
        {"type": "text", "text": instruction_prompt},
        {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{base64_img}"}},
    ])
    try:
        response = llm.invoke([SystemMessage(content=system_prompt), message])
        content = response.content.strip()
        logger.success(f"Successfully extracted VLM content from {filename} ({len(content)} chars)")
        return content
    except Exception as e:
        logger.exception(f"VLM extraction failed for {filename}")
        raise e

@task(name="Extract Text (Fallback)")
def extract_text_task(raw_text):
    if not raw_text or len(raw_text.strip()) == 0:
        return "[No text found on page]"
    return raw_text

@task(name="Process Single Page", retries=3, retry_delay_seconds=2)
def process_single_page_flow(page_data):
    page_num = page_data['page_num']
    image_path = page_data['image_path']
    raw_text = page_data['raw_text']
    
    logger.info(f"Processing Page {page_num}...")
    
    # Check for diagrams
    has_diagram = check_visuals_task(image_path)
    final_description = ""
    diagram_ref = ""
    
    if has_diagram:
        logger.info(f"Page {page_num}: Diagram detected. Initiating VLM extraction.")
        diagram_ref = image_path
        # We call the task directly here (synchronously inside the worker logic)
        # or we could submit it if we were in a flow. 
        # Since this is a task calling logic, we just run the logic.
        try:
            final_description = llm_extract_content_task(image_path)
        except Exception:
            logger.warning(f"Page {page_num}: VLM failed. Falling back.")
            final_description = extract_text_task(raw_text)
    else:
        final_description = extract_text_task(raw_text)

    return {
        "page_number": page_num,
        "description": final_description,
        "diagram_ref": diagram_ref
    }

@task(name="Identify Q&A Sections", retries=3, retry_delay_seconds=2)
def identify_qa_sections(results):
    logger.info("Starting QA section identification logic...")
    qa_pattern = re.compile(r'(Q\d+\.)')
    extracted_blocks = []
    current_block = None
    sorted_results = sorted(results, key=lambda x: x['page_number'])
    stop_collecting = False 

    for page_data in sorted_results:
        if stop_collecting: break
        text = page_data.get('description', '')
        image_ref = page_data.get('diagram_ref', '') 
        parts = qa_pattern.split(text)
        
        if len(parts) == 1:
            if current_block:
                if image_ref:
                    current_block["text"] += " " + text
                    current_block["images"].add(image_ref)
                else:
                    stop_collecting = True
        else:
            if current_block and parts[0].strip():
                current_block['text'] += " " + parts[0]
            
            for j in range(1, len(parts), 2):
                marker = parts[j] 
                content = parts[j+1] if j+1 < len(parts) else ""
                if current_block: extracted_blocks.append(current_block)
                current_block = {"id": marker.strip(), "text": marker + " " + content, "images": set()}
                if image_ref: current_block['images'].add(image_ref)
            stop_collecting = False

    if current_block: extracted_blocks.append(current_block)

    final_data = []
    for block in extracted_blocks:
        final_data.append({
            "raw_id": block["id"],
            "full_text": block["text"],
            "image_paths": list(block["images"])
        })
    return final_data

@task(name="Structure Rules via LLM", retries=5, retry_delay_seconds=3)
def structure_rule_with_llm(qa_block):
    rule_id = qa_block['raw_id']
    logger.info(f"Structuring data for rule: {rule_id}")
    text_content = qa_block['full_text']
    image_paths = qa_block['image_paths']
    
    llm = get_llm_extractor()
    parser = JsonOutputParser(pydantic_object=RegulatoryRule)
    
    system_instruction = "You are a regulatory analyst. Analyze text and diagrams. Extract rule details."
    prompt = ChatPromptTemplate.from_messages([
        ("system", system_instruction),
        ("human", "Context:\n{context_text}"),
        ("human", "Ensure your response is a JSON object: {format_instructions}")
    ])
    
    formatted_prompt = prompt.format_messages(
        context_text=text_content,
        format_instructions=parser.get_format_instructions()
    )
    
    final_messages = formatted_prompt
    if image_paths:
        image_content_blocks = []
        for img_path in image_paths:
             if os.path.exists(img_path):
                 b64 = encode_image_path_to_base64(img_path)
                 image_content_blocks.append({"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64}"}})
        if image_content_blocks:
            final_messages.append(HumanMessage(content=image_content_blocks))

    try:
        response = llm.invoke(final_messages)
        return parser.invoke(response)
    except Exception as e:
        logger.exception(f"Failed to structure rule {rule_id}")
        raise e

@task(name="Load to Database (SQLAlchemy Dump)")
def load_rules_to_db(rules_df: pd.DataFrame):
    """
    Loads rules to PostgreSQL using Pandas + SQLAlchemy.
    TRUNCATES the table first, then inserts the DataFrame.
    """
    logger.info(f"Initiating Raw SQL load to PostgreSQL...")
    
    # 1. Create SQLAlchemy Engine
    engine = create_engine(SQL_CONNECTION_STRING.replace("postgresql://", "postgresql+psycopg2://"))
    
    # 2. CRITICAL: Clean Primary Key (Strips whitespace/nulls before load)
    # The dataframe should already have keys: rule_id, rule_summary, measurement_basis
    df = rules_df.copy()
    df['rule_id'] = df['rule_id'].astype(str).str.strip()
    df['rule_id'] = df['rule_id'].replace('', pd.NA)
    cleaned_df = df.dropna(subset=['rule_id'])
    
    if len(cleaned_df) == 0:
        logger.warning("No valid records found after cleaning Primary Key. Skipping DB load.")
        return 0

    try:
        with engine.begin() as connection:
            # 3. TRUNCATE TABLE
            logger.info("Truncating table 'regulatory_rules'...")
            connection.execute(text("TRUNCATE TABLE regulatory_rules;"))
            
            # 4. DUMP DATA using Pandas
            logger.info(f"Inserting {len(cleaned_df)} clean rows...")
            cleaned_df.to_sql(
                'regulatory_rules', 
                con=connection, 
                if_exists='append', 
                index=False,
                method='multi' 
            )
            
        logger.success(f"Successfully loaded {len(cleaned_df)} rows to DB.")
        return len(cleaned_df)
        
    except Exception as e:
        logger.critical(f"Database Load FAILED (SQLAlchemy): {e}")
        raise e

# ---------------------------------------------------------
# CHUNKING TASK
# ---------------------------------------------------------

@task(name="Chunk Report")
def chunk_text_task(rows):
    SEP = "====="
    MIN_LEN = 300
    MAX_LEN = 1200

    df = pd.DataFrame(rows)

    # split by separator and keep image refs
    raw_sections = []
    for _, r in df.iterrows():
        text = str(r["description"])
        img = r.get("diagram_ref", "") or ""
        img_list = [img] if img else []
        for part in text.split(SEP):
            p = part.strip()
            if p:
                raw_sections.append({"text": p, "imgs": img_list.copy()})

    # merge small sections while preserving/concatenating image refs
    merged = []
    buf_text = ""
    buf_imgs = []
    for sec in raw_sections:
        t = sec["text"]
        imgs = sec["imgs"]
        if not buf_text:
            buf_text = t
            buf_imgs = imgs.copy()
        elif len(buf_text) < MIN_LEN:
            buf_text = buf_text + "\n\n" + t
            buf_imgs = buf_imgs + imgs
        else:
            merged.append({"text": buf_text, "imgs": list(dict.fromkeys(buf_imgs))})
            buf_text = t
            buf_imgs = imgs.copy()
        if len(buf_text) > MAX_LEN:
            merged.append({"text": buf_text, "imgs": list(dict.fromkeys(buf_imgs))})
            buf_text = ""
            buf_imgs = []
    if buf_text:
        merged.append({"text": buf_text, "imgs": list(dict.fromkeys(buf_imgs))})

    # recursively split long chunks and carry their image refs
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=MAX_LEN,
        chunk_overlap=50,
        separators=["\n\n", "\n", ".", " ", ""]
    )

    final_chunks = []
    cid = 0
    for m in merged:
        text = m["text"].strip()
        imgs = m.get("imgs", []) or []
        if not text:
            continue
        if len(text) <= MAX_LEN:
            cid += 1
            final_chunks.append({"chunk_id": cid, "text": text, "image_refs": imgs})
        else:
            pieces = splitter.split_text(text)
            for p in pieces:
                p = p.strip()
                if not p:
                    continue
                cid += 1
                final_chunks.append({"chunk_id": cid, "text": p, "image_refs": imgs})

    return final_chunks

# ---------------------------------------------------------
# GENERATE AND STORE EMBEDDINGS
# ---------------------------------------------------------

@task(name="Embed to Chroma", retries=5, retry_delay_seconds=3)
def embed_chunks_to_chroma_task(chunks: List):
    logger.info("Starting direct Chroma load...")
    if not chunks: 
        logger.warning("No content found to embed.")
        return
    logger.info(f"{len(chunks)} chunks to be embedded ")
    # 2. Prepare Documents (LangChain Document objects)
    documents = [
        Document(page_content=chunk['text'], metadata={'index':chunk['chunk_id'], 'images':'|'.join(chunk['image_refs'])})
        for chunk in chunks
    ]
    
    if config.CHROMA_PATH.exists():
        logger.warning(f"Deleting existing Chroma collection: {config.CHROMA_COLLECTION}")
        try:
            Chroma(
                embedding_function=embeddings,
                persist_directory=str(config.CHROMA_PATH),
                collection_name=config.CHROMA_COLLECTION
            ).delete_collection()
        except Exception as e:
            pass
            
    # Recreate the clean directory structure ---
    # We must ensure the PARENT exists, then recreate the main Chroma directory itself.
    config.CHROMA_PATH.mkdir(parents=True, exist_ok=True)
    
    vector_store = Chroma(
        embedding_function=embeddings,
        persist_directory=str(config.CHROMA_PATH),
        collection_name=config.CHROMA_COLLECTION
    )
    
    # Add documents to the collection
    vector_store.add_documents(documents)
    logger.success(f"Successfully embedded and stored {len(documents)} document chunks in ChromaDB.")




# ---------------------------------------------------------
# MAIN FLOW & DEPLOYMENT
# ---------------------------------------------------------

@flow(name=config.FLOW_NAME, 
      task_runner=ConcurrentTaskRunner(),
      on_completion=[notify_area_hook], 
      on_failure=[notify_area_hook],
      on_cancellation=[notify_area_hook])
def process_area_flow():
    """
    Main flow for processing the Area Definition PDF.
    """
    logger.info("Starting Area Definition Extraction Pipeline...")
    
    if not config.PDF_FILE.exists():
        logger.critical(f"PDF File not found at path: {config.PDF_FILE}")
        return

    # 1. Split Pages
    pages_data = split_pages_task(config.PDF_FILE)
    if not pages_data:
        return

    # 2. Parallel Page Processing
    # Since we are using ConcurrentTaskRunner on the flow, we can use .submit()
    futures = []
    for page in pages_data:
        future = process_single_page_flow.submit(page)
        futures.append(future)

    results = [f.result() for f in futures]
    results.sort(key=lambda x: x['page_number'])

    # 3. Save Intermediate
    df = pd.DataFrame(results)
    df.to_csv(CSV_OUTPUT_FILE, index=False, quoting=csv.QUOTE_ALL)

    # 4. split to chunks
    splits=chunk_text_task(df)
    
    # 5. Generate embedding
    embed_chunks_to_chroma_task(splits)

    # 6. Identify Sections
    qa_blocks = identify_qa_sections(results)
    if not qa_blocks:
        logger.warning("No Q&A patterns found.")
        return

    # 7. Structure Rules (Parallel)
    rule_futures = []
    for block in qa_blocks:
        rf = structure_rule_with_llm.submit(block)
        rule_futures.append(rf)
        
    structured_rules = [f.result() for f in rule_futures]
    
    # 8. Load to DB
    if structured_rules:
        # Save Local CSV
        rules_df = pd.DataFrame(structured_rules)
        rules_df.to_csv(RULES_CSV_OUTPUT_FILE, index=False, quoting=csv.QUOTE_ALL)
        load_rules_to_db(rules_df)
    else:
        logger.error("Failed to extract rules.")

if __name__ == "__main__":
    print(f"🚀 {config.FLOW_NAME} Worker is running and listening on '{config.DEPLOYMENT_NAME}'...")
    serve(process_area_flow.to_deployment(name=config.DEPLOYMENT_NAME))