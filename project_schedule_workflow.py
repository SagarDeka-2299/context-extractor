import pdfplumber
import pandas as pd
import numpy as np
import os
import shutil
import sys
import dlt
import chromadb
import uuid
import requests
from typing import Tuple, List, Any, Dict
from pathlib import Path
from dotenv import load_dotenv
import urllib.parse
# Prefect & Loguru
from prefect import flow, task, serve
from prefect.states import State
from prefect.client.schemas.objects import FlowRun
from prefect.logging import get_run_logger as get_prefect_logger
from loguru import logger
from prefect import serve



# LangChain (Embeddings only)
from langchain_openai import OpenAIEmbeddings
from langchain_chroma import Chroma
from langchain_core.documents import Document

# --- IMPORT CONFIG ---
import project_schedule_workflow_config as config

load_dotenv()

# ---------------------------------------------------------
# WEBHOOK HOOK
# ---------------------------------------------------------
def notify_schedule_hook(flow, flow_run: FlowRun, state: State):
    """
    Prefect hook to notify Django about the flow status.
    Uses config.WEBHOOK_URL for the endpoint.
    """
    status = "success" if state.is_completed() else "failed"
    payload = {"status": status}
    
    try:
        # Construct full URL using the base from config
        full_url = f"{config.WEBHOOK_URL}/schedule/update_status"
        requests.post(full_url, json=payload)
        print(f"🪝 Webhook sent: Schedule -> {status}")
    except Exception as e:
        print(f"⚠️ Webhook Error: {e}")

# ---------------------------------------------------------
# GLOBAL VARIABLES & LOCAL SETTINGS
# ---------------------------------------------------------

# Ensure Root Exists
config.OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

# 1. Internal Paths
CLEANED_CSV_PATH = config.OUTPUT_ROOT / "cleaned_project_schedule.csv"
FINAL_CSV_PATH = config.OUTPUT_ROOT / "final_project_schedule.csv"
REPORT_PATH = config.OUTPUT_ROOT / "project_schedule_report.txt"

# 2. PDF Settings
BOUNDING_BOX = (58, 60, 320, 532) 
HARDCODED_COLUMNS = ["Task Name", "Duration", "Start", "Finish"]

# 3. Database Connection
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# ENCODE CREDENTIALS to handle special characters like '@' in the password
# quote_plus replaces '@' with '%40', making it safe for the URL
safe_user = urllib.parse.quote_plus(DB_USER)
safe_password = urllib.parse.quote_plus(DB_PASSWORD)

SQL_CONNECTION_STRING = f"postgresql://{safe_user}:{safe_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# 4. Global Embedding Model
os.environ["OPENAI_API_KEY"]=os.environ["OPENAI_API_KEY_SUPPORT_EMBEDDING"]
os.environ["OPENAI_BASE_URL"]=os.environ["OPENAI_BASE_URL_SUPPORT_EMBEDDING"]
EMBEDDING_MODEL = OpenAIEmbeddings(
    model='text-embedding-3-large',
)

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
# TASKS & FLOW
# ---------------------------------------------------------

# ... [Tasks: is_cell_colored, extract_page_data_logic, extract_pdf_task, clean_data_task, apply_hierarchy_task are same as previous] ...
# (Helper functions kept concise for brevity, identical to previous version)

def is_cell_colored(page: pdfplumber.page.Page, cell_box: Tuple[float, float, float, float]) -> bool:
    if not cell_box: return False
    cx = (cell_box[0] + cell_box[2]) / 2
    cy = (cell_box[1] + cell_box[3]) / 2
    for rect in page.rects:
        if (rect['x0'] <= cx <= rect['x1']) and (rect['top'] <= cy <= rect['bottom']):
            if 'non_stroking_color' in rect:
                color = rect['non_stroking_color']
                if isinstance(color, (list, tuple)) and len(color) == 3: return True
    return False

def extract_page_data_logic(page: pdfplumber.page.Page) -> pd.DataFrame:
    tables = page.find_tables()
    if not tables: return pd.DataFrame()
    table = tables[0]
    text_data = table.extract()
    if not text_data or len(text_data[0]) != 4: return pd.DataFrame()
    color_flags = []
    for row in table.rows:
        has_color = False
        if hasattr(row, 'cells') and len(row.cells) > 0:
            has_color = is_cell_colored(page, row.cells[0])
        color_flags.append(has_color)
    df = pd.DataFrame(text_data)
    df.columns = HARDCODED_COLUMNS
    df['Has_Color'] = color_flags
    return df

@task(name="Extract PDF Data", retries=3, retry_delay_seconds=2)
def extract_pdf_task(file_path: Path, bounding_box: Tuple[int, int, int, int]) -> pd.DataFrame:
    logger.info(f"Starting PDF extraction from: {file_path}")
    if not file_path.exists():
        logger.critical(f"File not found: {file_path}")
        return pd.DataFrame()
    all_dfs = []
    try:
        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                cropped = page.crop(bounding_box)
                try:
                    df_page = extract_page_data_logic(cropped)
                    if not df_page.empty: all_dfs.append(df_page)
                except Exception: continue
    except Exception as e:
        logger.error(f"Critical PDF error: {e}")
        raise e
    if not all_dfs: return pd.DataFrame()
    return pd.concat(all_dfs, ignore_index=True)

@task(name="Clean Data", retries=3, retry_delay_seconds=2)
def clean_data_task(df: pd.DataFrame) -> pd.DataFrame:
    df = df.reset_index(drop=True)
    df.insert(0, 'ID', np.arange(1, len(df) + 1))
    cols_to_check = ["Task Name", "Duration", "Start", "Finish"]
    df[cols_to_check] = df[cols_to_check].replace(r'^\s*$', np.nan, regex=True)
    df = df.dropna(subset=cols_to_check, how='all')
    df["ID"] = df["ID"].astype(int)
    df["Has_Color"] = df["Has_Color"].astype(bool)
    df["Task Name"] = df["Task Name"].astype(str)
    df.to_csv(CLEANED_CSV_PATH, index=False)
    return df

@task(name="Apply Hierarchy", retries=3, retry_delay_seconds=2)
def apply_hierarchy_task(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Applying hierarchy logic...")
    building_tasks, summary_tasks = [], []
    current_building, current_summary = None, None
    has_color_col = df['Has_Color'].tolist()
    names = df['Task Name'].tolist()
    n = len(df)
    for i in range(n):
        is_colored = has_color_col[i]
        is_next_colored = has_color_col[i+1] if i + 1 < n else False
        task_name = names[i]
        if is_colored:
            if is_next_colored:
                current_building, current_summary = task_name, None
                building_tasks.append(task_name); summary_tasks.append(None)
            else:
                current_summary = task_name
                building_tasks.append(current_building); summary_tasks.append(task_name)
        else:
            building_tasks.append(current_building); summary_tasks.append(current_summary)
    
    df = df.rename(columns={"ID": "task_id", "Task Name": "task_name"})
    df['duration_days'] = df['Duration'].astype(str).str.extract(r'(\d+)').fillna(0).astype(int)
    df['start_date'] = pd.to_datetime(df['Start'], format='%a %m/%d/%y', errors='coerce').dt.date
    df['finish_date'] = pd.to_datetime(df['Finish'], format='%a %m/%d/%y', errors='coerce').dt.date
    df['building_task'] = building_tasks; df['summary_task'] = summary_tasks
    final_cols = ['task_id', 'task_name', 'duration_days', 'start_date', 'finish_date', 'building_task', 'summary_task']
    final_df = df[final_cols].copy()
    final_df.to_csv(FINAL_CSV_PATH, index=False)
    return final_df

@task(name="Load to Postgres (DLT)")
def load_to_postgres_dlt(df: pd.DataFrame, retries=3, retry_delay_seconds=2):
    logger.info("Initiating DLT load to PostgreSQL...")
    sql_df = df[['task_id', 'task_name', 'duration_days', 'start_date', 'finish_date']].copy()
    pipeline = dlt.pipeline(pipeline_name="project_schedule_pipeline", destination=dlt.destinations.postgres(credentials=SQL_CONNECTION_STRING), dataset_name="public")
    info = pipeline.run(sql_df, table_name="Project_Task", write_disposition="replace", primary_key="task_id")
    logger.success(f"SQL Load Complete. Info: {info}")

@task(name="Generate Text Report")
def generate_report_task(df: pd.DataFrame, retries=3, retry_delay_seconds=2) -> str:
    logger.info("Generating report...")
    df['start_date_dt'] = pd.to_datetime(df['start_date'])
    df['finish_date_dt'] = pd.to_datetime(df['finish_date'])
    df_tasks = df.dropna(subset=['summary_task'])
    groups = df_tasks[['building_task', 'summary_task']].drop_duplicates()
    lines = []

    # --- NEW: write building-level summaries for rows where summary_task is null ---
    # find building rows where summary_task is NaN (these represent whole-building summaries)
    building_summaries = df[df['summary_task'].isna() & df['building_task'].notna()]
    # keep one entry per building
    building_summaries = building_summaries.drop_duplicates(subset=['building_task'])
    for _, b_row in building_summaries.iterrows():
        b_task = b_row['building_task']
        # prefer a row whose task_name equals building name if available (more canonical)
        candidate = df[
            (df['building_task'] == b_task) &
            (df['task_name'] == b_task)
        ]
        if not candidate.empty:
            h_data = candidate.iloc[0]
        else:
            h_data = b_row

        s_str = h_data['start_date_dt'].strftime('%d %B %Y').lower() if pd.notna(h_data['start_date_dt']) else "N/A"
        f_str = h_data['finish_date_dt'].strftime('%d %B %Y').lower() if pd.notna(h_data['finish_date_dt']) else "N/A"
        lines.append("-" * 40)
        lines.append(f"# The whole `{b_task}` project will take {h_data['duration_days']} days, it is scheduled from {s_str} to {f_str}.")
        lines.append("-" * 40 + "\n")
    # --- end NEW section ---

    for _, row_group in groups.iterrows():
        b_task, s_task = row_group['building_task'], row_group['summary_task']
        subset = df_tasks[(df_tasks['summary_task'] == s_task) & (df_tasks['building_task'] == b_task if pd.notna(b_task) else df_tasks['building_task'].isna())]
        if subset.empty: continue
        h_data = subset[subset['task_name'] == s_task].iloc[0]
        children = subset[subset['task_name'] != s_task]
        context_str = f" for the {b_task}" if pd.notna(b_task) and str(b_task).strip().lower() != "pre_construction" else ""
        s_str = h_data['start_date_dt'].strftime('%A, %B %d, %Y') if pd.notna(h_data['start_date_dt']) else "N/A"
        f_str = h_data['finish_date_dt'].strftime('%A, %B %d, %Y') if pd.notna(h_data['finish_date_dt']) else "N/A"
        lines.append(f"{s_task.upper()}{context_str} takes {h_data['duration_days']} days, starting on {s_str} and finishing on {f_str}.")
        lines.append("sub tasks are:")
        for _, child in children.iterrows():
            cs = child['start_date_dt'].strftime('%d %B %Y').lower() if pd.notna(child['start_date_dt']) else "N/A"
            cf = child['finish_date_dt'].strftime('%d %B %Y').lower() if pd.notna(child['finish_date_dt']) else "N/A"
            lines.append(f"  - {child['task_name']}: takes {child['duration_days']} days scheduled from {cs} to {cf}")
        lines.append("\n" + "-"*40 + "\n") #Adding separator
    report_content = "\n".join(lines)
    with open(REPORT_PATH, "w") as f: f.write(report_content)
    return report_content

@task(name="Embed to Chroma")
def embed_data_task(report_content: str, retries=5, retry_delay_seconds=3):
    logger.info("Starting direct Chroma load...")
    
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
            
    # --- 2. Recreate the clean directory structure ---
    # We must ensure the PARENT exists, then recreate the main Chroma directory itself.
    config.CHROMA_PATH.mkdir(parents=True, exist_ok=False)
    # 1. Chunking
    separator = "-" * 40
    chunks = [c.strip() for c in report_content.split(separator) if c.strip()]
    
    if not chunks: 
        logger.warning("No content found to embed.")
        return
    logger.info(f"{len(chunks)} chunks to be embedded ")
    # 2. Prepare Documents (LangChain Document objects)
    documents = [
        Document(page_content=chunk, metadata={'index':i})
        for i,chunk in enumerate(chunks)
    ]
    
    # 3. Get Embeddings Model
    embeddings = OpenAIEmbeddings(
        model='text-embedding-3-large',
    )
    
    # 4. Initialize and Load to Chroma DB
    if not os.path.exists(config.CHROMA_PATH):
        os.makedirs(config.CHROMA_PATH, exist_ok=True)
    
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
      on_completion=[notify_schedule_hook], 
      on_failure=[notify_schedule_hook],
      on_cancellation=[notify_schedule_hook]
      )
def process_schedule_flow():
    """
    Main flow for processing the project schedule PDF.
    """
    logger.info("Initializing Schedule Processing Workflow...")
    
    # 1. Extract
    raw_df = extract_pdf_task(config.PDF_FILE, BOUNDING_BOX)
    if raw_df.empty: 
        logger.error("No data extracted.")
        return

    # 2. Clean & Transform
    cleaned_df = clean_data_task(raw_df)
    final_df = apply_hierarchy_task(cleaned_df)
    
    # 3. Load SQL (DLT)
    load_to_postgres_dlt(final_df)
    
    # 4. Generate Report
    report_text = generate_report_task(final_df)
    
    # 5. Load Vector DB (DLT Custom Destination)
    embed_data_task(report_text)
    
    logger.success("Workflow completed successfully.")

if __name__ == "__main__":
    print(f"🚀 {config.FLOW_NAME} Worker is running and listening on '{config.DEPLOYMENT_NAME}'...")
    serve(process_schedule_flow.to_deployment(name=config.DEPLOYMENT_NAME))
