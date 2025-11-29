from pathlib import Path
# --- COMMON PATHS ---
OUTPUT_ROOT = Path("project_schedule_content")
PDF_FILE = OUTPUT_ROOT / "project_schedule_document.pdf"
CHROMA_PATH = OUTPUT_ROOT / "chroma_db"
LOG_FILE = OUTPUT_ROOT / "project_schedule.log"
CHROMA_COLLECTION="project_schedule_chunks"

# --- DEPLOYMENT SETTINGS ---
FLOW_NAME = "Schedule Extractor" 
DEPLOYMENT_NAME = "schedule-deploy"
WEBHOOK_URL = "http://127.0.0.1:8000/api/webhooks"