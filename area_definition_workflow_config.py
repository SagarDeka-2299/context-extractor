from pathlib import Path

# --- COMMON PATHS ---
OUTPUT_ROOT = Path("area_definition_contents")
PDF_FILE = OUTPUT_ROOT / "area_definition_doc.pdf"
LOG_FILE = OUTPUT_ROOT / "pipeline_execution.log"
CHROMA_PATH=OUTPUT_ROOT/"chroma_db"
CHROMA_COLLECTION="area_definition_chunks"

# --- DEPLOYMENT SETTINGS ---
FLOW_NAME = "Area Extractor" 
DEPLOYMENT_NAME = "area-deploy"
WEBHOOK_URL = "http://127.0.0.1:8000/api/webhooks"
