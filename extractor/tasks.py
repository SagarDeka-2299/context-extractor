from prefect import flow, task
from prefect.states import State
from prefect.client.schemas.objects import FlowRun
import requests
import time
import random

# --- CONFIGURATION ---
API_BASE = "http://127.0.0.1:8000/api/webhooks"

# --- HOOKS ---
def notify_schedule_hook(flow, flow_run: FlowRun, state: State):
    status = "success" if state.is_completed() else "failed"
    try:
        requests.post(f"{API_BASE}/schedule/update_status", json={"status": status})
        print(f"🪝 Hook: Schedule -> {status}")
    except Exception as e:
        print(f"⚠️ Hook Error: {e}")

def notify_area_hook(flow, flow_run: FlowRun, state: State):
    status = "success" if state.is_completed() else "failed"
    try:
        requests.post(f"{API_BASE}/area/update_status", json={"status": status})
        print(f"🪝 Hook: Area -> {status}")
    except Exception as e:
        print(f"⚠️ Hook Error: {e}")

# --- TASKS ---
@task
def extract_pdf_content(doc_type: str):
    print(f"📄 Processing {doc_type}...")
    for i in range(5):
        time.sleep(1) 
        print(f"   ...processing {i+1}/5")
    
    if random.random() < 0.1: # 10% chance of failure
        raise ValueError("Random PDF Error")
    return "Done"

# --- FLOWS ---
@flow(name="Schedule Extractor", 
      on_completion=[notify_schedule_hook], 
      on_failure=[notify_schedule_hook],
      on_cancellation=[notify_schedule_hook])
def process_schedule_flow():
    extract_pdf_content("Project Schedule")

@flow(name="Area Extractor", 
      on_completion=[notify_area_hook], 
      on_failure=[notify_area_hook],
      on_cancellation=[notify_area_hook])
def process_area_flow():
    extract_pdf_content("URA Circular")

if __name__ == "__main__":
    from prefect import serve
    print("🚀 Worker is running and listening for Django...")
    
    s1 = process_schedule_flow.to_deployment(name="schedule-deploy")
    s2 = process_area_flow.to_deployment(name="area-deploy")
    
    # This keeps the process alive and listening
    serve(s1, s2)