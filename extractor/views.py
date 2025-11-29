

import sys
import os
from pathlib import Path
from django.shortcuts import render, redirect
from django.db import connection
from prefect.deployments import run_deployment
from extractor.state import get_status, set_status
from langchain_chroma import Chroma
from langchain_openai import OpenAIEmbeddings
from typing import List
import urllib # Added import for safety in helper function

# ---------------------------------------------------------
# CONFIGURATION IMPORT (From Root)
# ... [Omitted configuration imports for brevity] ...

try:
    import project_schedule_workflow_config as schedule_config
    import area_definition_workflow_config as area_config
except ImportError:
    schedule_config = None
    area_config = None


# ---------------------------------------------------------
# HELPER: FETCH DATA FROM DB (Omitted for brevity)
# ---------------------------------------------------------
def fetch_table_data(task_type):
    data = []
    table_name = "project_task" if task_type == 'schedule' else "regulatory_rules"
    query = f"SELECT * FROM {table_name} LIMIT 100;"

    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            
            for row in rows:
                row_dict = dict(zip(columns, row))
                clean_row = {k: v for k, v in row_dict.items() if not k.startswith('_dlt')}
                data.append(clean_row)
    except Exception as e:
        print(f"⚠️ Error fetching data from {table_name}: {e}")
        return []

    return data

# ---------------------------------------------------------
# NEW: SEMANTIC SEARCH LOGIC (With Debugging)
# ---------------------------------------------------------

def perform_semantic_search(query: str, task_type: str) -> List[dict]:
    """
    Connects to the appropriate Chroma DB and performs similarity search.
    Includes debug prints for verification.
    """
    
    config_module = schedule_config if task_type == 'schedule' else area_config

    try:
        # Load keys directly from OS environment
        os.environ['OPENAI_API_KEY'] = os.environ.get('OPENAI_API_KEY_SUPPORT_EMBEDDING', os.environ.get('OPENAI_API_KEY'))
        os.environ['OPENAI_BASE_URL'] = os.environ.get('OPENAI_BASE_URL_SUPPORT_EMBEDDING', os.environ.get('OPENAI_BASE_URL'))

        # Initialize the model 
        embeddings = OpenAIEmbeddings(
            model='text-embedding-3-large',
            base_url=os.environ.get('OPENAI_BASE_URL')
        )

        # 1. Connect to Persistent Chroma Store
        vector_store = Chroma(
            embedding_function=embeddings,
            persist_directory=str(config_module.CHROMA_PATH),
            collection_name=config_module.CHROMA_COLLECTION
        )
        
        # --- DEBUG STEP: Print Collection Info ---
        collection = vector_store.get().get('ids')
        num_records = len(collection) if collection else 0
        
        print("\n================ DEBUG: CHROMA STATE ================")
        print(f"Path: {config_module.CHROMA_PATH}")
        print(f"Total Records in Chroma DB: {num_records}")
        
        if num_records > 0:
            # Retrieve first 3 documents (content only)
            # Using query='' to get random/initial results
            debug_results = vector_store.similarity_search("", k=3)
            print("--- Top 3 Documents Retrieved for Debug ---")
            for i, doc in enumerate(debug_results):
                # Print only the start of the content to avoid massive logs
                print(f"[{i+1}] Content Start: {doc.page_content[:150]}...")
        print("=====================================================\n")
        # --- END DEBUG STEP ---
        
        # 2. Perform Search using user query
        results = vector_store.similarity_search_with_score(query, k=3)
        
        # 3. Format Results
        formatted = []
        for doc, score in results:
            formatted.append({
                'content': doc.page_content,
                'score': f"{score:.4f}",
                'metadata': doc.metadata
            })
        return formatted
    
    except Exception as e:
        print(f"❌ Symantic Search Error: {e}")
        return [{'error': f"Search failed. Error: {e}"}]

# ---------------------------------------------------------
# VIEWS (Omitted for brevity)
# ... [home_view, route_task_logic, view_setup, etc., remain the same] ...

def home_view(request):
    """Dashboard showing status of both extractors."""
    context = {
        'state': {
            'schedule': get_status('schedule'),
            'area': get_status('area')
        }
    }
    return render(request, 'extractor/home.html', context)

def route_task_logic(request, task_type):
    """Routing logic based on current state."""
    clean_key = 'schedule' if 'schedule' in task_type else 'area'
    current_status = get_status(clean_key)
    
    if current_status == 'running':
        return redirect(f'{clean_key}_doc_run_status')
    elif current_status == 'success':
        return redirect(f'{clean_key}_doc_table_data')
    else: 
        return redirect(f'{clean_key}_doc_setup_param')

def view_setup(request, task_type):
    """
    Handles File Upload -> Save to Disk -> Trigger Prefect Deployment.
    """
    clean_key = 'schedule' if 'schedule' in task_type else 'area'
    active_config = schedule_config if clean_key == 'schedule' else area_config

    if request.method == 'POST':
        uploaded_file = request.FILES.get('file')
        if not uploaded_file:
            return render(request, 'extractor/state_setup.html', {'task_type': clean_key, 'error': "Please upload a file."})

        if active_config:
            try:
                active_config.OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
                with open(active_config.PDF_FILE, 'wb+') as destination:
                    for chunk in uploaded_file.chunks():
                        destination.write(chunk)
                print(f"✅ File saved for worker: {active_config.PDF_FILE}")
            except Exception as e:
                print(f"❌ File save error: {e}")
                return render(request, 'extractor/state_setup.html', {'task_type': clean_key, 'error': f"Failed to save file: {str(e)}"})

        set_status(clean_key, 'running')
        
        deployment_id = f"{active_config.FLOW_NAME}/{active_config.DEPLOYMENT_NAME}" if active_config else f"Fallback/{clean_key}"

        try:
            print(f"🚀 Triggering Deployment: {deployment_id}")
            run_deployment(name=deployment_id, timeout=0)
        except Exception as e:
            print(f"❌ Prefect Trigger Error: {e}")
            set_status(clean_key, 'failed')
            return render(request, 'extractor/state_setup.html', {'task_type': clean_key, 'error': "Failed to start workflow. Check Prefect connection."})

        return redirect(f'{clean_key}_doc_run_status')

    context = {'task_type': clean_key}
    return render(request, 'extractor/state_setup.html', context)

def view_run_status(request, task_type):
    """
    Shows 'Running' spinner.
    ON RELOAD: Checks GLOBAL_STATE. If 'success', redirects to table.
    """
    clean_key = 'schedule' if 'schedule' in task_type else 'area'
    
    current_status = get_status(clean_key)
    
    if current_status == 'success':
        return redirect(f'{clean_key}_doc_table_data')
    elif current_status == 'failed':
        return redirect(f'{clean_key}_doc_setup_param')

    if request.method == 'POST':
        if 'cancel' in request.POST:
            set_status(clean_key, 'pending')
            return redirect('home')

    context = {'task_type': clean_key}
    return render(request, 'extractor/state_running.html', context)

def view_table_data(request, task_type):
    """
    Fetches the final data from PostgreSQL and displays it.
    """
    clean_key = 'schedule' if 'schedule' in task_type else 'area'
    rows = fetch_table_data(clean_key)
    context = {'task_type': clean_key, 'table_data': rows}
    return render(request, 'extractor/state_table.html', context)

def semantic_search_view(request, task_type):
    """
    Renders the search form and handles the POST request for semantic retrieval.
    """
    clean_key = 'schedule' if task_type == 'schedule' else 'area'
    results = []
    query = ""
    
    if request.method == 'POST':
        query = request.POST.get('query', '').strip()
        
        if query:
            results = perform_semantic_search(query, clean_key)

    context = {
        'task_type': clean_key,
        'query': query,
        'results': results
    }
    return render(request, 'extractor/semantic_search.html', context)