import json
from pathlib import Path
import os

# Path to the state file
STATE_FILE = Path(__file__).parent / "process_state.json"

# Default structure: Key maps directly to status string
DEFAULT_STATE = {
    'schedule': 'pending',
    'area': 'pending'
}

def _load_state():
    """Internal function to load the flat state from the JSON file."""
    if not STATE_FILE.exists():
        return DEFAULT_STATE
    try:
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    except Exception:
        # If the file is corrupted, return the default state
        return DEFAULT_STATE

def _save_state(data):
    """Internal function to save the flat state to disk."""
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, 'w') as f:
        json.dump(data, f, indent=4)

# --- Public Interface (Explicit Functions) ---

def get_status(doc_key: str) -> str:
    """
    Returns the current status string for a given document key ('schedule' or 'area').
    """
    data = _load_state()
    return data.get(doc_key, 'pending')

def set_status(doc_key: str, status: str):
    """
    Sets the status string for a given document key and saves the state to disk.
    """
    data = _load_state()
    data[doc_key] = status
    _save_state(data)