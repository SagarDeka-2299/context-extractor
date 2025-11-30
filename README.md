# Context Extraction Pipeline (Real-Estate & Construction PDFs)

This project is a full data-extraction pipeline built for two domain-specific PDF documents:

1. **URA-Circular on GFA Area Definition.pdf**
2. **Project Schedule Document.pdf**

The goal is to convert these heterogeneous PDFs into:
• **Structured relational data** (PostgreSQL)
• **Semantic embeddings** (ChromaDB)
• **Searchable contextual knowledge** for downstream AI agents.


---

## Screenshots

Placeholders:

<img width="1386" height="837" alt="Screenshot 2025-11-30 at 6 18 45 AM" src="https://github.com/user-attachments/assets/dbcc963f-058c-4433-8227-49758bff94c0" />

<img width="1315" height="828" alt="Screenshot 2025-11-30 at 6 20 48 AM" src="https://github.com/user-attachments/assets/24d6ceae-8450-421c-ae7f-331c433e10dc" />

<img width="1386" height="851" alt="Screenshot 2025-11-30 at 6 19 01 AM" src="https://github.com/user-attachments/assets/27f2bdb9-89c7-4493-88e2-9b3270bb0f1c" />

<img width="1339" height="840" alt="Screenshot 2025-11-30 at 6 21 28 AM" src="https://github.com/user-attachments/assets/1d0515ac-b834-4c50-a612-26864b050b7e" />

<img width="1340" height="835" alt="Screenshot 2025-11-30 at 6 19 41 AM" src="https://github.com/user-attachments/assets/fb87b271-0f84-4fba-bb0f-0a4d0d27a47d" />

<img width="1341" height="838" alt="Screenshot 2025-11-30 at 6 20 27 AM" src="https://github.com/user-attachments/assets/6a02198c-b12e-423e-b518-30cd4c784cd9" />


---

## Overview

The system is powered by **Prefect 3**, **Django**, **dltHub**, and a mix of classical PDF parsing and LLM/VLM-based extraction.
Two Prefect workflows run as long-lived services, triggered by the Django app.

### 🧩 Workflow 1 – Project Schedule Document

<img width="512" height="768" alt="image" src="https://github.com/user-attachments/assets/dfeff2c0-f1f7-4938-b5a0-9111dd3d5cd5" />


Used for extracting structured construction schedule data (Gantt-like table).
Key steps:

• Cropping & table extraction with **pdfplumber**
• Cleaning tabular data
• Storing normalized rows into **PostgreSQL**
• Generating semantic text (LLM) → chunking → embeddings → **ChromaDB**

### 🧩 Workflow 2 – URA Circular on GFA Area Definition

<img width="1516" height="1070" alt="image" src="https://github.com/user-attachments/assets/4c073924-95cd-4477-acac-1717bbc6136a" />


This document contains rules, definitions, Q&A, and diagrams.
Key steps:

• Page-level splitting
• Image-containing pages routed to a **Vision-Language Model (VLM)** to detect *diagram pages*
• VLM-generated text for diagrams
• Non-diagram pages extracted with **local OCR (pdfplumber)**
• Rule schema inferred via regex + LLM *structured output* → stored in **regulatory_rules** table
• Combined document text → split by semantic regions → embeddings → **ChromaDB**

---

## Data Schemas

### `project_task`

```sql
CREATE TABLE IF NOT EXISTS project_task (
    task_id        INTEGER PRIMARY KEY,
    task_name      VARCHAR NOT NULL,
    duration_days  INTEGER,
    start_date     DATE,
    finish_date    DATE
);
```

### `regulatory_rules`

```sql
CREATE TABLE IF NOT EXISTS regulatory_rules (
    rule_id           VARCHAR PRIMARY KEY,
    rule_summary      TEXT,
    measurement_basis VARCHAR
);
```

Both schemas align with the challenge requirements in the assignment document .

---

## Tech Stack

* **Prefect 3** (orchestration, workflow services)
* **Django + Django Ninja** (API triggers)
* **dltHub** (data loading to Postgres & ChromaDB)
* **PostgreSQL** (structured storage)
* **ChromaDB** (vector store)
* **pdfplumber** for deterministic extraction
* **VLM + LLMs** for diagrams, schema inference & description generation
* Local OCR for fallback extraction
* **uv** for dependency & environment management

---

## Setup

Clone the repository:

```bash
git clone <your-repo-url>
cd <project>
```

Install dependencies:

```bash
uv sync
```

Configure environment:

```bash
cp demo.env .env
```

Run workflows:

```bash
uv run area_definition_workflow_config.py
uv run project_schedule_workflow_config.py
```

Start Django server:

```bash
uv run manage.py runserver
```

Waa-lla — everything runs.


---
