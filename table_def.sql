CREATE TABLE IF NOT EXISTS project_task (
    task_id        INTEGER PRIMARY KEY,
    task_name      VARCHAR NOT NULL,
    duration_days  INTEGER,
    start_date     DATE,
    finish_date    DATE
);

CREATE TABLE IF NOT EXISTS regulatory_rules (
    rule_id           VARCHAR PRIMARY KEY,
    rule_summary      TEXT,
    measurement_basis VARCHAR
);
