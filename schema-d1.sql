-- Company Search Database Schema for Cloudflare D1
-- This schema supports 2M+ Indian companies

DROP TABLE IF EXISTS companies;

CREATE TABLE companies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_name TEXT NOT NULL,
    cin TEXT UNIQUE NOT NULL,
    status TEXT,
    registration_date TEXT,
    company_class TEXT,
    roc TEXT,
    email TEXT,
    state TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

-- Create indexes for fast searching
CREATE INDEX idx_company_name ON companies(company_name);
CREATE INDEX idx_cin ON companies(cin);
CREATE INDEX idx_status ON companies(status);
CREATE INDEX idx_state ON companies(state);

-- Full-text search index for company names (SQLite FTS5)
CREATE VIRTUAL TABLE IF NOT EXISTS companies_fts USING fts5(
    company_name,
    cin,
    content='companies',
    content_rowid='id'
);

-- Trigger to keep FTS index in sync
CREATE TRIGGER companies_ai AFTER INSERT ON companies BEGIN
    INSERT INTO companies_fts(rowid, company_name, cin)
    VALUES (new.id, new.company_name, new.cin);
END;

CREATE TRIGGER companies_ad AFTER DELETE ON companies BEGIN
    DELETE FROM companies_fts WHERE rowid = old.id;
END;

CREATE TRIGGER companies_au AFTER UPDATE ON companies BEGIN
    UPDATE companies_fts 
    SET company_name = new.company_name, cin = new.cin
    WHERE rowid = new.id;
END;