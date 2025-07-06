-- Xenodex Database Schema (SQLite version)
-- Normalized design to handle growing complexity

-- Main people table
CREATE TABLE people (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    row_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    email TEXT,
    type TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_people_row_id ON people(row_id);
CREATE INDEX idx_people_email ON people(email);
CREATE INDEX idx_people_type ON people(type);

-- Documents table (Google Docs, YouTube videos, etc)
CREATE TABLE documents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id INTEGER NOT NULL,
    url TEXT NOT NULL,
    document_type TEXT CHECK(document_type IN ('google_doc', 'youtube', 'google_drive', 'other')) DEFAULT 'other',
    document_text TEXT,
    processed BOOLEAN DEFAULT FALSE,
    extraction_date TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (person_id) REFERENCES people(id) ON DELETE CASCADE
);
CREATE INDEX idx_documents_person_id ON documents(person_id);
CREATE INDEX idx_documents_processed ON documents(processed);

-- Extracted links table (many-to-many relationship)
CREATE TABLE extracted_links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id INTEGER NOT NULL,
    url TEXT NOT NULL,
    link_type TEXT CHECK(link_type IN ('youtube_video', 'youtube_playlist', 'google_drive_file', 'google_drive_folder', 'other')) DEFAULT 'other',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE
);
CREATE INDEX idx_extracted_links_document_id ON extracted_links(document_id);
CREATE INDEX idx_extracted_links_link_type ON extracted_links(link_type);

-- Download tracking table
CREATE TABLE downloads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    link_id INTEGER NOT NULL,
    status TEXT CHECK(status IN ('pending', 'downloading', 'completed', 'failed', 'permanent_failure')) DEFAULT 'pending',
    file_path TEXT,
    media_id TEXT,
    last_attempt TIMESTAMP NULL,
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (link_id) REFERENCES extracted_links(id) ON DELETE CASCADE
);
CREATE INDEX idx_downloads_link_id ON downloads(link_id);
CREATE INDEX idx_downloads_status ON downloads(status);
CREATE INDEX idx_downloads_media_id ON downloads(media_id);

-- Processing log table (for audit trail)
CREATE TABLE processing_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id INTEGER,
    action TEXT NOT NULL,
    status TEXT CHECK(status IN ('started', 'completed', 'failed')) NOT NULL,
    details TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (person_id) REFERENCES people(id) ON DELETE SET NULL
);
CREATE INDEX idx_processing_log_person_id ON processing_log(person_id);
CREATE INDEX idx_processing_log_action ON processing_log(action);
CREATE INDEX idx_processing_log_created_at ON processing_log(created_at);

-- Views for common queries
CREATE VIEW person_summary AS
SELECT 
    p.id,
    p.row_id,
    p.name,
    p.email,
    p.type,
    COUNT(DISTINCT d.id) as document_count,
    COUNT(DISTINCT el.id) as link_count,
    COUNT(DISTINCT CASE WHEN dl.status = 'completed' THEN dl.id END) as downloaded_count
FROM people p
LEFT JOIN documents d ON p.id = d.person_id
LEFT JOIN extracted_links el ON d.id = el.document_id
LEFT JOIN downloads dl ON el.id = dl.link_id
GROUP BY p.id;

CREATE VIEW download_queue AS
SELECT 
    p.name,
    p.email,
    d.url as document_url,
    el.url as link_url,
    el.link_type,
    dl.status,
    dl.last_attempt,
    dl.retry_count
FROM people p
JOIN documents d ON p.id = d.person_id
JOIN extracted_links el ON d.id = el.document_id
LEFT JOIN downloads dl ON el.id = dl.link_id
WHERE dl.status IS NULL OR dl.status IN ('pending', 'failed')
ORDER BY dl.retry_count ASC, p.row_id ASC;