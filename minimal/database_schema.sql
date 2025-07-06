-- Xenodex Database Schema
-- Normalized design to handle growing complexity

-- Main people table
CREATE TABLE people (
    id INT PRIMARY KEY AUTO_INCREMENT,
    row_id INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    type VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_row_id (row_id),
    INDEX idx_email (email),
    INDEX idx_type (type)
);

-- Documents table (Google Docs, YouTube videos, etc)
CREATE TABLE documents (
    id INT PRIMARY KEY AUTO_INCREMENT,
    person_id INT NOT NULL,
    url TEXT NOT NULL,
    document_type ENUM('google_doc', 'youtube', 'google_drive', 'other') DEFAULT 'other',
    document_text LONGTEXT,
    processed BOOLEAN DEFAULT FALSE,
    extraction_date TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (person_id) REFERENCES people(id) ON DELETE CASCADE,
    INDEX idx_person_id (person_id),
    INDEX idx_processed (processed)
);

-- Extracted links table (many-to-many relationship)
CREATE TABLE extracted_links (
    id INT PRIMARY KEY AUTO_INCREMENT,
    document_id INT NOT NULL,
    url TEXT NOT NULL,
    link_type ENUM('youtube_video', 'youtube_playlist', 'google_drive_file', 'google_drive_folder', 'other') DEFAULT 'other',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE,
    INDEX idx_document_id (document_id),
    INDEX idx_link_type (link_type)
);

-- Download tracking table
CREATE TABLE downloads (
    id INT PRIMARY KEY AUTO_INCREMENT,
    link_id INT NOT NULL,
    status ENUM('pending', 'downloading', 'completed', 'failed', 'permanent_failure') DEFAULT 'pending',
    file_path TEXT,
    media_id VARCHAR(255),
    last_attempt TIMESTAMP NULL,
    error_message TEXT,
    retry_count INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (link_id) REFERENCES extracted_links(id) ON DELETE CASCADE,
    INDEX idx_link_id (link_id),
    INDEX idx_status (status),
    INDEX idx_media_id (media_id)
);

-- Processing log table (for audit trail)
CREATE TABLE processing_log (
    id INT PRIMARY KEY AUTO_INCREMENT,
    person_id INT,
    action VARCHAR(100) NOT NULL,
    status ENUM('started', 'completed', 'failed') NOT NULL,
    details TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (person_id) REFERENCES people(id) ON DELETE SET NULL,
    INDEX idx_person_id (person_id),
    INDEX idx_action (action),
    INDEX idx_created_at (created_at)
);

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