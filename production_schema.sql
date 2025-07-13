-- Production Database Schema for UUID-based S3 System
-- Created: 2025-07-13
-- 
-- This schema implements the simplified two-table design:
-- - person: Contains all person information
-- - file: Contains all file information with person relationships

-- Drop existing tables if they exist (for clean install)
DROP TABLE IF EXISTS file CASCADE;
DROP TABLE IF EXISTS person CASCADE;

-- Person table - central registry of all people
CREATE TABLE person (
    person_id SERIAL PRIMARY KEY,
    row_id INTEGER UNIQUE NOT NULL,  -- Original CSV row_id for compatibility
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    type VARCHAR(100),  -- personality type
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- File table - all files with UUID-based storage
CREATE TABLE file (
    file_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    person_id INTEGER NOT NULL REFERENCES person(person_id) ON DELETE CASCADE,
    storage_path TEXT NOT NULL,  -- S3 path: files/{uuid}.{ext}
    original_filename TEXT NOT NULL,  -- Original filename from source
    file_type VARCHAR(50) NOT NULL,  -- audio, video, document, etc.
    file_extension VARCHAR(20) NOT NULL,  -- .mp3, .mp4, etc.
    file_size BIGINT,  -- Size in bytes
    source_checksum VARCHAR(64),  -- Original file checksum
    mime_type VARCHAR(100),  -- MIME type for serving
    source_s3_path TEXT,  -- Original S3 path for reference
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB  -- Additional metadata as needed
);

-- Indexes for performance
CREATE INDEX idx_person_row_id ON person(row_id);
CREATE INDEX idx_person_name ON person(name);
CREATE INDEX idx_person_email ON person(email);

CREATE INDEX idx_file_person_id ON file(person_id);
CREATE INDEX idx_file_type ON file(file_type);
CREATE INDEX idx_file_extension ON file(file_extension);
CREATE INDEX idx_file_storage_path ON file(storage_path);
CREATE INDEX idx_file_uploaded_at ON file(uploaded_at);
CREATE INDEX idx_file_metadata ON file USING GIN(metadata);

-- Constraints
ALTER TABLE person ADD CONSTRAINT person_row_id_positive CHECK (row_id > 0);
ALTER TABLE person ADD CONSTRAINT person_name_not_empty CHECK (length(trim(name)) > 0);

ALTER TABLE file ADD CONSTRAINT file_size_positive CHECK (file_size IS NULL OR file_size >= 0);
ALTER TABLE file ADD CONSTRAINT file_storage_path_format CHECK (storage_path ~ '^files/[a-f0-9-]+\.[a-zA-Z0-9]+$');
ALTER TABLE file ADD CONSTRAINT file_extension_format CHECK (file_extension ~ '^\.[a-zA-Z0-9]+$');

-- Update trigger for person table
CREATE OR REPLACE FUNCTION update_person_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER person_update_timestamp
    BEFORE UPDATE ON person
    FOR EACH ROW
    EXECUTE FUNCTION update_person_timestamp();

-- Views for common queries
CREATE VIEW person_file_summary AS
SELECT 
    p.person_id,
    p.row_id,
    p.name,
    p.email,
    p.type,
    COUNT(f.file_id) as total_files,
    COUNT(CASE WHEN f.file_type = 'audio' THEN 1 END) as audio_files,
    COUNT(CASE WHEN f.file_type = 'video' THEN 1 END) as video_files,
    COUNT(CASE WHEN f.file_type = 'document' THEN 1 END) as document_files,
    COALESCE(SUM(f.file_size), 0) as total_size_bytes,
    ROUND(COALESCE(SUM(f.file_size), 0) / 1024.0 / 1024.0, 2) as total_size_mb
FROM person p
LEFT JOIN file f ON p.person_id = f.person_id
GROUP BY p.person_id, p.row_id, p.name, p.email, p.type
ORDER BY p.row_id;

CREATE VIEW file_type_summary AS
SELECT 
    file_type,
    file_extension,
    COUNT(*) as file_count,
    COALESCE(SUM(file_size), 0) as total_size_bytes,
    ROUND(COALESCE(SUM(file_size), 0) / 1024.0 / 1024.0, 2) as total_size_mb,
    ROUND(AVG(file_size), 0) as avg_size_bytes
FROM file
GROUP BY file_type, file_extension
ORDER BY file_type, file_extension;

-- Function to get files by person
CREATE OR REPLACE FUNCTION get_person_files(p_row_id INTEGER)
RETURNS TABLE (
    file_id UUID,
    storage_path TEXT,
    original_filename TEXT,
    file_type VARCHAR,
    file_size BIGINT,
    uploaded_at TIMESTAMP
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        f.file_id,
        f.storage_path,
        f.original_filename,
        f.file_type,
        f.file_size,
        f.uploaded_at
    FROM file f
    JOIN person p ON f.person_id = p.person_id
    WHERE p.row_id = p_row_id
    ORDER BY f.uploaded_at DESC;
END;
$$ LANGUAGE plpgsql;

-- Function to get files by type
CREATE OR REPLACE FUNCTION get_files_by_type(f_type TEXT)
RETURNS TABLE (
    file_id UUID,
    person_name TEXT,
    person_row_id INTEGER,
    storage_path TEXT,
    original_filename TEXT,
    file_size BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        f.file_id,
        p.name,
        p.row_id,
        f.storage_path,
        f.original_filename,
        f.file_size
    FROM file f
    JOIN person p ON f.person_id = p.person_id
    WHERE f.file_type = f_type
    ORDER BY p.row_id, f.original_filename;
END;
$$ LANGUAGE plpgsql;

-- Function to search files
CREATE OR REPLACE FUNCTION search_files(search_term TEXT)
RETURNS TABLE (
    file_id UUID,
    person_name TEXT,
    person_row_id INTEGER,
    storage_path TEXT,
    original_filename TEXT,
    file_type VARCHAR
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        f.file_id,
        p.name,
        p.row_id,
        f.storage_path,
        f.original_filename,
        f.file_type
    FROM file f
    JOIN person p ON f.person_id = p.person_id
    WHERE 
        f.original_filename ILIKE '%' || search_term || '%'
        OR p.name ILIKE '%' || search_term || '%'
        OR f.file_type ILIKE '%' || search_term || '%'
    ORDER BY p.row_id, f.original_filename;
END;
$$ LANGUAGE plpgsql;

-- Insert initial configuration
INSERT INTO person (row_id, name, email, type) VALUES (0, 'System', 'system@typing-clients.com', 'SYSTEM')
ON CONFLICT (row_id) DO NOTHING;

-- Grant permissions to migration user
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO migration_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO migration_user;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO migration_user;

-- Comments for documentation
COMMENT ON TABLE person IS 'Central registry of all people in the typing clients system';
COMMENT ON TABLE file IS 'All files stored with UUID-based naming and person associations';
COMMENT ON COLUMN file.storage_path IS 'S3 path in format: files/{uuid}.{extension}';
COMMENT ON COLUMN file.source_s3_path IS 'Original S3 path for migration reference';
COMMENT ON COLUMN file.metadata IS 'Additional file metadata stored as JSON';

-- Print summary
DO $$
BEGIN
    RAISE NOTICE 'Production schema created successfully!';
    RAISE NOTICE 'Tables: person, file';
    RAISE NOTICE 'Views: person_file_summary, file_type_summary';
    RAISE NOTICE 'Functions: get_person_files(), get_files_by_type(), search_files()';
END $$;