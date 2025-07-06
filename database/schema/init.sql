-- Initial database schema for typing clients
-- Preserves the critical Name/Email/Type → content mapping from CSV

-- Drop tables if doing a clean install (commented out for safety)
-- DROP TABLE IF EXISTS people CASCADE;

-- Core people table - matches RowContext from row_context.py
CREATE TABLE IF NOT EXISTS people (
    row_id VARCHAR PRIMARY KEY,      -- From CSV row_id (e.g., "484")
    name VARCHAR NOT NULL,           -- Person name - CRITICAL for step 6 mapping
    email VARCHAR,                   -- Email address
    personality_type VARCHAR,        -- Type like "FF-Si/Fi-SC/P(B) #3" - CRITICAL
    source_link VARCHAR,             -- Google Doc URL from sheet
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for common queries
CREATE INDEX IF NOT EXISTS idx_people_type ON people(personality_type);
CREATE INDEX IF NOT EXISTS idx_people_name ON people(name);

-- Grant permissions to our application user
GRANT ALL PRIVILEGES ON TABLE people TO typing_user;

-- Simple function to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger to automatically update updated_at (with IF NOT EXISTS)
DROP TRIGGER IF EXISTS update_people_updated_at ON people;
CREATE TRIGGER update_people_updated_at 
    BEFORE UPDATE ON people 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();