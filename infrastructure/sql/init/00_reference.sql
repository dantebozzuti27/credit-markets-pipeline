-- Reference Schema: Configuration tables for pipeline

CREATE SCHEMA IF NOT EXISTS reference;

CREATE TABLE IF NOT EXISTS reference.fred_series (
     series_id VARCHAR(20) PRIMARY KEY
    ,name VARCHAR(100) NOT NULL
    ,category VARCHAR(50) NOT NULL
    ,is_active BOOLEAN DEFAULT TRUE
    ,created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS reference.sec_companies (
     cik VARCHAR(10) PRIMARY KEY
    ,ticker VARCHAR(10) NOT NULL
    ,name VARCHAR(255) NOT NULL
    ,is_active BOOLEAN DEFAULT TRUE
    ,created_at TIMESTAMP DEFAULT NOW() 
);