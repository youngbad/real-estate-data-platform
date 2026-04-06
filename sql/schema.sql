IF OBJECT_ID('dbo.fact_macro_indicators', 'U') IS NOT NULL DROP TABLE dbo.fact_macro_indicators;
IF OBJECT_ID('dbo.fact_listings', 'U') IS NOT NULL DROP TABLE dbo.fact_listings;
IF OBJECT_ID('dbo.dim_source', 'U') IS NOT NULL DROP TABLE dbo.dim_source;
IF OBJECT_ID('dbo.dim_property', 'U') IS NOT NULL DROP TABLE dbo.dim_property;
IF OBJECT_ID('dbo.dim_location', 'U') IS NOT NULL DROP TABLE dbo.dim_location;
IF OBJECT_ID('dbo.dim_date', 'U') IS NOT NULL DROP TABLE dbo.dim_date;

-- ====================================================================
-- SQL Server / Azure SQL Star Schema DDL for Real Estate Data Platform
-- ====================================================================

-- 1. Date Dimension (Conformed Dimension for Date Lookups)
CREATE TABLE dim_date (
    date_id INT PRIMARY KEY,              -- Integer format: YYYYMMDD (e.g., 20250312)
    full_date DATE NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    day_of_month INT NOT NULL,
    day_of_week INT NOT NULL,             -- 1-7
    day_name VARCHAR(20) NOT NULL,
    quarter INT NOT NULL,
    is_weekend BIT NOT NULL
);

-- 2. Location Dimension (Hierarchical Region Information)
CREATE TABLE dim_location (
    location_id INT IDENTITY(1,1) PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    district VARCHAR(100) NOT NULL,
    -- Add constraint to prevent inserting duplicate city/district pairs
    UNIQUE (city, district)
);

-- 3. Property Dimension (Physical Attributes of the Listing)
CREATE TABLE dim_property (
    property_id INT IDENTITY(1,1) PRIMARY KEY,
    rooms INT,
    building_year INT,
    area_bucket VARCHAR(20) NOT NULL,     -- e.g., '0-40', '40-60', '60-80', '80+'
    -- Note: For attributes like "Has Balcony" or "Property Type" in the future, add them here.
    -- Constraint to prevent duplicate generic combinations
    UNIQUE (rooms, building_year, area_bucket)
);

-- 4. Source Dimension (Where we scraped the data from)
CREATE TABLE dim_source (
    source_id INT IDENTITY(1,1) PRIMARY KEY,
    source_name VARCHAR(50) NOT NULL UNIQUE -- e.g., 'otodom', 'olx', 'gus'
);


-- ====================================================================
-- FACT TABLE
-- ====================================================================

CREATE TABLE fact_listings (
    listing_sk INT IDENTITY(1,1) PRIMARY KEY,    -- Surrogate Key
    listing_natural_key VARCHAR(100) NOT NULL,   -- Original string ID (e.g., 'otodom_3bf59e')
    
    -- Foreign Keys to Dimensions
    date_id INT NOT NULL REFERENCES dim_date(date_id),
    location_id INT NOT NULL REFERENCES dim_location(location_id),
    property_id INT NOT NULL REFERENCES dim_property(property_id),
    source_id INT NOT NULL REFERENCES dim_source(source_id),
    
    -- Degenerate Dimensions (Useful for auditing without joins)
    date_scraped DATETIME2 NOT NULL,
    
    -- Fact Measures (Additive or Semi-Additive)
    price NUMERIC(15, 2) NOT NULL,
    area NUMERIC(10, 2) NOT NULL,
    price_per_m2 NUMERIC(15, 2) NOT NULL,
    
    -- Metadata limits
    url VARCHAR(500),
    
    -- Constraint to avoid duplicating the same natural representation multiple times
    UNIQUE (listing_natural_key)
);

CREATE TABLE fact_macro_indicators (
    macro_sk INT IDENTITY(1,1) PRIMARY KEY,
    indicator_natural_key VARCHAR(100) NOT NULL,

    date_id INT NOT NULL REFERENCES dim_date(date_id),
    location_id INT NOT NULL REFERENCES dim_location(location_id),
    source_id INT NOT NULL REFERENCES dim_source(source_id),

    date_recorded DATETIME2 NOT NULL,
    average_price_per_m2 NUMERIC(15, 2) NOT NULL,

    UNIQUE (indicator_natural_key)
);

-- Create indices on Fact table Foreign Keys to speed up BI aggregations
CREATE INDEX idx_fact_date ON fact_listings(date_id);
CREATE INDEX idx_fact_location ON fact_listings(location_id);
CREATE INDEX idx_fact_property ON fact_listings(property_id);
CREATE INDEX idx_fact_macro_date ON fact_macro_indicators(date_id);
CREATE INDEX idx_fact_macro_location ON fact_macro_indicators(location_id);
