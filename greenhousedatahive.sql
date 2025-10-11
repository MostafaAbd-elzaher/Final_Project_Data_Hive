-- 1. ???? ????? ???????? (?? ?? ??????)
IF DB_ID('GreenhouseDB') IS NULL
BEGIN
    CREATE DATABASE GreenhouseDB;
END
GO

USE GreenhouseDB;
GO

-- 2. ???? ??????? (locations)
IF OBJECT_ID('dbo.Locations','U') IS NULL
CREATE TABLE dbo.Locations (
    location_id INT IDENTITY(1,1) PRIMARY KEY,
    location_name NVARCHAR(100) NOT NULL
);
GO

-- 3. ???? ???????? (measurements) — ?????? ??????? ??? sensor readings
IF OBJECT_ID('dbo.Measurements','U') IS NULL
CREATE TABLE dbo.Measurements (
    measurement_id INT IDENTITY(1,1) PRIMARY KEY,
    timestamp DATETIME2 NULL,
    reading_date DATE NULL,
    reading_time TIME NULL,
    season NVARCHAR(20) NULL,
    day_period NVARCHAR(20) NULL,
    daytime NVARCHAR(20) NULL,
    soil_temperature_c FLOAT NULL,
    air_temperature_c FLOAT NULL,
    soil_humidity_percent FLOAT NULL,
    air_humidity_percent FLOAT NULL,
    soil_ph FLOAT NULL,
    soil_salinity_ds_m FLOAT NULL,
    light_intensity_lux FLOAT NULL,
    water_level_percent FLOAT NULL,
    location_id INT NULL, -- FK to Locations
    is_error BIT NULL,
    anomaly_flag NVARCHAR(30) NULL
);
GO

-- 4. FK constraint (??? ????? ???????)
IF NOT EXISTS (
    SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_Measurements_Locations'
)
ALTER TABLE dbo.Measurements
ADD CONSTRAINT FK_Measurements_Locations
FOREIGN KEY (location_id) REFERENCES dbo.Locations(location_id);
GO

-- 5. ???????: ???? ?????? ?????????? (?? ???? ?????? ???? ????)
IF OBJECT_ID('dbo.Anomalies','U') IS NULL
CREATE TABLE dbo.Anomalies (
    anomaly_id INT IDENTITY(1,1) PRIMARY KEY,
    measurement_id INT NOT NULL,
    anomaly_type NVARCHAR(50) NOT NULL,
    detected_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    description NVARCHAR(400) NULL,
    CONSTRAINT FK_Anomalies_Measurements FOREIGN KEY (measurement_id) REFERENCES dbo.Measurements(measurement_id)
);
GO

-- 6. Indexes
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Measurements_timestamp')
CREATE INDEX IX_Measurements_timestamp ON dbo.Measurements (timestamp);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Measurements_location')
CREATE INDEX IX_Measurements_location ON dbo.Measurements (location_id);
GO
