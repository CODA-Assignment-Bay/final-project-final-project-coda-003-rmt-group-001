CREATE TABLE dim_country (
    country TEXT PRIMARY KEY,
    country_code TEXT,
    region TEXT,
    subregion TEXT,
    latitude NUMERIC,
    longitude NUMERIC,
    country_status TEXT
);

CREATE TABLE fact_education (
    country_year TEXT PRIMARY KEY,
    country TEXT REFERENCES dim_country (country),
    completion_rate NUMERIC,
    dropout_rate NUMERIC,
    unemployment_rate NUMERIC
);

CREATE TABLE fact_GDP (
    country_year TEXT PRIMARY KEY,
	country TEXT REFERENCES dim_country (country),
	year INTEGER,
    gdp NUMERIC
);

CREATE TABLE fact_homicide (
    country_year TEXT PRIMARY KEY,
    country TEXT REFERENCES dim_country (country),
    year INTEGER,
    homicide_number NUMERIC,
    rate_per_100k_population NUMERIC
);

SELECT * FROM dim_country