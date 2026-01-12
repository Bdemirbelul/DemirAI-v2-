CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS raw.user_cars (
    manufacturer TEXT,
    model TEXT,
    year INT,
    mileage INT,
    engine TEXT,
    transmission TEXT,
    drivetrain TEXT,
    fuel_type TEXT,
    mpg NUMERIC,
    exterior_color TEXT,
    interior_color TEXT,
    accidents_or_damage BOOLEAN,
    one_owner BOOLEAN,
    personal_use_only BOOLEAN,
    seller_name TEXT,
    seller_rating NUMERIC,
    driver_rating NUMERIC,
    driver_reviews_num INT,
    price_drop NUMERIC,
    price NUMERIC
);

CREATE TABLE stg.used_cars AS
SELECT
    manufacturer,
    model,
    year,
    mileage,
    engine,
    transmission,
    drivetrain,
    fuel_type,
    mpg,
    exterior_color,
    interior_color,
    accidents_or_damage,
    one_owner,
    personal_use_only,
    seller_name,
    seller_rating,
    driver_rating,
    driver_reviews_num,
    price_drop,
    price,
    CASE
        WHEN transmission ILIKE '%auto%' OR transmission ILIKE '%cvt%' OR transmission ILIKE '%a/t%' THEN 'automatic'
        WHEN transmission ILIKE '%manual%' OR transmission ILIKE '%m/t%' THEN 'manual'
        ELSE 'other'
    END AS transmission_norm,
    CASE
        WHEN fuel_type ILIKE '%gas%' THEN 'gasoline'
        WHEN fuel_type ILIKE '%diesel%' THEN 'diesel'
        WHEN fuel_type ILIKE '%hybrid%' THEN 'hybrid'
        WHEN fuel_type ILIKE '%electric%' THEN 'electric'
        ELSE 'other'
    END AS fuel_type_norm
FROM raw.user_cars;

CREATE TABLE mart.dim_vehicle (
    vehicle_id BIGSERIAL PRIMARY KEY,
    manufacturer TEXT,
    model TEXT,
    year INT,
    engine TEXT,
    transmission TEXT,
    drivetrain TEXT,
    fuel_type TEXT,
    exterior_color TEXT,
    interior_color TEXT,
    transmission_norm TEXT,
    fuel_type_norm TEXT
);

INSERT INTO mart.dim_vehicle (
    manufacturer, model, year, engine, transmission,
    drivetrain, fuel_type, exterior_color, interior_color,
    transmission_norm, fuel_type_norm
)
SELECT DISTINCT
    manufacturer,
    model,
    year,
    engine,
    transmission,
    drivetrain,
    fuel_type,
    exterior_color,
    interior_color,
    transmission_norm,
    fuel_type_norm
FROM stg.used_cars;

CREATE TABLE mart.dim_seller (
    seller_id BIGSERIAL PRIMARY KEY,
    seller_name TEXT,
    seller_rating NUMERIC
);

INSERT INTO mart.dim_seller (seller_name, seller_rating)
SELECT DISTINCT
    seller_name,
    seller_rating
FROM stg.used_cars
WHERE seller_name IS NOT NULL;

CREATE TABLE mart.dim_time (
    time_id BIGSERIAL PRIMARY KEY,
    year INT
);

INSERT INTO mart.dim_time (year)
SELECT DISTINCT year
FROM stg.used_cars
WHERE year IS NOT NULL;

CREATE TABLE mart.fact_listings (
    listing_id BIGSERIAL PRIMARY KEY,
    vehicle_id BIGINT REFERENCES mart.dim_vehicle(vehicle_id),
    seller_id BIGINT REFERENCES mart.dim_seller(seller_id),
    time_id BIGINT REFERENCES mart.dim_time(time_id),
    price NUMERIC,
    price_drop NUMERIC,
    mileage INT,
    mpg NUMERIC,
    driver_rating NUMERIC,
    driver_reviews_num INT,
    accidents_or_damage BOOLEAN,
    one_owner BOOLEAN,
    personal_use_only BOOLEAN
);

INSERT INTO mart.fact_listings (
    vehicle_id,
    seller_id,
    time_id,
    price,
    price_drop,
    mileage,
    mpg,
    driver_rating,
    driver_reviews_num,
    accidents_or_damage,
    one_owner,
    personal_use_only
)
SELECT
    v.vehicle_id,
    s.seller_id,
    t.time_id,
    u.price,
    u.price_drop,
    u.mileage,
    u.mpg,
    u.driver_rating,
    u.driver_reviews_num,
    u.accidents_or_damage,
    u.one_owner,
    u.personal_use_only
FROM stg.used_cars u
JOIN mart.dim_vehicle v
    ON u.manufacturer = v.manufacturer
   AND u.model = v.model
   AND u.year = v.year
   AND u.transmission_norm = v.transmission_norm
   AND u.fuel_type_norm = v.fuel_type_norm
LEFT JOIN mart.dim_seller s
    ON u.seller_name = s.seller_name
LEFT JOIN mart.dim_time t
    ON u.year = t.year;
