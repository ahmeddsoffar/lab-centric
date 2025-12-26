CREATE TABLE IF NOT EXISTS weather_readings (
    id BIGSERIAL PRIMARY KEY,
    station_id BIGINT,
    sequence_number BIGINT,
    battery_status VARCHAR(10),
    timestamp BIGINT,
    humidity INT,
    temperature INT,
    wind_speed INT
);