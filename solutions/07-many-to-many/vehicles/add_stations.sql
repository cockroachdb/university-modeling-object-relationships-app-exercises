CREATE TABLE movr_vehicles.stations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name STRING,
    latitude FLOAT8 NOT NULL,
    longitude FLOAT8 NOT NULL,
    docks INT2 NOT NULL
);

INSERT INTO movr_vehicles.stations (id, name, latitude, longitude, docks)
VALUES
('83a52f1c-6b35-403d-b415-9cb4876b19a6', 'East Park', 40.667668, -73.929062, 10),
('49a81d1b-f14b-4d19-99f9-f469b07af5db', 'Main St', 40.697533, -73.895632, 8),
('220b7340-20c7-43a3-8edb-237abbceb8bd', 'Union Square', 40.678177, -73.944161, 5),
('88075a6d-6ca0-4727-a81f-123e03de0ea3', 'Grand Plaza', 40.682725, -73.975109, 10);
