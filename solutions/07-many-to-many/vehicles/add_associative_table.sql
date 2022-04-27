CREATE TABLE movr_vehicles.vehicles_stations (
    vehicle_id UUID REFERENCES movr_vehicles.vehicles(id),
    station_id UUID REFERENCES movr_vehicles.stations(id),
    docked_ts TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO movr_vehicles.vehicles_stations (vehicle_id, station_id, docked_ts) VALUES
    ('d0e896f2-2f5c-4d56-9b26-9d98abc9856e', '83a52f1c-6b35-403d-b415-9cb4876b19a6', '2022-03-22 14:03:44'),
    ('d0e896f2-2f5c-4d56-9b26-9d98abc9856e', '220b7340-20c7-43a3-8edb-237abbceb8bd', '2022-03-23 08:45:12'),
    ('cedd9808-ef8d-4a90-b1c2-2062eed45c5b', '83a52f1c-6b35-403d-b415-9cb4876b19a6', '2022-03-22 15:12:56'),
    ('cedd9808-ef8d-4a90-b1c2-2062eed45c5b', '49a81d1b-f14b-4d19-99f9-f469b07af5db', '2022-03-24 13:23:44'),
    ('5e97256b-a9d2-43e3-95af-5fbe4f79cc3b', '220b7340-20c7-43a3-8edb-237abbceb8bd', '2022-03-24 11:24:46'),
    ('739b9530-7b25-4c98-91a7-184ace7642a9', '220b7340-20c7-43a3-8edb-237abbceb8bd', '2022-03-25 09:43:12');