CREATE TABLE movr_vehicles.bicycles (
    vehicle_id UUID NOT NULL REFERENCES movr_vehicles.vehicles (id),
    is_electric BOOLEAN NOT NULL,
    battery STRING
);

INSERT INTO movr_vehicles.bicycles (vehicle_id, is_electric, battery) 
VALUES 
    ('5e97256b-a9d2-43e3-95af-5fbe4f79cc3b', TRUE, 'LB4523'),
    ('e25cad53-fb7d-46d2-bd0b-0aef9fa79db6', FALSE, NULL);

CREATE TABLE movr_vehicles.scooters (
    vehicle_id UUID NOT NULL REFERENCES movr_vehicles.vehicles (id),
    motor STRING,
    battery STRING
);

INSERT INTO movr_vehicles.scooters (vehicle_id, motor, battery) 
VALUES 
    ('f675d44b-4446-400f-bf91-99b23a281161', 'MMR3023-D', 'LS3029'),
    ('cedd9808-ef8d-4a90-b1c2-2062eed45c5b', 'MMR2021-A', 'LS3032');

CREATE TABLE movr_vehicles.skateboards (
    vehicle_id UUID NOT NULL REFERENCES movr_vehicles.vehicles (id),
    type STRING NOT NULL,
    motor STRING,
    battery STRING
);

INSERT INTO movr_vehicles.skateboards (vehicle_id, type, motor, battery) 
VALUES 
    ('739b9530-7b25-4c98-91a7-184ace7642a9', 'Cruiser', 'MTW2245-S', 'LS1123'),
    ('d0e896f2-2f5c-4d56-9b26-9d98abc9856e', 'Longboard', 'MTW2256-A', 'LS1123');