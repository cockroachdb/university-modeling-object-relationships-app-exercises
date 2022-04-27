CREATE TABLE movr_vehicles.maintenance_history (
  maintenance_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  vehicle_id UUID NOT NULL,
  maintenance_date DATE NOT NULL DEFAULT current_date(),
  cost DECIMAL
);
 
INSERT INTO movr_vehicles.maintenance_history 
    (vehicle_id, maintenance_date, cost) 
VALUES
    ('03d0a3a4-ae36-4178-819c-0c1b08e59afc', '2022-04-01', 250),
    ('03d0a3a4-ae36-4178-819c-0c1b08e59afc', '2022-02-11', 175),
    ('03d0a3a4-ae36-4178-819c-0c1b08e59afc', '2021-12-12', 200),
    ('648aefea-9fbc-11ec-b909-0242ac120002', '2022-03-26', 225),
    ('648aefea-9fbc-11ec-b909-0242ac120002', '2022-01-02', 245);
