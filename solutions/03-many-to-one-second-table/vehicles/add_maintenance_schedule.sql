CREATE TABLE movr_vehicles.maintenance_schedule (
    make STRING NOT NULL,
    model STRING NOT NULL,
    maintenance_frequency INT2,
    PRIMARY KEY (make, model)
);

INSERT INTO movr_vehicles.maintenance_schedule (make, model, maintenance_frequency) VALUES
    ('Spitfire', 'Inferno', 300),
    ('Hot Wheelies', 'Citrus', 350),
    ('Dirt Devilz', 'MX-4', 200),
    ('Dirt Devilz', 'MX-6', 400),
    ('Street Slider', 'Motherboard', 200);

SET sql_safe_updates = false;
ALTER TABLE movr_vehicles.vehicles DROP COLUMN maintenance_frequency;
SET sql_safe_updates = true;