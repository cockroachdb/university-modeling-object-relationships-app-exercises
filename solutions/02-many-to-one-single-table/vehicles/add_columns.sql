ALTER TABLE movr_vehicles.vehicles ADD COLUMN mileage INT NOT NULL DEFAULT 0;
ALTER TABLE movr_vehicles.vehicles ADD COLUMN maintenance_frequency INT NULL;
ALTER TABLE movr_vehicles.vehicles ADD COLUMN last_maintenance INT NULL;
