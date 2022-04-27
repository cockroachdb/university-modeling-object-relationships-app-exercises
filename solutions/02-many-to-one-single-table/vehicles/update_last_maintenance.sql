UPDATE movr_vehicles.vehicles
   SET last_maintenance = maintenance_frequency
 WHERE mileage > maintenance_frequency;
