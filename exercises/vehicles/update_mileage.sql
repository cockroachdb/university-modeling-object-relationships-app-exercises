UPDATE movr_vehicles.vehicles
   SET mileage = 1 + CAST((RANDOM() * CAST(4 * maintenance_frequency AS FLOAT) ) AS INT)
 WHERE mileage = 0;
