SELECT v.ID, v.mileage, v.last_maintenance, ms.maintenance_frequency
FROM movr_vehicles.vehicles AS v JOIN movr_vehicles.maintenance_schedule AS ms ON (v.make = ms.make) AND (v.model = ms.model);