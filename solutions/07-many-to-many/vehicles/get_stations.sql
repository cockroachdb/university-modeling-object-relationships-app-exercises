SELECT s.name
FROM movr_vehicles.vehicles_stations as vs JOIN movr_vehicles.stations as s 
ON vs.station_id = s.id
WHERE vs.vehicle_id = 'cedd9808-ef8d-4a90-b1c2-2062eed45c5b';