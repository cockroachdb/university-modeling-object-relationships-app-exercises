SELECT v.id, v.vehicle_type 
FROM 
  movr_vehicles.vehicles as v
  JOIN movr_vehicles.vehicles_stations as vs
    ON v.id = vs.vehicle_id
  JOIN movr_vehicles.stations as s
    ON vs.station_id = s.id
WHERE 
  s.name = 'Union Square';