-- database setup
SET sql_safe_updates = false;
DROP DATABASE IF EXISTS movr_vehicles;
SET sql_safe_updates = true;

CREATE DATABASE movr_vehicles;

-- create the vehicles table
CREATE TABLE movr_vehicles.vehicles (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    vehicle_type STRING NOT NULL,
    purchase_date DATE NOT NULL DEFAULT current_date(),
    serial_number STRING NOT NULL,
    make STRING NOT NULL,
    model STRING NOT NULL,
    year INT2 NOT NULL,
    color STRING NOT NULL,
    description STRING
);

-- add data
INSERT INTO movr_vehicles.vehicles (
    id,
    vehicle_type,
    purchase_date,
    serial_number,
    make,
    model,
    year,
    color,
    description
) VALUES (
    '03d0a3a4-ae36-4178-819c-0c1b08e59afc',
    'Scooter',
    '2022-03-07',
    'SC9757543886484387',
    'Spitfire',
    'Inferno',
    2021,
    'Red',
    'Scratch on the left side'
), (
    '648aefea-9fbc-11ec-b909-0242ac120002',
    'Skateboard',
    current_date(),
    'SB6694627626486622',
    'Street Slider',
    'Motherboard',
    2020,
    'Blue',
    'Alien painted on the bottom'
),(
    'a0dd6bd9-c530-4c23-b401-185c7328a4dd',
    'Bicycle',
    current_date(),
    'BK6522688477384422',
    'Dirt Devilz',
    'MX-4',
    2018,
    'Orange',
    NULL
), (
    'e25cad53-fb7d-46d2-bd0b-0aef9fa79db6',
    'Bicycle',
    current_date(),
    'BK9596625974336633',
    'Dirt Devilz',
    'MX-6',
    2022,
    'Green',
    'White Wall Tires'
), (
    gen_random_uuid(),
    'Scooter',
    current_date(),
    'SC3269997743352394',
    'Hot Wheelies',
    'Citrus',
    2021,
    'Grapefruit',
    NULL
);
