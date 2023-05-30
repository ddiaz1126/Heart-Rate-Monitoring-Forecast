-- Creating heart_disease (database)

create database heart_disease;

-- Create profile table

create table physical_info
(name varchar(30),
time varchar(30),
sex varchar(30),
birthday date,
height_cm float,
weight_kg float,
vo2Max int,
max_hr int);

insert into physical_info
values
('Diego', '2021-12-09T23:36:30.000', 'Male', '1998-11-26', 175.0, 74.0, 54, 197);

select * from physical_info;

-- Create hr_zones table

create table hr_zones
(lowerLimit int,
higherLimit int,
inZone varchar(30),
zoneIndex int);

insert into hr_zones
values
(98, 118,'PT157S', 1),
(118, 137,'PT1952S', 2),
(137, 157, 'PT55S', 3),
(157, 177, 'PT0S', 4),
(177, 197, 'PT0S', 5);

select * from hr_zones;

-- Create exercise table

create table exercise_details
(starttime time,
stoptime time,
sport varchar(30),
kilo_cal int,
min_hr int,
avg_hr int,
max_hr int);

insert into exercise_details
values
('17:59:49.786','18:35:57.786', 'Running', 347, 92, 129, 140);

select * from exercise_details;

describe exercise_details;

-- Create Heart Rate table
-- df.to_csv('hr_sql.csv', sep=',', index=True). This was the line used on python to convert dataframe into csv. Most time effective method.
-- hr_sql tabel created using Table Data Import Wizard

select * from hr_sql;

describe hr_sql;

ALTER TABLE hr_sql MODIFY dateTime time;


-- Analysis --

select timediff(stoptime, starttime) as total_run_time
from exercise_details;

-- Total run time is 36.08 minutes

-- Classify heart rate values by the hr zone from the hr_zones table
SELECT h.value, z.zoneIndex
FROM hr_sql h
JOIN hr_zones z ON h.value >= z.lowerLimit AND h.value < z.higherLimit;


-- See value count of values in each zone
SELECT z.zoneIndex, COUNT(*)/60 AS value_count_minutes
FROM hr_sql h
JOIN hr_zones z ON h.value >= z.lowerLimit AND h.value < z.higherLimit
GROUP BY z.zoneIndex;





