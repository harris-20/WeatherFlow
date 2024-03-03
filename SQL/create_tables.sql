-- Table used to store station information
CREATE TABLE `W_STATION` (
  `station_id` integer PRIMARY KEY,
  `lat` float,
  `long` float,
  `city` varchar(255),
  `country_code` varchar(255),
  `enabled` int
);

CREATE TABLE `W_FACT` (
  `record_id` varchar(255),
  `station_id` integer,
  `time_id` varchar(255),
  `parameter_id` varchar(255),
  `temp_id` varchar(255),
  `heat_index_id` varchar(255)
);

CREATE TABLE `W_TIME_DIM` (
  `time_id` varchar(255) PRIMARY KEY,
  `record_datetime` timestamp,
  `record_date` timestamp,
  `record_month` varchar(255),
  `record_year` varchar(255),
  `record_quarter` varchar(255),
  `record_season` varchar(255),
  `record_weekday` varchar(255)
);

CREATE TABLE `W_PARAM_DIM` (
  `parameter_id` varchar(255) PRIMARY KEY,
  `humidity` integer,
  `pressure` integer,
  `wind_speed` float,
  `wind_direction` float,
  `visibility` integer,
  `cloudiness` integer,
  `dew_point` float
);

CREATE TABLE `W_TEMP_DIM` (
  `temp_id` varchar(255) PRIMARY KEY,
  `temp` float,
  `temp_range_min` float,
  `temp_range_max` float,
  `feels_like` float
);

CREATE TABLE `W_HEAT_INDEX_DIM` (
  `heat_index_id` varchar(255) PRIMARY KEY,
  `heat_index` float,
  `heat_index_category` integer,
  `description` text
);
