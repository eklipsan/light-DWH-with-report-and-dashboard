drop schema if exists core;
create schema core;

-- creating core tables
drop table if exists core.fact_flights;
create table core.fact_flights (
	flight_pk integer primary key,
	departure_airport_fk bpchar(3),
	actual_departure_date_fk integer
);

drop table if exists core.dim_date;
create table core.dim_date (
  date_pk INTEGER primary key,
  date_actual DATE,
  year INTEGER,
  quarter INTEGER,
  month INTEGER,
  day_of_month INTEGER,
  day_of_week INTEGER,
  day_of_year INTEGER,
  week_of_year INTEGER,
  is_weekday BOOLEAN,
  is_holiday BOOLEAN
);

drop table if exists core.dim_airports;
create table core.dim_airports (
	airport_pk bpchar(3) primary key,
	city jsonb,
	airport_name jsonb
);


-- creating procedures to load core tables
create or replace procedure load_dim_date(start_date timestamptz default '2016-01-01 00:00'::timestamptz) as 
$$
	truncate core.dim_date;
	INSERT INTO core.dim_date (date_pk, date_actual, year, quarter, month, day_of_month, day_of_week, day_of_year, week_of_year, is_weekday, is_holiday)
	SELECT
	  to_char(date(d),'yyyymmdd')::integer AS date_pk,
	  d AS calendar_date,
	  EXTRACT(YEAR FROM d) AS year,
	  EXTRACT(QUARTER FROM d) AS quarter,
	  EXTRACT(MONTH FROM d) AS month,
	  EXTRACT(DAY FROM d) AS day_of_month,
	  EXTRACT(ISODOW FROM d) AS day_of_week,
	  EXTRACT(DOY FROM d) AS day_of_year,
	  EXTRACT(WEEK FROM d) AS week_of_year,
	  CASE EXTRACT(ISODOW FROM d) WHEN 6 THEN false WHEN 7 THEN false ELSE true END AS is_weekday,
	  CASE WHEN EXTRACT(MONTH FROM d) = 1 AND EXTRACT(DAY FROM d) = 1 THEN true ELSE false END AS is_holiday
	FROM generate_series(start_date::DATE, '2050-12-31'::DATE, '1 day'::INTERVAL) AS g(d);
$$ language sql;


create or replace procedure load_dim_airports() as $$
	truncate core.dim_airports;
	
	insert into core.dim_airports select
	airport_code,
	city,
	airport_name 
	from staging.airports_data;	
$$ language sql;


create or replace procedure load_fact_flights() as $$
	truncate core.fact_flights;

	insert into core.fact_flights select 
		flight_id,
		departure_airport,
		to_char(actual_departure, 'yyyymmdd')::integer as actual_departure_date_fk
		from staging.flights;
$$ language sql;

-- calling procedures

call load_dim_date();
call load_fact_flights();
call load_dim_airports();