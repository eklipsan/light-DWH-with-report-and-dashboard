drop schema if exists staging;
create schema staging;

-- creating table for staging schema

drop table if exists staging.aircrafts_data;
create table staging.aircrafts_data (
	aircraft_code bpchar(3),
	model jsonb,
	range int4
);

drop table if exists staging.airports_data;
create table staging.airports_data (
	airport_code bpchar(3),
	airport_name jsonb,
	city jsonb,
	coordinates point,
	timezone text
);

drop table if exists staging.boarding_passes;
create table staging.boarding_passes (
	ticket_no bpchar(40),
	flight_id int4,
	boarding_no int4,
	seat_no varchar(4)
);

drop table if exists staging.bookings;
create table staging.bookings (
	book_ref bpchar(6),
	book_date timestamptz,
	total_amount numeric(10,2)
);

drop table if exists staging.flights;
create table staging.flights (
	flight_id serial4 not null,
	flight_no bpchar(6) not null,
	scheduled_departure timestamptz not null,
	scheduled_arrival timestamptz not null,
	departure_airport bpchar(3) not null,
	arrival_airport bpchar(3) not null,
	status varchar(20) not null,
	aircraft_code bpchar(3) not null,
	actual_departure timestamptz null,
	actual_arrival timestamptz null
);

drop table if exists staging.seats;
create table staging.seats (
	aircraft_code bpchar(3),
	seat_no varchar(4),
	fare_conditions varchar(10)
);

drop table if exists staging.ticket_flights;
create table staging.ticket_flights (
	ticket_no bpchar(13),
	flight_id int4,
	fare_conditions varchar(10),
	amount numeric(10,2)
);

drop table if exists staging.tickets;
create table staging.tickets (
	ticket_no bpchar(13),
	book_ref bpchar(6),
	passenger_id varchar(20),
	passenger_name text,
	contact_date jsonb
);

-- creating procedure to load data into tables 

drop procedure if exists load_aircrafts_data;
create or replace procedure load_aircrafts_data() as 
$$
	truncate staging.aircrafts_data;
	
	insert into staging.aircrafts_data select 
	* from bookings.aircrafts_data
 $$ language sql;


drop procedure if exists load_airports_data;
create or replace procedure load_airports_data() as 
$$
	truncate staging.airports_data;
	
	insert into staging.airports_data select 
	* from bookings.airports_data
 $$ language sql;


drop procedure if exists load_boarding_passes;
create or replace procedure load_boarding_passes() as 
$$
	truncate staging.boarding_passes;
	
	insert into staging.boarding_passes  select 
	* from bookings.boarding_passes 
 $$ language sql;


drop procedure if exists load_bookings;
create or replace procedure load_bookings() as 
$$
	truncate staging.bookings;
	
	insert into staging.bookings select 
	* from bookings.bookings 
 $$ language sql;


drop procedure if exists load_flights;
create or replace procedure load_flights() as 
$$
	truncate staging.flights ;
	
	insert into staging.flights select 
	* from bookings.flights 
 $$ language sql;


drop procedure if exists load_seats;
create or replace procedure load_seats() as 
$$
	truncate staging.seats;
	
	insert into staging.seats select 
	* from bookings.seats 
 $$ language sql;


drop procedure if exists load_ticket_flights;
create or replace procedure load_ticket_flights() as 
$$
	truncate staging.ticket_flights;
	
	insert into staging.ticket_flights  select 
	* from bookings.ticket_flights 
 $$ language sql;


drop procedure if exists load_tickets;
create or replace procedure load_tickets() as 
$$
	truncate staging.tickets ;
	
	insert into staging.tickets select 
	* from bookings.tickets 
 $$ language sql;


drop procedure if exists full_load_staging;
create or replace procedure full_load_staging() as
$$
	call load_aircrafts_data();
	call load_airports_data();
	call load_boarding_passes();
	call load_bookings();
	call load_flights();
	call load_seats();
	call load_ticket_flights();
 	call load_tickets();	
$$ language sql;

call full_load_staging();