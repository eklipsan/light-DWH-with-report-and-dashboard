drop schema if exists report;
create schema report;

-- creating first report

drop table if exists report.departure_airport_count;
create table report.departure_airport_count (
	departure_airport varchar(100),
	departure_date date,
	amount integer
);

create or replace procedure load_departure_airport_count() as $$
	truncate report.departure_airport_count;

	with names as (
		select city::json ->> 'ru' as city, 
		airport_name::json  ->> 'ru' as airport,
		airport_pk
		from core.dim_airports
	)
	insert into report.departure_airport_count select 
	city || ' - ' || airport,
	date_actual,
	count(*)
	from names join core.fact_flights
	on airport_pk = departure_airport_fk
	join core.dim_date
	on date_pk = actual_departure_date_fk
	group by 1,2
	order by 2,3 desc 
$$ language sql;

call load_departure_airport_count();

select * from report.departure_airport_count;