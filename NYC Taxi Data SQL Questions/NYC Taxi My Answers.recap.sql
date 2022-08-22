USE NYCTaxiData;

/* 
1- Most expensive trip (total amount)
*/

SELECT	TOP 1 *
FROM	[dbo].[nyc_sample_data_for_sql_sep_2015]
ORDER BY Total_amount DESC

/*
2- Most expensive trip per mile (total amount/mile)
*/

SELECT	MAX(Total_amount/Trip_distance) trip_per_mile
FROM	[dbo].[nyc_sample_data_for_sql_sep_2015]
WHERE	Trip_distance != 0 AND Total_amount != 0

/*
3- Most generous trip (highest tip)
*/

SELECT	TOP 1 *
FROM	[dbo].[nyc_sample_data_for_sql_sep_2015]
ORDER BY Tip_amount DESC
;

/*
4- Longest trip duration
*/

SELECT	TOP 1 *, DATEDIFF(MINUTE, lpep_pickup_datetime, Lpep_dropoff_datetime) trip_dur
FROM	[dbo].[nyc_sample_data_for_sql_sep_2015]
ORDER BY trip_dur DESC
;
--
select max(DATEDIFF(ms, lpep_pickup_datetime ,Lpep_dropoff_datetime))
from nyc_sample_data_for_sql_sep_2015

/*
5- Mean tip by hour
*/

SELECT	DATEPART(HOUR, lpep_pickup_datetime) trip_hours, ROUND(AVG(Tip_amount), 1) hourly_mean_tip
FROM	[dbo].[nyc_sample_data_for_sql_sep_2015]
GROUP BY DATEPART(HOUR, lpep_pickup_datetime)
ORDER BY trip_hours
;
--
select distinct datepart(hh,lpep_pickup_datetime), round(avg(Tip_amount) OVER (PARTITION by  datepart(hh,lpep_pickup_datetime)),2,2)
from nyc_sample_data_for_sql_sep_2015
order by 1

/*
6- Median trip cost (This question is optional. You can search for “median” calculation if you want)
*/

/*
7- Average total trip by day of week 
(Fortunately, we have day of week information. 
Otherwise, we need to create a new date column without hours from date column. 
Then, we need to create "day of week" column, i.e Monday, Tuesday .. or 1, 2 ..,  from that new date column. 
Total trip count should be found for each day, lastly average total trip should be calculated for each day)
*/

WITH T2 AS
(
SELECT	CAST(lpep_pickup_datetime AS DATE) everyday, lpep_pickup_day_of_week, COUNT(trip_id) trips_cnt_dow
FROM	[dbo].[nyc_sample_data_for_sql_sep_2015]
GROUP BY CAST(lpep_pickup_datetime AS DATE), lpep_pickup_day_of_week
)
SELECT lpep_pickup_day_of_week, AVG(trips_cnt_dow) avg_trips
FROM T2
GROUP BY lpep_pickup_day_of_week
;
--
select day_of_week, avg(tot_trip)
from (select distinct cast(lpep_pickup_datetime as date) date_
    ,DATENAME(dw, lpep_pickup_datetime) day_of_week
    ,count(trip_id) over(PARTITION by DATEPART(dd, lpep_pickup_datetime)) tot_trip
from nyc_sample_data_for_sql_sep_2015) cnt
GROUP by day_of_week

/*
8- Count of trips by hour 
(Luckily, we have hour column. Otherwise, a new hour column should be created from date column, then count trips by hour)
*/

SELECT	lpep_pickup_hour, COUNT(trip_id) trip_cnt
FROM	[dbo].[nyc_sample_data_for_sql_sep_2015]
GROUP BY lpep_pickup_hour
ORDER BY lpep_pickup_hour
;
--
select distinct DATEPART(hh, lpep_pickup_datetime),count(trip_id) OVER(PARTITION by datepart(hh,lpep_pickup_datetime))
from nyc_sample_data_for_sql_sep_2015

/*
9- Average passenger count per trip
*/

SELECT	AVG(Passenger_count) 
FROM	[dbo].[nyc_sample_data_for_sql_sep_2015]