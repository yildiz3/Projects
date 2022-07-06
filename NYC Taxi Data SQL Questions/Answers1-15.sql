select *
from nyc_sample_data_for_sql_sep_2015
-- Question 1    1. Most expensive trip (total amount).
select max(Total_amount)
from nyc_sample_data_for_sql_sep_2015

select top 1 *
from nyc_sample_data_for_sql_sep_2015
order by Total_amount desc

select *
from nyc_sample_data_for_sql_sep_2015
where Total_amount = (select max(Total_amount) from nyc_sample_data_for_sql_sep_2015)

-- Q2 Most expensive trip per mile (total amount/mile).

select max(Total_amount/ nullif(Trip_distance,0))
from nyc_sample_data_for_sql_sep_2015
--where Trip_distance <> 0

select top 1 *, Total_amount/Trip_distance as a
from nyc_sample_data_for_sql_sep_2015
where Trip_distance <> 0
order by a desc

select *, Total_amount / Trip_distance 
from nyc_sample_data_for_sql_sep_2015
where Trip_distance <> 0 and Total_amount / Trip_distance = (
    select max(Total_amount/Trip_distance)
    from nyc_sample_data_for_sql_sep_2015
    where Trip_distance <> 0 
)

--Q3. Most generous trip (highest tip).

select max(Tip_amount)
from nyc_sample_data_for_sql_sep_2015

select top 1 *
from nyc_sample_data_for_sql_sep_2015
order by Tip_amount desc

select *
from nyc_sample_data_for_sql_sep_2015
where Tip_amount = (select max(Tip_amount) from nyc_sample_data_for_sql_sep_2015)

--Q4 Longest trip duration.

select max(DATEDIFF(ms,lpep_pickup_datetime,Lpep_dropoff_datetime))
from nyc_sample_data_for_sql_sep_2015

select *
from nyc_sample_data_for_sql_sep_2015
where DATEDIFF(ms,lpep_pickup_datetime,Lpep_dropoff_datetime) = (
    select max(DATEDIFF(ms,lpep_pickup_datetime,Lpep_dropoff_datetime))
    from nyc_sample_data_for_sql_sep_2015
)

-- Q5  Mean tip by hour.

select distinct datepart(hh,lpep_pickup_datetime) as hours_, round(avg(Tip_amount) OVER(PARTITION by datepart(hh,lpep_pickup_datetime)),2,2)
from nyc_sample_data_for_sql_sep_2015


/* Q7 . Average total trip by day of week (Fortunately, we have day of week information. Otherwise, we need to 
create a new date column without hours from date column. Then, we need to create "day of week" column, i.e Monday,
 Tuesday .. or 1, 2 ..,  from that new date column. Total trip count should be found for each day, lastly average 
 total trip should be calculated for each day). */

select distinct cast(lpep_pickup_datetime as date) date_,
        DATENAME(dw,lpep_pickup_datetime) day_of_week,
        count(trip_id) over(partition by DATEPART(dd,lpep_pickup_datetime)) tot_trip
from nyc_sample_data_for_sql_sep_2015

select day_of_week, avg(tot_trip)
from (select distinct cast(lpep_pickup_datetime as date) date_,
        DATENAME(dw,lpep_pickup_datetime) day_of_week,
        count(trip_id) over(partition by DATEPART(dd,lpep_pickup_datetime)) tot_trip
from nyc_sample_data_for_sql_sep_2015) asd
GROUP by day_of_week

-- Q8 . Count of trips by hour (Luckily, we have hour column. Otherwise, a new hour column should be created 
-- from date column, then count trips by hour).

select DATEPART(hh,lpep_pickup_datetime), COUNT(*)
from nyc_sample_data_for_sql_sep_2015
GROUP by DATEPART(hh,lpep_pickup_datetime)
order by 1

select distinct DATEPART(hh,lpep_pickup_datetime), count(trip_id) OVER (partition by datepart(hh,lpep_pickup_datetime))
from nyc_sample_data_for_sql_sep_2015

-- Q9 Average passenger count per trip. 

select round(avg(Passenger_count),2,2) as avg_passenger
from nyc_sample_data_for_sql_sep_2015

-- Q10 Average passenger count per trip by hour.

select distinct datepart(hh,lpep_pickup_datetime) picked_hour, avg(Passenger_count) over(PARTITION by datepart(hh,lpep_pickup_datetime))
from nyc_sample_data_for_sql_sep_2015

--Q11 Which airport welcomes more passengers: JFK or EWR? Tip: check RateCodeID from data dictionary for the definition (2: JFK, 3: Newark).

select distinct case when RateCodeID = 2 then 'JFK'
                when RateCodeID = 3 then 'Newark'
                end as RateCodeID, sum(Passenger_count) over(PARTITION by RateCodeID)
from nyc_sample_data_for_sql_sep_2015
where RateCodeID = 2 or RateCodeID = 3

--Q12 How many nulls are there in Total_amount?
select sum(case when Total_amount is null then 1 else 0 end) count_null
from nyc_sample_data_for_sql_sep_2015
--
select COUNT(total_amount)
from nyc_sample_data_for_sql_sep_2015
where Total_amount is null

-- Q13 How many values are there in Trip_distance? (count of non-missing values)
select sum(case when Trip_distance is not null then 1 else 0 end ) count_not_null
from nyc_sample_data_for_sql_sep_2015
--
select COUNT(Trip_distance)
from nyc_sample_data_for_sql_sep_2015
where Trip_distance is not null

--Q14 How many nulls are there in Ehail_fee? 
select sum(case when Ehail_fee is null then 1 else 0 end) count_null 
from nyc_sample_data_for_sql_sep_2015
--
select count(Ehail_fee)
from nyc_sample_data_for_sql_sep_2015
where Ehail_fee is null

/* Q15 Find the trips of which trip distance is greater than 15 miles 
(included) or less than 0.1 mile (included). It is possible to write this
 with only one where statement. However, this time write two queries and 
 "union" them. The purpose of this question is to use union function. 
 You can consider this question as finding outliers in a quick and dirty
  way, which you would do in your professional life too often. */

  select *
  from nyc_sample_data_for_sql_sep_2015
  where Trip_distance <= 0.1 or Trip_distance >= 15

  select *
  from nyc_sample_data_for_sql_sep_2015
  where Trip_distance <= 0.1
  union ALL
  select *
  from nyc_sample_data_for_sql_sep_2015
  where Trip_distance >= 15