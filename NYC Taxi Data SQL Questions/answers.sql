select top 3 * 
from nyc_sample_data_for_sql_sep_2015

-- Q1
select max(Total_amount) as Mx
from nyc_sample_data_for_sql_sep_2015
--Q2
select max(Total_amount/Trip_distance)
from nyc_sample_data_for_sql_sep_2015
where Trip_distance <> 0
--Q3
select *
from nyc_sample_data_for_sql_sep_2015
where Tip_amount = (select MAX(Tip_amount) as mxtip from nyc_sample_data_for_sql_sep_2015)
--Q4
select max(DATEDIFF(ms,lpep_pickup_datetime ,Lpep_dropoff_datetime))
from nyc_sample_data_for_sql_sep_2015
--Q5
select distinct datepart(hh,lpep_pickup_datetime), round(avg(Tip_amount) OVER (PARTITION by  datepart(hh,lpep_pickup_datetime)),2,2)
from nyc_sample_data_for_sql_sep_2015
order by 1
--Q7
select day_of_week, avg(tot_trip)
from (select distinct cast(lpep_pickup_datetime as date) date_
    ,DATEname(dw, lpep_pickup_datetime) day_of_week
    ,count(trip_id) over(PARTITION by DATEPART(dd, lpep_pickup_datetime)) tot_trip
from nyc_sample_data_for_sql_sep_2015) cnt
GROUP by day_of_week
--Q8
select distinct DATEPART(hh, lpep_pickup_datetime),count(trip_id) OVER(PARTITION by datepart(hh,lpep_pickup_datetime))
from nyc_sample_data_for_sql_sep_2015
--Q9
select avg(Passenger_count)
from nyc_sample_data_for_sql_sep_2015
--Q10
select distinct datepart(hh,lpep_pickup_datetime), AVG(Passenger_count) OVER(PARTITION by datepart(hh,lpep_pickup_datetime))
from nyc_sample_data_for_sql_sep_2015
--Q11
select distinct 
    case when RateCodeID = '1' then 'Standart Rate'
        when RateCodeID = '2' then 'JFK'
        when RateCodeID = '3' then 'Newark'
        when RateCodeID = '4' then 'Nassau or Westchester'
        when RateCodeID = '5' then 'Negoatiated Fare'
        when RateCodeID = '6' then 'Group Ride'
    end as RateCodeID,sum(Passenger_count) OVER(PARTITION by RateCodeID)
from nyc_sample_data_for_sql_sep_2015
where RateCodeID = 2 or RateCodeID = 3
--Q12
select sum(case when Total_amount is null then 1 else 0 end) count_nulls
from nyc_sample_data_for_sql_sep_2015
--
select COUNT(Total_amount)
from nyc_sample_data_for_sql_sep_2015
where Total_amount is null
--Q13
select sum(case when Trip_distance is not null then 1 else 0 end)
from nyc_sample_data_for_sql_sep_2015
--
select count(Trip_distance)
from nyc_sample_data_for_sql_sep_2015
where Trip_distance is not null
--Q14
select sum(case when Ehail_fee is null then 1 else 0 end)
from nyc_sample_data_for_sql_sep_2015
--
select count(Ehail_fee)
from nyc_sample_data_for_sql_sep_2015
where Ehail_fee is null
--
select count(*)
from (select Ehail_fee cnt
from nyc_sample_data_for_sql_sep_2015
where Ehail_fee is null
) dfs
--Q15
select *
from nyc_sample_data_for_sql_sep_2015
where Trip_distance <= 0.1 or Trip_distance >= 15
--
select *
from nyc_sample_data_for_sql_sep_2015
where Trip_distance <= 0.1
UNION ALL
select *
from nyc_sample_data_for_sql_sep_2015
WHERE Trip_distance >= 15
--
select Payment_Range_in_Dollars, COUNT(Total_amount)
from (select CASE WHEN Total_amount < 5 THEN '0 - 5'
                    WHEN Total_amount >= 5 AND Total_amount < 10 THEN '5 - 10'
                    WHEN Total_amount >= 10 AND Total_amount < 15 THEN '10 - 15'
                    WHEN Total_amount >= 15 AND Total_amount < 20 THEN '15 - 20'
                    WHEN Total_amount >= 20 AND Total_amount < 25 THEN '20 - 25'
                    WHEN Total_amount >= 25 AND Total_amount < 30 THEN '25 - 30'
                    WHEN Total_amount >= 30 AND Total_amount < 35 THEN '30 - 35'
                    WHEN Total_amount >= 35 THEN '35+' END as Payment_Range_in_Dollars,
                    Total_amount
from nyc_sample_data_for_sql_sep_2015) fsa
GROUP by Payment_Range_in_Dollars
order by 1 

with cte as(select CASE WHEN Total_amount < 5 THEN '0 - 5'
                    WHEN Total_amount >= 5 AND Total_amount < 10 THEN '5 - 10'
                    WHEN Total_amount >= 10 AND Total_amount < 15 THEN '10 - 15'
                    WHEN Total_amount >= 15 AND Total_amount < 20 THEN '15 - 20'
                    WHEN Total_amount >= 20 AND Total_amount < 25 THEN '20 - 25'
                    WHEN Total_amount >= 25 AND Total_amount < 30 THEN '25 - 30'
                    WHEN Total_amount >= 30 AND Total_amount < 35 THEN '30 - 35'
                    WHEN Total_amount >= 35 THEN '35+' END as Payment_Range_in_Dollars,
                    Total_amount
from nyc_sample_data_for_sql_sep_2015
) 
select Payment_Range_in_Dollars, sum(Total_amount) 
from cte
GROUP by Payment_Range_in_Dollars
order by 1
--Q17
select driver_id, Payment_Range_in_Dollars, count(*)
from (select CASE WHEN Total_amount < 5 THEN '0 - 5'
                    WHEN Total_amount >= 5 AND Total_amount < 10 THEN '5 - 10'
                    WHEN Total_amount >= 10 AND Total_amount < 15 THEN '10 - 15'
                    WHEN Total_amount >= 15 AND Total_amount < 20 THEN '15 - 20'
                    WHEN Total_amount >= 20 AND Total_amount < 25 THEN '20 - 25'
                    WHEN Total_amount >= 25 AND Total_amount < 30 THEN '25 - 30'
                    WHEN Total_amount >= 30 AND Total_amount < 35 THEN '30 - 35'
                    WHEN Total_amount >= 35 THEN '35+' END as Payment_Range_in_Dollars,
                    Total_amount , driver_id
from nyc_sample_data_for_sql_sep_2015) aagf
group by driver_id, Payment_Range_in_Dollars
order by 1
--
WITH vte as (select CASE WHEN Total_amount < 5 THEN '0 - 5'
                    WHEN Total_amount >= 5 AND Total_amount < 10 THEN '5 - 10'
                    WHEN Total_amount >= 10 AND Total_amount < 15 THEN '10 - 15'
                    WHEN Total_amount >= 15 AND Total_amount < 20 THEN '15 - 20'
                    WHEN Total_amount >= 20 AND Total_amount < 25 THEN '20 - 25'
                    WHEN Total_amount >= 25 AND Total_amount < 30 THEN '25 - 30'
                    WHEN Total_amount >= 30 AND Total_amount < 35 THEN '30 - 35'
                    WHEN Total_amount >= 35 THEN '35+' END as Payment_Range_in_Dollars,
                    Total_amount , driver_id
from nyc_sample_data_for_sql_sep_2015
)
select driver_id, Payment_Range_in_Dollars, COUNT(Total_amount)
from vte
GROUP by driver_id, Payment_Range_in_Dollars
order by 1 
--Q18
select *, sum(Total_amount) OVER(PARTITION by driver_id )
from (select driver_id ,total_amount, RANK() OVER(PARTITION by driver_id order by Total_amount desc) rnk
from nyc_sample_data_for_sql_sep_2015) aaf
where rnk <=3
--Q19
select *, sum(Total_amount) OVER(PARTITION by driver_id )
from (select trip_id,driver_id ,total_amount, RANK() OVER(PARTITION by driver_id order by Total_amount asc) rnk
from nyc_sample_data_for_sql_sep_2015) aaf
where rnk <=3
--Q20
select distinct top 10 Total_amount
from nyc_sample_data_for_sql_sep_2015
where driver_id = 1
order by Total_amount asc
--
select *
from(select Total_amount,driver_id, RANK() OVER(PARTITION by driver_id order by total_amount) rnk
from nyc_sample_data_for_sql_sep_2015) ffs 
where rnk <= 10 and driver_id = 1
--
select *
from(select Total_amount,driver_id, dense_RANK() OVER(PARTITION by driver_id order by total_amount) rnk
from nyc_sample_data_for_sql_sep_2015) ffs 
where rnk <= 10 and driver_id = 1
--Q21
SELECT lpep_pickup_datetime, Total_amount, Passenger_count, 
        sum(Total_amount) OVER(PARTITION by driver_id order by lpep_pickup_datetime) cumulative_sum
from nyc_sample_data_for_sql_sep_2015
where driver_id = 1
--Q22
select lpep_pickup_datetime, Total_amount, Passenger_count
from nyc_sample_data_for_sql_sep_2015
where
Total_amount = (select max(Total_amount) from nyc_sample_data_for_sql_sep_2015 where driver_id = 1) or 
Total_amount = (select min(Total_amount) from nyc_sample_data_for_sql_sep_2015 where driver_id = 1)
--
select lpep_pickup_datetime, Total_amount, Passenger_count
from (select top 1 *
    from nyc_sample_data_for_sql_sep_2015
    where driver_id = 1
    ORDER by Total_amount) akk
UNION ALL
SELECT lpep_pickup_datetime, Total_amount, Passenger_count
from (select top 1 *
    from nyc_sample_data_for_sql_sep_2015
    where driver_id = 1 
    order by Total_amount desc
) dfd 
--
select *
from (
    select total_amount, 
            RANK() over(partition by driver_id order by total_amount asc) rnk,
            RANK() OVER(partition by driver_id order by total_amount desc) inverse_rnk
    from nyc_sample_data_for_sql_sep_2015
    where driver_id = 1
) dfds
where rnk = 1 or inverse_rnk = 1
--Q23
SELECT lpep_pickup_datetime, driver_id,Total_amount,Passenger_count
from nyc_sample_data_for_sql_sep_2015
where driver_id = 1
ORDER by 3, 1
--Q24
select distinct driver_id
from nyc_sample_data_for_sql_oct_2015
where driver_id not in (select driver_id from nyc_sample_data_for_sql_sep_2015)
--
select driver_id
from nyc_sample_data_for_sql_oct_2015
EXCEPT
select driver_id
from nyc_sample_data_for_sql_sep_2015
--
SELECT distinct driver_id
from nyc_sample_data_for_sql_oct_2015 oct
where not exists (select distinct driver_id from nyc_sample_data_for_sql_sep_2015
                where driver_id = oct.driver_id)
--Q25
select oct_rev - sep_rev
from (select (select sum(total_amount) from nyc_sample_data_for_sql_sep_2015) sep_rev,
        (select sum(total_amount) from nyc_sample_data_for_sql_oct_2015) oct_rev) opo 

/* YANLIŞ ÇÖZÜM!!!
select sum(sep.Total_amount), sum(oct.Total_amount)
from nyc_sample_data_for_sql_sep_2015 sep, nyc_sample_data_for_sql_oct_2015 oct
where sep.driver_id = oct.driver_id*/

select distinct sum(sep.Total_amount) OVER(PARTITION by month(sep.lpep_pickup_datetime)), 
        sum(oct.Total_amount) over(PARTITION by month(oct.lpep_pickup_datetime))
from nyc_sample_data_for_sql_oct_2015 oct
JOIN nyc_sample_data_for_sql_sep_2015 sep on sep.trip_id = oct.trip_id

with cte as (
select lpep_pickup_datetime, Total_amount sep_r
from nyc_sample_data_for_sql_sep_2015
UNION ALL
select lpep_pickup_datetime, Total_amount 
from nyc_sample_data_for_sql_oct_2015 
) 
select distinct sum(sep_r) over(PARTITION by month(lpep_pickup_datetime))
from cte
