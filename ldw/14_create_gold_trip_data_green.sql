USE nyc_taxi_ldw;
/*
SELECT statement for the Stored Procedure
joining different silver data and get the columns we want
convert DATETIME to DATE exclude time of the day for JOIN
look for weekends and aggregate for card/cash trip counts
*/
/*
SELECT td.year, 
       td.month, 
       tz.borough,
       CONVERT(DATE, td.lpep_pickup_datetime) AS trip_date,
       cal.day_name AS trip_day,       
       CASE WHEN cal.day_name IN ('Saturday', 'Sunday') THEN 'Y' ELSE 'N' END AS is_weekend,
       SUM(CASE WHEN pt.description = 'Credit card' THEN 1 ELSE 0 END) AS card_trip_count,
       SUM(CASE WHEN pt.description = 'Cash' THEN 1 ELSE 0 END) AS cash_trip_count,
       SUM(CASE WHEN tt.trip_type_desc = 'Dispatch' THEN 1 ELSE 0 END) AS dispatch_trip_count,
       SUM(CASE WHEN tt.trip_type_desc = 'Street-hail' THEN 1 ELSE 0 END) AS street_hail_trip_count,
       SUM(td.fare_amount) AS fare_amount,
       SUM(td.trip_distance) AS trip_distance,
       SUM(DATEDIFF(MINUTE, td.lpep_pickup_datetime, td.lpep_dropoff_datetime)) AS trip_duration
    FROM silver.view_trip_data_green td
    JOIN silver.taxi_zone tz 
        ON td.pu_location_id = tz.location_id
    JOIN silver.calendar cal
        ON cal.date = CONVERT(DATE, td.lpep_pickup_datetime)
    JOIN silver.payment_type pt
        ON td.payment_type = pt.payment_type
    JOIN silver.trip_type tt
        ON td.trip_type = tt.trip_type
WHERE td.year = '2021' 
  AND td.month = '01'
GROUP BY td.year, 
         td.month, 
         tz.borough,
         CONVERT(DATE, td.lpep_pickup_datetime),
         cal.day_name
*/

EXEC gold.sp_gold_trip_data_green '2020', '01';
EXEC gold.sp_gold_trip_data_green '2020', '02';
EXEC gold.sp_gold_trip_data_green '2020', '03';
EXEC gold.sp_gold_trip_data_green '2020', '04';
EXEC gold.sp_gold_trip_data_green '2020', '05';
EXEC gold.sp_gold_trip_data_green '2020', '06';
EXEC gold.sp_gold_trip_data_green '2020', '07';
EXEC gold.sp_gold_trip_data_green '2020', '08';
EXEC gold.sp_gold_trip_data_green '2020', '09';
EXEC gold.sp_gold_trip_data_green '2020', '10';
EXEC gold.sp_gold_trip_data_green '2020', '11';
EXEC gold.sp_gold_trip_data_green '2020', '12';
EXEC gold.sp_gold_trip_data_green '2021', '01';
EXEC gold.sp_gold_trip_data_green '2021', '02';
EXEC gold.sp_gold_trip_data_green '2021', '03';
EXEC gold.sp_gold_trip_data_green '2021', '04';
EXEC gold.sp_gold_trip_data_green '2021', '05';
EXEC gold.sp_gold_trip_data_green '2021', '06';