SELECT location_id,
       datetime(
                   strftime('%Y-%m-%d %H:', time_start) ||
                   CASE
                       WHEN CAST(strftime('%M', time_start) AS INTEGER) BETWEEN 0 AND 14 THEN '00'
                       WHEN CAST(strftime('%M', time_start) AS INTEGER) BETWEEN 15 AND 29 THEN '15'
                       WHEN CAST(strftime('%M', time_start) AS INTEGER) BETWEEN 30 AND 44 THEN '30'
                       ELSE '45'
                       END
       )             AS interval_start,
       SUM(sb_cars_r + sb_cars_t + sb_cars_l + nb_cars_r + nb_cars_t + nb_cars_l + wb_cars_r + wb_cars_t + wb_cars_l +
           eb_cars_r + eb_cars_t + eb_cars_l + sb_truck_r + sb_truck_t + sb_truck_l + nb_truck_r + nb_truck_t +
           nb_truck_l + wb_truck_r + wb_truck_t + wb_truck_l + eb_truck_r + eb_truck_t + eb_truck_l + sb_bus_r +
           sb_bus_t + sb_bus_l + nb_bus_r + nb_bus_t + nb_bus_l + wb_bus_r + wb_bus_t + wb_bus_l + eb_bus_r + eb_bus_t +
           eb_bus_l) AS total_counts
FROM raw_data
GROUP BY location_id,
         datetime(
                     strftime('%Y-%m-%d %H:', time_start) ||
                     CASE
                         WHEN CAST(strftime('%M', time_start) AS INTEGER) BETWEEN 0 AND 14 THEN '00'
                         WHEN CAST(strftime('%M', time_start) AS INTEGER) BETWEEN 15 AND 29 THEN '15'
                         WHEN CAST(strftime('%M', time_start) AS INTEGER) BETWEEN 30 AND 44 THEN '30'
                         ELSE '45'
                         END
         )
ORDER BY location_id, interval_start;
