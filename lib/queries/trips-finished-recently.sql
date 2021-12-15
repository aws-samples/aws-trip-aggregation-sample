SELECT 
  tripid 
FROM "devax_demos"."trip_aggregation" 
where eventtype = 'trip-finished' 
  and date_diff('minute', date_parse(concat(year, '/', month, '/', day, ' ', hour, ':', minute), '%Y/%m/%d %H:%i'), current_timestamp) < 5