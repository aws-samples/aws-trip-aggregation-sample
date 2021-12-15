select 
  a.tripid, 
  from_iso8601_timestamp(a.eventdate) as startdate,
  from_iso8601_timestamp(b.eventdate) as enddate,
  date_diff('second', from_iso8601_timestamp(a.eventdate), from_iso8601_timestamp(b.eventdate)) as duration,
  (select count(1) from trip_aggregation c where c.tripid = a.tripid group by tripid) as eventCount,
  cast((select count (1) from trip_aggregation c where c.tripid = a.tripid group by tripid) as double) / cast(date_diff('second', from_iso8601_timestamp(a.eventdate), from_iso8601_timestamp(b.eventdate)) as double) as completionRate
from trip_aggregation a
left join trip_aggregation b on a.tripid = b.tripid and b.eventtype = 'trip-finished'
where 
  a.tripid in 
    (SELECT 
      tripid 
    FROM "devax_demos"."trip_aggregation" 
    where eventtype = 'trip-finished' 
      and date_diff('minute', date_parse(concat(year, '/', month, '/', day, ' ', hour, ':', minute), '%Y/%m/%d %H:%i'), current_timestamp) < 5) 
      and a.eventtype = 'engine-start'
order by b.eventdate desc;