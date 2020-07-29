SELECT
to_char(t2.ts, 'YYYY-MM') as year_month
, count(distinct(t1.userid))
FROM raw_data.user_session_channel as t1
JOIN raw_data.session_timestamp as t2
ON t1.sessionid = t2.sessionid
GROUP BY 1
ORDER BY 1