SELECT
	usc.userid
	, sum(st.amount) AS Gross_Revenue  
FROM raw_data.user_session_channel AS usc
JOIN raw_data.session_transaction AS st
ON usc.sessionid = st.sessionid
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;
