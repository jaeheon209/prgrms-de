SELECT
	LEFT(sts.ts::text, 7) AS month
	, c.channelname AS channel
	, COUNT(DISTINCT(usc.userid)) AS uniqueUsers
	, COUNT(st.refunded) AS paidUsers
	, CASE
			WHEN paidUsers = 0 THEN 0
			ELSE (paidUsers::numeric/uniqueUsers::numeric) * 100
		END AS conversionRate
	, SUM(st.amount) AS grossRevenue
	, SUM(CASE WHEN NOT st.refunded THEN st.amount END) AS netRevenue
FROM raw_data.channel AS c
LEFT JOIN raw_data.user_session_channel AS usc ON c.channelname = usc.channel
LEFT JOIN raw_data.session_timestamp AS sts ON usc.sessionid = sts.sessionid
LEFT JOIN raw_data.session_transaction AS st ON usc.sessionid = st.sessionid
GROUP BY 1, 2
ORDER BY 1, 2;