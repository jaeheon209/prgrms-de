## 3 Week Assignment

### 1. Jupyter Notebook으로 SQL 정리 (feat. Colab)
* [assignment_1_sql_summary.ipynb](./assignment_1_sql_summary.ipynb)


### 2. Gross Revenue가 가장 큰 User ID 10개 찾기
* [assignment_2.sql](./assignment_2.sql)
```SQL
SELECT
	usc.userid
	, sum(st.amount) AS Gross_Revenue  
FROM raw_data.user_session_channel AS usc
JOIN raw_data.session_transaction AS st
ON usc.sessionid = st.sessionid
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;
```

### 3. 채널별 월 매출액 테이블 만들기
* [assignment_3.sql](./assignment_3.sql)
```SQL
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
```
