SELECT customer, store, DATE(date) as day, pumpNumber, sum(1) as EPSDecline
  FROM `loganalysis-278801.CircleKVantage.mv_logs_basicHealthIndicators`
  WHERE message like '%, Approved: 0, CardNumber: %'
  GROUP BY customer, store, day, pumpNumber
  ORDER BY customer, store, day, pumpNumber

SELECT customer, store, DATE(date) as day, pumpNumber, sum(1) as icroffline  
  FROM `loganalysis-278801.CircleKVantage.mv_logs_basicHealthIndicators`
  WHERE message like '%icr offline event%'
  GROUP BY customer, store, day, pumpNumber
  ORDER BY customer, store, day, pumpNumber

SELECT customer, store, DATE(date) as day, sum(1) as failIcarusLoad --NO PUMPNUMBER
  FROM `loganalysis-278801.CircleKVantage.mv_logs_basicHealthIndicators`
  WHERE message like '%Failed to Load Icarus%'
  GROUP BY customer, store, day
  ORDER BY customer, store, day

SELECT customer, store, DATE(date) as day, pumpNumber, sum(1) as missedHeartbeats  
  FROM `loganalysis-278801.CircleKVantage.mv_logs_basicHealthIndicators`
  WHERE message like '%DC has not received%'
  GROUP BY  customer, store, day, pumpNumber
  ORDER BY  customer, store, day, pumpNumber

SELECT customer, store, DATE(date) as day, pumpNumber, sum(1) as heartbeatsNotSent  
  FROM `loganalysis-278801.CircleKVantage.mv_logs_basicHealthIndicators`
  WHERE message like '%Unable to Send Heartbeat Msg%'
  GROUP BY custoemr, store, day, pumpNumber
  ORDER BY customer, store, day, pumpNumber

SELECT customer, store, DATE(date) as day, pumpNumber, sum(1) as idlePercent
  FROM `loganalysis-278801.CircleKVantage.mv_logs_basicHealthIndicators`
  WHERE message like '%Icarus idle time is at%'
  GROUP BY customer, store, day, pumpNumber
  ORDER BY customer, store, day, pumpNumber

SELECT customer, store, DATE(date) as day, pumpNumber, sum(1) as icrdown
  FROM `loganalysis-278801.CircleKVantage.mv_logs_basicHealthIndicators`
  WHERE message like '%ICR down%'
  GROUP BY customer, store, day, pumpNumber
  ORDER BY customer, store, day, pumpNumber

SELECT customer, store, DATE(date) as day, sum(1) as startingProgram --NO PUMPNUMBER
  FROM `loganalysis-278801.CircleKVantage.mv_logs_basicHealthIndicators`
  WHERE message like '%Started program%'
  GROUP BY customer, store, day
  ORDER BY customer, store, day

SELECT customer, store, DATE(date) as day, pumpNumber, sum(1) as waitonlineevent
  FROM `loganalysis-278801.CircleKVantage.mv_logs_basicHealthIndicators`
  WHERE message like '%Start WaitForSPOTOnlineEvent%'
  GROUP BY customer, store, day, pumpNumber
  ORDER BY customer, store, day, pumpNumber

CREATE MATERIALIZED VIEW `${projectId}.${datasetId}.${materializedViewId}` AS
        SELECT customer, store , date, DATE(date) as day, pumpNumber, message
        FROM `${projectId}.${datasetId}.jag_logs`
        WHERE (message like '%icr offline event%') OR --ICR Offline
        (message like '%EpsilonAuthInfo POSTran#:%') OR --EPS Info
        (message like '%Unable to Send Heartbeat Msg%') OR --Heartbeat not sent
        (message like '%DC has not received%') OR --Heartbeat not received
        (message like '%Icarus idle time is at%') OR -- High CPU Usage
        (message like '%ICR down%') OR --ICR Down
        (message like '%Failed to Load Icarus%') OR --Failed Icarus Load
        (message like '%Started program%') OR --Starting Executables (Icarus/DevMan/PCM)
        (message like '%Start WaitForSPOTOnlineEvent%') -- Connected to ICR, no Online signal
        GROUP BY  customer, date, store, day, pumpNumber, message