-- =====================================================
-- Economic Indicators Analytical Queries
-- Project: Brazil Economic Data Platform
-- =====================================================

-- 1. View all economic indicators ordered by date

SELECT
    date,
    indicator,
    value
FROM gold_economic_indicators
ORDER BY date;


-- 2. Average value of each indicator

SELECT
    indicator,
    AVG(value) AS avg_value
FROM gold_economic_indicators
GROUP BY indicator;


-- 3. Latest value of each indicator

SELECT
    indicator,
    MAX(date) AS latest_date
FROM gold_economic_indicators
GROUP BY indicator;


-- 4. Time series trend for indicators

SELECT
    date,
    indicator,
    value
FROM gold_economic_indicators
WHERE indicator IN ('IPCA','SELIC','UNEMPLOYMENT')
ORDER BY date;