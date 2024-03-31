-- 1 a. Dimension table named "dim_user"
CREATE TABLE dim_user AS

SELECT
    u.id AS user_id,
    u.client_id,
    CONCAT(u.first_name, ' ', u.last_name) AS full_name,
    u.email,
    u.DOB,
    u.gender,
    TO_CHAR(u.register_date, 'YYYY-MM-DD') AS register_date,
    CASE
        WHEN fa.id IS NOT NULL THEN 'Facebook'
        WHEN ia.id IS NOT NULL THEN 'Instagram'
        ELSE 'Unknown'
    END AS ads_source,
    EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM u.DOB) AS age
FROM user1.users u
LEFT JOIN social_media.facebook_ads fa ON u.client_id = fa.id
LEFT JOIN social_media.instagram_ads ia ON u.client_id = ia.id;
/* 
 * This query creates the "dim_user" dimension table by enriching user profiles with additional insights from 
 * Facebook and Instagram ads. It combines user data with attributes like full name, email, gender, ads source 
 * affiliation, and age. The resulting table provides valuable information for analyzing user engagement and behavior, 
 * benefiting marketing strategies and targeting efforts. 
 */


-- 1 b. Dimension table named "dim_ads"
CREATE TABLE dim_ads AS
SELECT
    'Facebook' AS ads_source,
    id,
    ads_id,
    device_type,
    device_id,
    timestamp
FROM social_media.facebook_ads

UNION ALL

SELECT
    'Instagram' AS ads_source,
    id,
    ads_id,
    device_type,
    device_id,
    timestamp
FROM social_media.instagram_ads;
/*
 * This query retrieves data from both the "facebook_ads" and "instagram_ads" tables, combining them using the 
 * UNION ALL operation. The resulting dataset includes essential details like ad ID, device type, device ID, and timestamp 
 * for advertisements from both social media platforms. This consolidated data can be used to analyze ad performance, 
 * user interactions, and trends across different devices and platforms, aiding marketing decisions and insights.
 */

-- 2a. "fact_user_performance"
CREATE TABLE fact_user_performance AS
SELECT
    u.id AS user_id,
    u.first_name || ' ' || u.last_name AS user_name,
    MAX(e.timestamp)::date AS last_login,
    MAX(e.timestamp)::date AS last_activity,
    COUNT(DISTINCT e.id) AS total_events,
    COUNT(DISTINCT CASE WHEN e.event_type = 'login' THEN e.id END) AS total_logins,
    COUNT(DISTINCT CASE WHEN e.event_type = 'search' THEN e.id END) AS total_searches,
    COUNT(DISTINCT CASE WHEN e.event_type = 'purchase' THEN e.id END) AS total_purchases,
    SUM(CASE WHEN t.transaction_type = 'purchase' THEN t.amount ELSE 0 END) AS total_purchase_amount,
    AVG(CASE WHEN e.event_type = 'search' THEN (e.event_data->>'total_result')::integer ELSE 0 END) AS avg_search_results
FROM user1.users u
LEFT JOIN event."User Event" e ON u.id = e.user_id
LEFT JOIN user1.user_transactions t ON u.id = t.user_id
GROUP BY u.id;
/*
 * This query generates the "fact_user_performance" table, providing insights into user engagement. It calculates metrics 
 * like last login and activity dates, total events, logins, searches, and purchases. By combining user, event, and 
 * transaction data, it offers valuable information for refining marketing strategies and enhancing user experiences.
 */


-- 2b. fact_ads_performance
CREATE TABLE fact_ads_performance AS
SELECT
    a.ads_id,
    COUNT(DISTINCT a.id) AS total_clicks,
    COUNT(DISTINCT CASE WHEN a.ads_source = 'Facebook' THEN a.id END) AS total_facebook_clicks,
    COUNT(DISTINCT CASE WHEN a.ads_source = 'Instagram' THEN a.id END) AS total_instagram_clicks,
    COUNT(DISTINCT CASE WHEN u.id IS NOT NULL THEN a.id END) AS total_converted,
    SUM(CASE WHEN t.transaction_type = 'purchase' THEN 1 ELSE 0 END) AS total_purchases,
    SUM(CASE WHEN t.transaction_type = 'purchase' THEN t.amount ELSE 0 END) AS total_purchase_amount
FROM (
    SELECT id, ads_id, 'Facebook' AS ads_source FROM social_media.facebook_ads
    UNION ALL
    SELECT id, ads_id, 'Instagram' AS ads_source FROM social_media.instagram_ads
) AS a
LEFT JOIN user1.users AS u ON a.id = u.client_id
LEFT JOIN event."User Event" AS e ON a.ads_id = e.event_data->>'ads_id'
LEFT JOIN user1.user_transactions AS t ON u.id = t.user_id AND t.transaction_type = 'purchase'
GROUP BY a.ads_id;
/* 
 * This query compiles data for the "fact_weekly_ads_performance" datamart table, offering insights into the 
 * weekly performance of Facebook and Instagram ads. The metrics encompass total clicks, categorized as 
 * Facebook and Instagram, total converted users, total purchases, and total purchase amount. The query combines 
 * data from both ads sources, associating them with user information. It calculates the aggregated metrics for 
 * each ads_id, facilitating the evaluation of ad effectiveness over time. The result aids in understanding user 
 * engagement and optimizing ad strategies on a weekly basis.
 */


-- 3a. CREATE TABLE fact_daily_event_performance 
CREATE TABLE fact_daily_event_performance AS
SELECT
    e.timestamp::date AS event_date,
    COUNT(DISTINCT e.id) AS total_events,
    COUNT(DISTINCT CASE WHEN e.event_type = 'login' THEN e.id END) AS total_logins,
    COUNT(DISTINCT CASE WHEN e.event_type = 'logout' THEN e.id END) AS total_logouts,
    COUNT(DISTINCT CASE WHEN e.event_type = 'search' THEN e.id END) AS total_searches,
    COUNT(DISTINCT u.id) AS total_users,
    COUNT(DISTINCT CASE WHEN t.transaction_type = 'purchase' THEN u.id END) AS total_purchasing_users,
    SUM(CASE WHEN t.transaction_type = 'purchase' THEN t.amount ELSE 0 END) AS total_purchase_amount
FROM event."User Event" e
LEFT JOIN user1.users u ON e.user_id = u.id
LEFT JOIN user1.user_transactions t ON u.id = t.user_id
GROUP BY event_date
ORDER BY event_date;

/* The "fact_daily_event_performance" datamart table captures daily event-related insights from the "User Event" database, 
 * allowing detailed analysis of user engagement. It provides a breakdown of events such as logins, logouts, and searches, 
 * along with the total number of unique users involved in these activities. Additionally, it tracks user transactions, 
 * indicating the number of users making purchases and the corresponding total purchase amount each day. 
 * This table facilitates monitoring and understanding user behavior, aiding in data-driven decision-making for optimizing 
 * user interactions and overall performance.*/

-- 3b. CREATE TABLE fact_weekly_ads_performance 
CREATE TABLE fact_weekly_ads_performance AS
SELECT
    DATE_TRUNC('week', a.timestamp) AS week_start,
    a.ads_id,
    COUNT(DISTINCT a.id) AS total_clicks,
    COUNT(DISTINCT CASE WHEN u.id IS NOT NULL THEN a.id END) AS total_converted_users,
    COUNT(DISTINCT CASE WHEN a.ads_source = 'Facebook' THEN a.id END) AS total_facebook_clicks,
    COUNT(DISTINCT CASE WHEN a.ads_source = 'Instagram' THEN a.id END) AS total_instagram_clicks,
    SUM(CASE WHEN t.transaction_type = 'purchase' THEN 1 ELSE 0 END) AS total_purchases,
    SUM(CASE WHEN t.transaction_type = 'purchase' THEN t.amount ELSE 0 END) AS total_purchase_amount
    -- Add more relevant metrics here
FROM (
    SELECT id, ads_id, 'Facebook' AS ads_source, timestamp FROM social_media.facebook_ads
    UNION ALL
    SELECT id, ads_id, 'Instagram' AS ads_source, timestamp FROM social_media.instagram_ads
) AS a
LEFT JOIN user1.users AS u ON a.id = u.client_id
LEFT JOIN user1.user_transactions AS t ON u.id = t.user_id AND t.transaction_type = 'purchase'
GROUP BY week_start, a.ads_id;

/* The "fact_weekly_ads_performance" datamart table is designed to analyze the weekly performance of ads from the 
 * Social Media Marketing DB. It provides insights into the effectiveness of ads on a weekly basis by capturing relevant 
 * metrics such as total clicks, total converted users, total Facebook and Instagram clicks, total purchases, and 
 * total purchase amount. The table aggregates data from both Facebook and Instagram ads, grouping the information 
 * based on the start of each week and the specific ads_id. This allows for a comprehensive assessment of ad performance 
 * over time, aiding in strategic decision-making and optimization efforts.*/