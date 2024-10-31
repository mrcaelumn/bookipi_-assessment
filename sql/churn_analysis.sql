WITH weekly_subscriptions AS (
    SELECT 
        subscription._id AS subscription_id,
        companyId AS company_id,
        DATE_TRUNC(startDate, WEEK) AS week,
        status
    FROM `project.dataset.subscriptions` subscription
)

SELECT 
    week,
    COUNTIF(status = 'active') AS new_subscriptions,
    COUNTIF(status = 'cancelled') AS cancelled_subscriptions,
    COUNTIF(status = 'terminated') AS terminated_subscriptions
FROM 
    weekly_subscriptions
GROUP BY 
    week
ORDER BY 
    week DESC;