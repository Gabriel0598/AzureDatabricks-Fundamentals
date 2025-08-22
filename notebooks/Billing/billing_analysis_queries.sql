-- Query to get the total usage quantity for each billing origin product for the current month
SELECT billing_origin_product,
    usage_date,
    sum(usage_quantity) as usage_quantity
FROM system.billing.usage
WHERE month(usage_date) = month(NOW())
    AND year(usage_date) = year(NOW())
GROUP BY billing_origin_product,
    usage_date;

-- Query to calculate the growth rate of usage quantity for each billing origin product
SELECT
after.billing_origin_product,
    before_dbus,
    after_dbus,
    ((after_dbus - before_dbus) / before_dbus * 100) AS growth_rate
FROM (
        SELECT billing_origin_product,
            sum(usage_quantity) as before_dbus
        FROM system.billing.usage
        WHERE usage_date BETWEEN "2024-04-01" and "2024-04-30"
        GROUP BY billing_origin_product
    ) as before
    JOIN (
        SELECT billing_origin_product,
            sum(usage_quantity) as after_dbus
        FROM system.billing.usage
        WHERE usage_date BETWEEN "2024-05-01" and "2024-05-30"
        GROUP BY billing_origin_product
    ) as
after
WHERE before.billing_origin_product =
after.billing_origin_product SORT BY growth_rate DESC;