-- Alias the field 'email' to itself (as 'email') to prevent the
-- permission logic from showing up directly in the column name results.
CREATE VIEW sales_redacted AS
SELECT
  user_id,
  CASE WHEN
    is_account_group_member('auditors') THEN email
    ELSE 'REDACTED'
  END AS email,
  country,
  product,
  total
FROM sales_raw

--------------
CREATE VIEW sales_redacted AS
SELECT
  user_id,
  country,
  product,
  total
FROM sales_raw
WHERE
  CASE
    WHEN is_account_group_member('managers') THEN TRUE
    ELSE total <= 1000000
  END;

--------------
-- The regexp_extract function takes an email address such as
-- user.x.lastname@example.com and extracts 'example', allowing
-- analysts to query the domain name.

CREATE VIEW sales_redacted AS
SELECT
  user_id,
  region,
  CASE
    WHEN is_account_group_member('auditors') THEN email
    ELSE regexp_extract(email, '^.*@(.*)$', 1)
  END
  FROM sales_raw

--------------
CREATE MATERIALIZED VIEW mv1
AS SELECT
  date, sum(sales) AS sum_of_sales
FROM
  table1
GROUP BY
  date;

REFRESH MATERIALIZED VIEW mv1;

--------------
