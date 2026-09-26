-- Returns any customer records with overlapping validity intervals
SELECT
    a.customer_id,
    a.customer_sk AS sk_a,
    b.customer_sk AS sk_b,
    a.valid_from AS a_valid_from,
    a.valid_to AS a_valid_to,
    b.valid_from AS b_valid_from,
    b.valid_to AS b_valid_to
FROM {{ ref('dim_customer') }} a
JOIN {{ ref('dim_customer') }} b
    ON a.customer_id = b.customer_id
   AND a.customer_sk != b.customer_sk
   AND a.valid_from < b.valid_to
   AND a.valid_to > b.valid_from
