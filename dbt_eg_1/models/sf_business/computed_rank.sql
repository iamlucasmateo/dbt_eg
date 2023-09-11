SELECT
    cr.*,
    DENSE_RANK() OVER (ORDER BY cr.total_computed DESC) AS rank
FROM {{ ref('computed_region_qgnn_b9vv_by_state') }} AS cr