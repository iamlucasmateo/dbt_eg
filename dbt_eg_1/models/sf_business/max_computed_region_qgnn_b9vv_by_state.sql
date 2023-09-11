{{ config(materialized="view") }}

SELECT state
FROM {{ ref('computed_region_qgnn_b9vv_by_state') }}
WHERE total_computed = (
    SELECT MAX(total_computed) FROM {{ ref('computed_region_qgnn_b9vv_by_state') }}
) 