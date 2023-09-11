SELECT state, SUM(computed_region_qgnn_b9vv) AS total_computed
FROM sf_business_v1
WHERE computed_region_qgnn_b9vv IS NOT NULL
GROUP BY state