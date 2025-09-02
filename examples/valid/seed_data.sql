CREATE TABLE public.tobacco_problem (
  report_id BIGINT PRIMARY KEY,
  _fivetran_synced TIMESTAMPTZ,
  date_submitted VARCHAR(256),
  nonuser_affected VARCHAR(256),
  reported_health_problems VARCHAR(256),
  number_tobacco_products NUMERIC(38,0),
  number_health_problems NUMERIC(38,0),
  reported_product_problems VARCHAR(256),
  _fivetran_deleted BOOLEAN,
  tobacco_products VARCHAR(256),
  number_product_problems NUMERIC(38,0)
);

INSERT INTO public.tobacco_problem (
  report_id, _fivetran_synced, date_submitted, nonuser_affected, reported_health_problems,
  number_tobacco_products, number_health_problems, reported_product_problems,
  _fivetran_deleted, tobacco_products, number_product_problems
) VALUES
  (1, now(), '2025-08-01', 'Yes', 'Headache', 2, 1, 'Packaging issue', false, 'Cigarette', 1),
  (2, now(), '2025-08-02', 'No', 'Nausea', 1, 1, 'Burnt smell', false, 'Vape', 1),
  (3, now(), '2025-08-03', 'Yes', 'Coughing', 3, 2, 'Leaking cartridge', false, 'E-cigarette', 2),
  (4, now(), '2025-08-04', 'No', 'Dizziness', 1, 1, 'Defective filter', false, 'Cigar', 1),
  (5, now(), '2025-08-05', 'Yes', 'Rash', 2, 1, 'Wrong label', true, 'Nicotine pouch', 1);

select * from public.tobacco_problem


INSERT INTO public.tobacco_problem (
  report_id, _fivetran_synced, date_submitted, nonuser_affected, reported_health_problems,
  number_tobacco_products, number_health_problems, reported_product_problems,
  _fivetran_deleted, tobacco_products, number_product_problems
)
SELECT
  gs AS report_id,
  now() AS _fivetran_synced,
  to_char(current_date + gs - 1, 'YYYY-MM-DD') AS date_submitted,
  CASE WHEN gs % 2 = 0 THEN 'No' ELSE 'Yes' END AS nonuser_affected,
  CASE gs
    WHEN 1 THEN 'Headache'
    WHEN 2 THEN 'Nausea'
    WHEN 3 THEN 'Coughing'
    WHEN 4 THEN 'Dizziness'
    ELSE 'Rash'
  END AS reported_health_problems,
  (gs % 3) + 1 AS number_tobacco_products,
  (gs % 2) + 1 AS number_health_problems,
  CASE gs
    WHEN 1 THEN 'Packaging issue'
    WHEN 2 THEN 'Burnt smell'
    WHEN 3 THEN 'Leaking cartridge'
    WHEN 4 THEN 'Defective filter'
    ELSE 'Wrong label'
  END AS reported_product_problems,
  gs % 5 = 0 AS _fivetran_deleted,
  CASE gs
    WHEN 1 THEN 'Cigarette'
    WHEN 2 THEN 'Vape'
    WHEN 3 THEN 'E-cigarette'
    WHEN 4 THEN 'Cigar'
    ELSE 'Nicotine pouch'
  END AS tobacco_products,
  1 AS number_product_problems
FROM generate_series(6,100) AS gs;
