-- Databricks notebook source
CREATE OR REFRESH STREAMING LIVE TABLE pax_bronze
AS SELECT current_timestamp() processing_time, input_file_name() source_file, *
FROM cloud_files("/mnt/aws/passengers/", "csv", map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE pax_silver
(CONSTRAINT valid_date EXPECT (month < 4) )
COMMENT "Append only passengers for the first 3 months"
TBLPROPERTIES ("quality" = "silver")
AS SELECT lower(airport) AS airportname, * except(airport)
FROM STREAM(LIVE.pax_bronze)

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE pax_by_month
AS SELECT year,
month,
sum(dom_pax_total) as total_domestic_passengers,
sum(Int_pax_total) as total_International_passengers,
sum(pax_total) as total_passengers
FROM LIVE.pax_silver
GROUP BY year,month
order by year,month

-- COMMAND ----------


