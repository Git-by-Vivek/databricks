# Databricks notebook source
# DBTITLE 1,Setting configuration Parameters
service_account_json="**************"

spark.conf.set("google.cloud.auth.service.account.enable", "true")
spark.conf.set("google.cloud.auth.service.account.json.keyfile", service_account_json)

# COMMAND ----------

# MAGIC %sql create schema if not exists qld

# COMMAND ----------

# MAGIC %sql use qld

# COMMAND ----------

# DBTITLE 1,Reading files from object storage(GCS) to Dataframe
roadcrash = (spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
.load("gs://tmrqld/roadcrash_loc/roadcrashloc.csv"))

vehicle = (spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
.load("gs://tmrqld/vehicle/vehicle_involvement.csv"))

seatbelt = (spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
.load("gs://tmrqld/seatbelt/restraint_helmet_use.csv"))

roadcasualties = (spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
.load("gs://tmrqld/roadcasualties/road_casualties.csv"))

crashfactors = (spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
.load("gs://tmrqld/factors/alcohol_speed_fatigue_defect.csv"))

driverfactors = (spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
.load("gs://tmrqld/driverdemo/driver_involvement.csv"))


# COMMAND ----------

# DBTITLE 1,Saving Dataframes as Bronze layer Delta tables
roadcrash.write.format("delta").mode("overwrite").saveAsTable("roadcrash_bronze")
vehicle.write.format("delta").mode("overwrite").saveAsTable("vehicle_bronze")
seatbelt.write.format("delta").mode("overwrite").saveAsTable("seatbelt_bronze")
roadcasualties.write.format("delta").mode("overwrite").saveAsTable("roadcasualties_bronze")
crashfactors.write.format("delta").mode("overwrite").saveAsTable("crashfactors_bronze")
driverfactors.write.format("delta").mode("overwrite").saveAsTable("driverfactors_bronze")

# COMMAND ----------

# DBTITLE 1,Creating Silver layer Delta table
# MAGIC %sql 
# MAGIC create table roadcrash_silver
# MAGIC comment "Qld road crash location data"
# MAGIC tblproperties ("layer"="silver")
# MAGIC partitioned by (Crash_Year) 
# MAGIC as select 
# MAGIC Crash_Ref_Number
# MAGIC ,Crash_Severity
# MAGIC ,Crash_Year
# MAGIC ,Crash_Month
# MAGIC ,Crash_Day_Of_Week
# MAGIC ,Crash_Hour
# MAGIC ,Crash_Nature
# MAGIC ,Crash_Type
# MAGIC ,Crash_Street
# MAGIC ,State_Road_Name
# MAGIC ,Loc_Suburb
# MAGIC ,Loc_Local_Government_Area
# MAGIC ,Crash_DCA_Group_Description
# MAGIC ,Loc_ABS_Statistical_Area_2
# MAGIC ,Loc_ABS_Statistical_Area_3
# MAGIC ,Loc_ABS_Statistical_Area_4
# MAGIC ,Loc_Post_Code
# MAGIC ,Loc_Police_Division
# MAGIC ,Loc_Police_District
# MAGIC ,Loc_Police_Region
# MAGIC ,Loc_Queensland_Transport_Region
# MAGIC ,Loc_Main_Roads_Region
# MAGIC ,Loc_State_Electorate
# MAGIC ,Loc_Federal_Electorate
# MAGIC ,Crash_Controlling_Authority
# MAGIC ,Crash_Roadway_Feature
# MAGIC ,Crash_Traffic_Control
# MAGIC ,Crash_Speed_Limit
# MAGIC ,Crash_Road_Surface_Condition
# MAGIC ,Crash_Atmospheric_Condition
# MAGIC ,Crash_Lighting_Condition
# MAGIC ,Count_Casualty_Fatality
# MAGIC ,Count_Casualty_Hospitalised
# MAGIC ,Count_Casualty_MedicallyTreated
# MAGIC ,Count_Casualty_MinorInjury
# MAGIC ,Count_Casualty_Total
# MAGIC ,Count_Unit_Car
# MAGIC ,Count_Unit_Motorcycle_Moped
# MAGIC ,Count_Unit_Truck
# MAGIC ,Count_Unit_Bus
# MAGIC ,Count_Unit_Bicycle
# MAGIC ,Count_Unit_Pedestrian
# MAGIC ,Count_Unit_Other from roadcrash_bronze
# MAGIC

# COMMAND ----------

# DBTITLE 1,Creating Silver layer Delta table
# MAGIC %sql 
# MAGIC create table roadcasualties_silver
# MAGIC comment "Qld road crash casualities data"
# MAGIC tblproperties ("layer"="silver")
# MAGIC partitioned by (crash_year) 
# MAGIC select 
# MAGIC crash_year,
# MAGIC crash_police_region,
# MAGIC casualty_severity,
# MAGIC replace(regexp_replace(casualty_agegroup,'[a-z]',''),'  ','-') as casualty_group,
# MAGIC case when casualty_gender='Male' then 'M'
# MAGIC when casualty_gender='Female' then 'F'
# MAGIC end as casulty_gender,
# MAGIC casualty_roadusertype,
# MAGIC casualty_count
# MAGIC from roadcasualties_bronze 

# COMMAND ----------

# DBTITLE 1,Creating Silver layer Delta table
# MAGIC %sql 
# MAGIC create table vehicle_silver
# MAGIC comment "Qld road crash vehicle data"
# MAGIC tblproperties ("layer"="silver")
# MAGIC partitioned by (crash_year) 
# MAGIC select 
# MAGIC crash_year,
# MAGIC crash_police_region,
# MAGIC crash_severity,
# MAGIC case when Involving_motorcycle_moped='No' then 'N'
# MAGIC when Involving_motorcycle_moped='Yes' then 'Y'
# MAGIC end as motorcycle,
# MAGIC case when Involving_truck='No' then 'N'
# MAGIC when Involving_truck='Yes' then 'Y'
# MAGIC end as truck,
# MAGIC case when Involving_bus='No' then 'N'
# MAGIC when Involving_bus='Yes' then 'Y'
# MAGIC end as bus,
# MAGIC count_crashes,
# MAGIC count_casualty_fatality
# MAGIC from vehicle_bronze 

# COMMAND ----------

# DBTITLE 1,Creating Silver layer Delta table
# MAGIC %sql 
# MAGIC create table seatbelt_silver
# MAGIC comment "Qld road crash seatbelt data"
# MAGIC tblproperties ("layer"="silver")
# MAGIC partitioned by (crash_year) 
# MAGIC select 
# MAGIC crash_year,
# MAGIC crash_policeregion as crash_police_region,
# MAGIC casualty_severity,
# MAGIC replace(regexp_replace(casualty_agegroup,'[a-z]',''),'  ','-') as casualty_agegroup,
# MAGIC casualty_gender,
# MAGIC casualty_road_user_type,
# MAGIC casualty_restraint_helmet_use,
# MAGIC casualty_count
# MAGIC from seatbelt_bronze 

# COMMAND ----------

# DBTITLE 1,Creating Silver layer Delta table
# MAGIC %sql 
# MAGIC create table crashfactors_silver
# MAGIC comment "Qld road crash factors data"
# MAGIC tblproperties ("layer"="silver")
# MAGIC partitioned by (crash_year) 
# MAGIC select
# MAGIC crash_year,
# MAGIC crash_police_region,
# MAGIC crash_severity,
# MAGIC case when involving_drink_driving='No' then 'N'
# MAGIC when involving_drink_driving='Yes' then 'Y'
# MAGIC end as involving_drink_driving,
# MAGIC case when involving_driver_speed='No' then 'N'
# MAGIC when involving_driver_speed='Yes' then 'Y'
# MAGIC end as involving_driver_speeding,
# MAGIC case when involving_fatigued_driver='No' then 'N'
# MAGIC when involving_fatigued_driver='Yes' then 'Y'
# MAGIC end as involving_fatigued_driver,
# MAGIC case when involving_defective_vehicle='No' then 'N'
# MAGIC when involving_defective_vehicle='Yes' then 'Y'
# MAGIC end as involving_defective_vehicle,
# MAGIC count_crashes,
# MAGIC count_fatality,
# MAGIC count_hospitalised,
# MAGIC count_medically_treated,
# MAGIC count_minor_injury,
# MAGIC count_all_casualties
# MAGIC from crashfactors_bronze

# COMMAND ----------

# DBTITLE 1,Creating Silver layer Delta table
# MAGIC %sql
# MAGIC
# MAGIC create table driverfactors_silver
# MAGIC comment "Qld road crash driver data"
# MAGIC tblproperties ("layer"="silver")
# MAGIC partitioned by (crash_year) 
# MAGIC select
# MAGIC Crash_Year
# MAGIC ,Crash_Police_Region
# MAGIC ,Crash_Severity
# MAGIC ,case when Involving_Male_Driver='No' then 'N'
# MAGIC when Involving_Male_Driver='Yes' then 'Y'
# MAGIC end as Involving_Male_Driver
# MAGIC ,case when Involving_Female_Driver='No' then 'N'
# MAGIC when Involving_Female_Driver='Yes' then 'Y'
# MAGIC end as Involving_Female_Driver
# MAGIC ,case when `Involving_Young_Driver_16-24`='No' then 'N'
# MAGIC when `Involving_Young_Driver_16-24`='Yes' then 'Y'
# MAGIC end as Involving_Young_Driver_16_24
# MAGIC ,case when Involving_Senior_Driver_60plus='No' then 'N'
# MAGIC when Involving_Senior_Driver_60plus='Yes' then 'Y'
# MAGIC end as Involving_Senior_Driver_60plus
# MAGIC ,case when Involving_Provisional_Driver='No' then 'N'
# MAGIC when Involving_Provisional_Driver='Yes' then 'Y'
# MAGIC end as Involving_Provisional_Driver
# MAGIC ,case when Involving_Overseas_Licensed_Driver='No' then 'N'
# MAGIC when Involving_Overseas_Licensed_Driver='Yes' then 'Y'
# MAGIC end as Involving_Overseas_Licensed_Driver
# MAGIC ,case when Involving_Unlicensed_Driver='No' then 'N'
# MAGIC when Involving_Unlicensed_Driver='Yes' then 'Y'
# MAGIC end as Involving_Unlicensed_Driver
# MAGIC ,Count_Crashes
# MAGIC ,Count_Casualty_Fatality
# MAGIC ,Count_Casualty_Hospitalised
# MAGIC ,Count_Casualty_MedicallyTreated
# MAGIC ,Count_Casualty_MinorInjury
# MAGIC ,Count_Casualty_All
# MAGIC from driverfactors_bronze

# COMMAND ----------

# DBTITLE 1,Creating Gold layer Delta table 
# MAGIC %sql 
# MAGIC create table fatality_agegroup_gold
# MAGIC comment "Qld road crash fatality by age group data"
# MAGIC tblproperties ("layer"="gold")
# MAGIC as
# MAGIC select crash_year,
# MAGIC crash_police_region,
# MAGIC collect_list(casualty_group) age_group,
# MAGIC collect_list(casualty_count) age_group_fatality,
# MAGIC fatality_count total_fatality_count
# MAGIC from (
# MAGIC select rdcas.crash_year,
# MAGIC rdcas.crash_police_region,
# MAGIC rdcas.casualty_group ,
# MAGIC sum(casualty_count) casualty_count,
# MAGIC totfat.fatality_count 
# MAGIC from roadcasualties_silver rdcas
# MAGIC inner join 
# MAGIC (select crash_year,
# MAGIC crash_police_region,
# MAGIC sum(casualty_count) as fatality_count
# MAGIC from roadcasualties_silver
# MAGIC where casualty_severity='Fatality'
# MAGIC group by 1,2) totfat
# MAGIC on rdcas.crash_year=totfat.crash_year
# MAGIC and rdcas.crash_police_region=totfat.crash_police_region
# MAGIC where rdcas.casualty_severity='Fatality'
# MAGIC --and rdcas.crash_year=2001
# MAGIC --and rdcas.crash_police_region='Brisbane'
# MAGIC group by 1,2,3,5) age_grp_fatality
# MAGIC group by 1,2,5

# COMMAND ----------

# DBTITLE 1,Creating Gold layer Delta table
# MAGIC %sql 
# MAGIC
# MAGIC create table crash_vehiclestype_gold
# MAGIC comment "Qld road crash involved vehicle types"
# MAGIC tblproperties ("layer"="gold")
# MAGIC as
# MAGIC select crash_year,
# MAGIC crash_police_region
# MAGIC --,crash_severity
# MAGIC ,sum(case when motorcycle='Y' then '1' else 0 end) motorcycle_count
# MAGIC ,sum(case when bus='Y' then '1' else 0 end) bus_count
# MAGIC ,sum(case when truck='Y' then '1' else 0 end) truck_count
# MAGIC ,sum(count_casualty_fatality) fatality_count
# MAGIC ,sum(count_crashes) crashes_count
# MAGIC from vehicle_silver
# MAGIC where --crash_year=2001
# MAGIC --and crash_police_region='Brisbane'
# MAGIC  crash_severity='Fatal'
# MAGIC group by 1,2
# MAGIC having fatality_count >0 

# COMMAND ----------

# DBTITLE 1,Creating Gold layer Delta table
# MAGIC %sql 
# MAGIC
# MAGIC create table crashfatality_ageveh_gold
# MAGIC comment "Qld road crash fatality by age group data"
# MAGIC tblproperties ("layer"="gold")
# MAGIC as
# MAGIC select
# MAGIC veh.crash_year,
# MAGIC veh.crash_police_region,
# MAGIC veh.fatality_count total_fatality_count,
# MAGIC age_fat.age_group,
# MAGIC age_fat.age_group_fatality ,
# MAGIC veh.motorcycle_count total_motorcycles_involved,
# MAGIC veh.bus_count total_buses_involved,
# MAGIC veh.truck_count total_trucks_involved,
# MAGIC veh.crashes_count total_crashes
# MAGIC from
# MAGIC crash_vehiclestype_gold veh
# MAGIC inner join fatality_agegroup_gold age_fat
# MAGIC on veh.crash_year=age_fat.crash_year
# MAGIC and veh.crash_police_region=age_fat.crash_police_region
# MAGIC and veh.fatality_count=age_fat.total_fatality_count

# COMMAND ----------

# DBTITLE 1,Report :- Fatality Count by Age Group for  2022,2021,2020
# MAGIC %sql 
# MAGIC select crash_year,
# MAGIC age_group,
# MAGIC sum(age_group_fatality) as fatality
# MAGIC from (
# MAGIC select crash_year,
# MAGIC explode(arrays_zip(age_group,age_group_fatality)) as crashagg,
# MAGIC crashagg.age_group,
# MAGIC crashagg.age_group_fatality
# MAGIC from crashfatality_ageveh_gold
# MAGIC where crash_year in ('2022','2021','2020'))a
# MAGIC group by 1,2
# MAGIC order by 1 desc
# MAGIC

# COMMAND ----------

# DBTITLE 1,Report :- Crash Count by Crash Year for  2021-2022
# MAGIC %sql 
# MAGIC select 
# MAGIC crash_year,
# MAGIC crash_police_region,
# MAGIC Crash_Severity,
# MAGIC case when involving_male_driver='Y' and involving_female_driver='N' then 'M'
# MAGIC when involving_male_driver='N' and involving_female_driver='Y' then 'F'
# MAGIC when involving_male_driver='Y' and involving_female_driver='Y' then 'Both'
# MAGIC else 'Data Unavailable'
# MAGIC end as driver,
# MAGIC sum(count_crashes) crash_count
# MAGIC from driverfactors_silver
# MAGIC group by 1,2,3,4
# MAGIC order by crash_year desc,driver desc

# COMMAND ----------

# DBTITLE 1,Report :- Casualty Count by Crash Severity for 2021-2022
# MAGIC %sql 
# MAGIC select 
# MAGIC crash_year,
# MAGIC crash_severity,
# MAGIC sum(case when Crash_Severity='Fatal' then count_casualty_fatality
# MAGIC when Crash_Severity='Hospitalisation' then count_casualty_hospitalised
# MAGIC when Crash_Severity='Medical treatment' then count_casualty_medicallytreated
# MAGIC when Crash_Severity='Minor injury' then count_casualty_minorinjury
# MAGIC --when Crash_Severity='Property damage only' then Count_Crashes
# MAGIC end
# MAGIC ) as casualty_count
# MAGIC from driverfactors_silver
# MAGIC group by 1,2
# MAGIC having casualty_count is not null

# COMMAND ----------

# DBTITLE 1,Report :- Crash Factors per Year
# MAGIC %sql select crash_year,
# MAGIC crash_severity,
# MAGIC case when involving_drink_driving='Y' then 'Drink Driving'
# MAGIC when involving_driver_speeding='Y' then 'Driver Speeding'
# MAGIC when involving_fatigued_driver='Y' then 'Fatigued Driver'
# MAGIC when involving_defective_vehicle='Y' then 'Defective Vehicle'
# MAGIC else 'Other Factors'
# MAGIC end crash_factor,
# MAGIC sum(count_crashes) crash_count
# MAGIC from crashfactors_silver
# MAGIC where crash_year in ('2022','2021','2020')
# MAGIC group by 1,2,3
# MAGIC --having crash_factor<>'Other Factors'
# MAGIC order by crash_year desc

# COMMAND ----------


