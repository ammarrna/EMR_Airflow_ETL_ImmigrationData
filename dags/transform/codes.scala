val state_df = spark.read.format("csv").option("delimiter", ";").option("header", "true").option("inferschema", "true").load("s3://<s3-bucket>/us-cities-demographics.csv")
var newDf = state_df
for(col <- state_df.columns){
    newDf = newDf.withColumnRenamed(col,col.replaceAll("\\s", "_"))
  }
newDf.write.mode("overwrite").parquet("s3://<s3-bucket>/lake/city_demographics_dim/")
val state_dataframe = newDf.select("State_Code","State")
state_dataframe.write.mode("overwrite").parquet("s3://<s3-bucket>/lake/state_dim/")
