val state_df = spark.read.format("csv").option("header", "true").option("inferschema", "true").load("s3://<s3-bucket>/state_codecsv")

state_df.write.mode("overwrite").parquet("s3://<s3-bucket>/lake/state_dim/")

val country_df = spark.read.format("csv").option("header", "true").option("inferschema", "true").load("s3://<s3-bucket>/country_code.csv")

country_df.write.mode("overwrite").parquet("s3://<s3-bucket>/lake/country_dim/")
