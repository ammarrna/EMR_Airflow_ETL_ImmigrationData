val state_df = spark.read.format("csv").option("header", "true").option("inferschema", "true").load("s3://<s3-bucket>/state_code.csv.csv")

state_df.write.mode("overwrite").parquet("s3://<s3-bucket>/lake/state_dim/")
