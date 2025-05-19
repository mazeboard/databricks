# MAGIC %pip install delta-spark==3.2.0

import sys
import os

sys.path.append(os.path.abspath('../'))

from pyspark.sql import SparkSession
from delta.tables import DeltaTable

from delta import *

    #.config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
    #.config("spark.executor.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
spark = SparkSession.builder \
    .appName("Workspace Initialization") \
    .config("spark.master", "local[1]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:3.2.0")  \
    .config("spark.driver.memory", "5g") \
    .config("spark.executor.memory", "24g") \
    .getOrCreate()

"""
builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
"""

sc = spark.sparkContext

tenant_id = 'root'
workspace_id = 'root'

# COMMAND ----------

from delta.tables import DeltaTable

arlequinDir = os.getenv('ARLEQUIN_DIR')
table_path = f"{arlequinDir}/delta-tables"

# Normalized Tables
(DeltaTable.createIfNotExists(spark)
    .tableName(f"norm_contents")
    .location(f"{table_path}/norm_contents")
    .addColumn("project_id", dataType = "STRING")
    .addColumn("content_group", dataType = "STRING")
    .addColumn("id", dataType = "STRING")
    .addColumn("original_id", dataType = "STRING")
    .addColumn("line", dataType = "INTEGER")
    .addColumn("content", dataType = "STRING")
    .addColumn("dim_1", dataType = "STRING")
    .addColumn("dim_2", dataType = "STRING")
    .addColumn("dim_3", dataType = "STRING")
    .addColumn("dim_4", dataType = "STRING")
    .addColumn("dim_5", dataType = "STRING")
    .addColumn("dim_6", dataType = "STRING")
    .addColumn("dim_7", dataType = "STRING")
    .addColumn("dim_8", dataType = "STRING")
    .addColumn("dim_9", dataType = "STRING")
    .addColumn("dim_10", dataType = "STRING")
    .addColumn("dim_11", dataType = "STRING")
    .addColumn("dim_12", dataType = "STRING")
    .addColumn("dim_13", dataType = "STRING")
    .addColumn("dim_14", dataType = "STRING")
    .addColumn("dim_15", dataType = "STRING")
    .addColumn("dim_16", dataType = "STRING")
    .addColumn("dim_17", dataType = "STRING")
    .addColumn("dim_18", dataType = "STRING")
    .addColumn("dim_19", dataType = "STRING")
    .addColumn("dim_20", dataType = "STRING")
    .addColumn("dim_21", dataType = "STRING")
    .addColumn("dim_22", dataType = "STRING")
    .addColumn("dim_23", dataType = "STRING")
    .addColumn("dim_24", dataType = "STRING")
    .addColumn("dim_25", dataType = "STRING")
    .partitionedBy("project_id", "content_group", "id")
    .execute())

(DeltaTable.createIfNotExists(spark)
    .tableName(f"norm_content_dims")
    .location(f"{table_path}/norm_content_dims")
    .addColumn("project_id", dataType = "STRING")
    .addColumn("content_group", dataType = "STRING")
    .addColumn("dimension", dataType = "INTEGER")
    .addColumn("id", dataType = "STRING")
    .addColumn("data_1", dataType = "STRING")
    .addColumn("data_2", dataType = "STRING")
    .addColumn("data_3", dataType = "STRING")
    .addColumn("data_4", dataType = "STRING")
    .addColumn("data_5", dataType = "STRING")
    .addColumn("data_6", dataType = "STRING")
    .addColumn("data_7", dataType = "STRING")
    .addColumn("data_8", dataType = "STRING")
    .addColumn("data_9", dataType = "STRING")
    .addColumn("data_10", dataType = "STRING")
    .addColumn("count", dataType = "INTEGER")
    .partitionedBy("project_id", "content_group", "dimension", "id")
    .execute())

# COMMAND ----------

# Computed Tables
(DeltaTable.createIfNotExists(spark)
    .tableName(f"comp_content_clusterings")
    .location(f"{table_path}/comp_content_clusterings")
    .addColumn("project_id", dataType = "STRING")
    .addColumn("content_group", dataType = "STRING")
    .addColumn("content_id", dataType = "STRING")
    .addColumn("content_topic", dataType = "INTEGER")
    .addColumn("v1", dataType = "DOUBLE")
    .addColumn("v2", dataType = "DOUBLE")
    .addColumn("v3", dataType = "DOUBLE")
    .addColumn("v4", dataType = "DOUBLE")
    .addColumn("v5", dataType = "DOUBLE")
    .addColumn("mean_v1", dataType = "DOUBLE")
    .addColumn("mean_v2", dataType = "DOUBLE")
    .addColumn("mean_v3", dataType = "DOUBLE")
    .addColumn("mean_v4", dataType = "DOUBLE")
    .addColumn("mean_v5", dataType = "DOUBLE")
    .addColumn("content_topic_clusters", dataType = "INTEGER")
    .addColumn("content_topic_ancestor", dataType = "INTEGER")
    .addColumn("dist", dataType = "DOUBLE")
    .partitionedBy("project_id", "content_group", "content_id")
    .execute())

(DeltaTable.createIfNotExists(spark)
    .tableName(f"comp_content_topics")
    .location(f"{table_path}/comp_content_topics")
    .addColumn("project_id", dataType = "STRING")
    .addColumn("content_group", dataType = "STRING")
    .addColumn("content_topic", dataType = "INTEGER")
    .addColumn("description", dataType = "STRING")
    .addColumn("medium_description", dataType = "STRING")
    .addColumn("small_description", dataType = "STRING")
    .addColumn("topic_size", dataType = "INTEGER")
    .addColumn("total_size", dataType = "INTEGER")
    .addColumn("content_topic_clusters", dataType = "INTEGER")
    .addColumn("content_topic_ancestor", dataType = "INTEGER")
    .partitionedBy("project_id", "content_group", "content_topic")
    .execute())

(DeltaTable.createIfNotExists(spark)
    .tableName(f"comp_content_terms")
    .location(f"{table_path}/comp_content_terms")
    .addColumn("project_id", dataType = "STRING")
    .addColumn("content_group", dataType = "STRING")
    .addColumn("content_topic", dataType = "INTEGER")
    .addColumn("term", dataType = "STRING")
    .addColumn("score", dataType = "DOUBLE")
    .addColumn("content_topic_clusters", dataType = "INTEGER")
    .partitionedBy("project_id", "content_group", "content_topic", "score")
    .execute())

(DeltaTable.createIfNotExists(spark)
    .tableName(f"comp_content_topic_dims")
    .location(f"{table_path}/comp_content_topic_dims")
    .addColumn("project_id", dataType = "STRING")
    .addColumn("content_group", dataType = "STRING")
    .addColumn("content_topic", dataType = "INTEGER")
    .addColumn("dimension", dataType = "INTEGER")
    .addColumn("dimension_id", dataType = "STRING")
    .addColumn("topic_size", dataType = "INTEGER")
    .addColumn("total_size", dataType = "INTEGER")
    .addColumn("score", dataType = "DOUBLE")
    .addColumn("content_topic_clusters", dataType = "INTEGER")
    .partitionedBy("project_id", "content_group", "content_topic", "dimension_id")
    .execute())

(DeltaTable.createIfNotExists(spark)
    .tableName(f"comp_content_dim_clusterings")
    .location(f"{table_path}/comp_content_dim_clusterings")
    .addColumn("project_id", dataType = "STRING")
    .addColumn("content_group", dataType = "STRING")
    .addColumn("dimension", dataType = "INTEGER")
    .addColumn("dimension_id", dataType = "STRING")
    .addColumn("pc1", dataType = "DOUBLE")
    .addColumn("pc2", dataType = "DOUBLE")
    .addColumn("pc3", dataType = "DOUBLE")
    .addColumn("pc4", dataType = "DOUBLE")
    .addColumn("pc5", dataType = "DOUBLE")
    .addColumn("content_topic_clusters", dataType = "INTEGER")
    .partitionedBy("project_id", "content_group", "dimension", "dimension_id")
    .execute())


(DeltaTable.createIfNotExists(spark)
    .tableName(f"comp_content_dim_content_topics")
    .location(f"{table_path}/comp_content_dim_content_topics")
    .addColumn("project_id", dataType = "STRING")
    .addColumn("content_group", dataType = "STRING")
    .addColumn("content_topic", dataType = "INTEGER")
    .addColumn("dimension", dataType = "INTEGER")
    .addColumn("pc1", dataType = "DOUBLE")
    .addColumn("pc2", dataType = "DOUBLE")
    .addColumn("pc3", dataType = "DOUBLE")
    .addColumn("pc4", dataType = "DOUBLE")
    .addColumn("pc5", dataType = "DOUBLE")
    .addColumn("total_size", dataType = "INTEGER")
    .addColumn("topic_size", dataType = "INTEGER")
    .addColumn("min_score", dataType = "DOUBLE")
    .addColumn("max_score", dataType = "DOUBLE")
    .addColumn("content_topic_clusters", dataType = "INTEGER")
    .partitionedBy("project_id", "content_group", "content_topic", "dimension")
    .execute())

###

(DeltaTable.createIfNotExists(spark)
    .tableName(f"comp_content_embeddings")
    .location(f"{table_path}/comp_content_embeddings")
    .addColumn("project_id", dataType = "STRING")
    .addColumn("content_group", dataType = "STRING")
    .addColumn("content_id", dataType = "STRING")
    .addColumn("dense", dataType = "ARRAY<INTEGER>")
    .partitionedBy("project_id", "content_group", "content_id")
    .execute())

(DeltaTable.createIfNotExists(spark)
    .tableName(f"data_project_content_topics")
    .location(f"{table_path}/data_project_content_topics")
    .addColumn("id", dataType = "STRING")
    .addColumn("project_id", dataType = "STRING")
    .addColumn("content_group", dataType = "STRING")
    .addColumn("content_topic", dataType = "INTEGER")
    .addColumn("content_topic_clusters", dataType = "INTEGER")
    .addColumn("content_topic_ancestor", dataType = "INTEGER")
    .addColumn("description", dataType = "STRING")
    .addColumn("topic_size", dataType = "INTEGER")
    .addColumn("total_size", dataType = "INTEGER")
    .execute())

(DeltaTable.createIfNotExists()
    .tableName(f"comp_content_time_series")
    .location(f"{table_path}/comp_content_time_series")
    .addColumn("project_id", dataType = "STRING")
    .addColumn("content_group", dataType = "STRING")
    .addColumn("dimension", dataType = "INTEGER")
    .addColumn("content_topic_clusters", dataType = "INTEGER")
    .addColumn("content_topic", dataType = "INTEGER")
    .addColumn("date", dataType = "STRING")
    .addColumn("forecast_model", dataType = "STRING")
    .addColumn("count", dataType = "INTEGER")
    .addColumn("lower_count", dataType = "INTEGER" )
    .addColumn("upper_count", dataType = "INTEGER" )
    .partitionedBy("project_id", "content_group", "dimension", "date")
    .execute())