# COMMAND ----------
# Databricks notebook source
# MAGIC %pip install -U sktime

# COMMAND ----------
#dbutils.library.restartPython()

# COMMAND ----------
import sys
import os
sys.path.append(os.path.abspath('../libs'))
sys.path.append(os.path.abspath('../'))
from init import spark
from init import sc
from IPython.display import display

# COMMAND ----------
from context import initialize_context

context = initialize_context()

(context)

# COMMAND ----------
#from workspace import Workspace
from dataproject import DataProject
#workspace = Workspace(context["workspace_id"])
data_project = DataProject(context["project_id"])
configuration = data_project.getConfiguration()

content_clusters = [20, 60, 180, 500]

(content_clusters)

# COMMAND ----------
import pandas as pd
import numpy as np
import traceback
from pyspark.sql.functions import col, lit, to_number
from functools import reduce
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, StringType
from pyspark.ml.functions import predict_batch_udf
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import spark_partition_id, col
from pyspark.sql.types import *
from sktime.utils.plotting import plot_series
from sktime.forecasting.arima import AutoARIMA
from sktime.forecasting.ets import AutoETS
from sktime.utils import plotting
from sktime.forecasting.base import ForecastingHorizon
from sktime.forecasting.trend import STLForecaster
from sktime.split import temporal_train_test_split

# COMMAND ----------
# prepare data

df = spark.sql("""SELECT comp_content_clusterings.content_topic_clusters,
  norm_content_dims.data_1,
  comp_content_clusterings.content_topic,
  COUNT(norm_contents.id) AS count
FROM norm_content_dims
JOIN norm_contents ON norm_content_dims.id = norm_contents.dim_2
JOIN comp_content_clusterings ON norm_contents.id = comp_content_clusterings.content_id
WHERE norm_contents.project_id = 'project1' AND
      norm_content_dims.project_id = 'project1' AND
      comp_content_clusterings.project_id = 'project1' AND
      dimension = 2 
GROUP BY content_topic_clusters, content_topic, data_1""")

df.groupBy("content_topic_clusters").count().show()

data_df = df.select(col("data_1").alias("time"), col("content_topic").alias("class"), "count")

# COMMAND ----------
def forecastAndPlot(data, cluster, class_to_plot):
    print(f"*** forecast cluster:{cluster}")
    data = data.groupby('class').filter(lambda x: len(x) >= 20)
    forecaster = AutoETS(auto = True) #AutoETS(sp = 7, error = "add", trend = None, seasonal = "add")  #Model
    pred = forecaster.fit(data, fh=fh).predict() # Point forecast
    pred_ints = forecaster.predict_interval(coverage=0.9) # Prediction intervals
    fig, ax = plotting.plot_series(
        data.xs(class_to_plot, level='class'), pred.xs(class_to_plot, level='class'), 
        labels=["Data", "Forecast"],
        pred_interval=pred_ints.xs(class_to_plot, level='class')
    )
    ax.legend();

data = data_df.filter(col("content_topic_clusters") == cluster).orderBy("time").toPandas()
data['time'] = pd.to_datetime(data['time'], format="%Y-%m-%d") # On déclare le format date
data = data.set_index('time').groupby('class').resample('D').asfreq() # On génère les dates manquantes
data = data.drop(columns = 'class') # On drop la colonne class qui est maintenant en index
data = data.fillna(0) # On remplit les dates manquantes avec des zéros
forecastAndPlot(data, 18, 3)

# COMMAND ----------
fh = np.arange(1, 10) # Forecasting Horizon, should depend on size of data used to do the forecast

def forecast(data, cluster):
    print(f"*** forecast cluster:{cluster}")
    try:
        spark.sql(f"""
            DELETE FROM comp_content_time_series
            WHERE project_id='{context["project_id"]}' AND
                  content_group='{context["import_id"]}' AND
                  content_topic_clusters={cluster}
        """)
        data = data.groupby('class').filter(lambda x: len(x) >= 20)
        forecaster = AutoETS(initialization_method='heuristic', auto = True) 
                    #AutoETS(sp = 7, error = "add", trend = None, seasonal = "add")  #Model
        pred = forecaster.fit(data, fh=fh).predict() # Point forecast
        pred_ints = forecaster.predict_interval(coverage=0.9) # Prediction intervals
    
        data_spark = spark.createDataFrame(data.reset_index()) \
            .withColumn("lower_count", lit(None)) \
            .withColumn("upper_count", lit(None)) \
            .filter(col("count") != 0.0) # not needed, all this data were added to correctly do the predictions
        
        pred_spark = spark.createDataFrame(pred.reset_index())
        pred_ints_spark = spark.createDataFrame(pred_ints.reset_index()) \
            .withColumnRenamed("('class', '', '')", "class") \
            .withColumnRenamed("('time', '', '')", "time") \
            .withColumnRenamed("('count', 0.9, 'lower')", "lower_count") \
            .withColumnRenamed("('count', 0.9, 'upper')", "upper_count") \
        
        result_df = data_spark.union(pred_spark.join(pred_ints_spark, on=['class', 'time'], how='left'))
        result_df = result_df \
            .withColumn("forecast_model", lit("AutoETS")) \
            .withColumn("project_id", lit(context["project_id"])) \
            .withColumn("content_group", lit(context["import_id"])) \
            .withColumn("dimension", lit(2)) \
            .withColumn("content_topic_clusters", lit(cluster)) \
            .withColumnRenamed("class", "content_topic") \
            .withColumn("content_topic", col("content_topic").cast(IntegerType())) \
            .withColumnRenamed("time", "date") \
            .withColumn("date", col("date").cast(StringType())) \
            .withColumn("count", col("count").cast(IntegerType())) \
            .withColumn("lower_count", col("lower_count").cast(IntegerType())) \
            .withColumn("upper_count", col("upper_count").cast(IntegerType()))
        result_df.writeTo(f"""comp_content_time_series""").using("delta").append()
    except Exception as err:
        print("error", err)
        traceback.print_exc()

clusters = [row['content_topic_clusters'] for row in df.select("content_topic_clusters").distinct().collect()]

for cluster in clusters:
    data = data_df.filter(col("content_topic_clusters") == cluster).orderBy("time").toPandas()
    data['time'] = pd.to_datetime(data['time'], format="%Y-%m-%d") # On déclare le format date
    data = data.set_index('time').groupby('class').resample('D').asfreq() # On génère les dates manquantes
    data = data.drop(columns = 'class') # On drop la colonne class qui est maintenant en index
    data = data.fillna(0) # On remplit les dates manquantes avec des zéros
    forecast(data, cluster)

# COMMAND ----------
spark.sql(f"""SELECT content_topic_clusters,count(*) FROM comp_content_time_series
            WHERE project_id='{context["project_id"]}' AND
                  content_group='{context["import_id"]}'
            GROUP BY content_topic_clusters
            order by content_topic_clusters
        """).show()