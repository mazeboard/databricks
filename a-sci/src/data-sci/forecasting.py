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

h_backward = 7 # Horizon sur lequel on veut tester le modèle vs le réalisé (réel connu)
h_max = 7 # Horizon de prédiction (réel inconnu)
k = 10 # Thème à prédire (prédiction univariée)

# COMMAND ----------

# TODO prepare data

Data = pd.read_excel('Export_ts_daily.xlsx')

data = spark.sql("""SELECT
  norm_content_dims.data_1,
  comp_content_clusterings.content_topic,
  COUNT(norm_contents.id) AS count
FROM norm_content_dims
JOIN norm_contents ON norm_content_dims.id = norm_contents.dim_2
JOIN comp_content_clusterings ON norm_contents.id = comp_content_clusterings.content_id
WHERE norm_contents.project_id = 'project1' AND
      norm_content_dims.project_id = 'project1' AND
      comp_content_clusterings.project_id = 'project1' AND
      dimension = 2 AND
      comp_content_clusterings.content_topic_clusters = 18
GROUP BY content_topic, data_1""")

data.limit(5).show(truncate=False)
display(data.count())

# COMMAND ----------

# On s'assure que la date est bien déclarée

Data['time'] = pd.to_datetime(Data['time'], format='%Y%m%d')

# On déclare la date en index (important pour sktime!)

Data_ts = Data.set_index('time')

# Pour cet exemple, on ne garde que les séries à partir de juin 2024 (trop de zéros avant).
# On doit aussi choisir la classification à prédire

Data_ts = Data_ts[(Data_ts.index>'2024-06-15') & (Data_ts['class']==k)]

# On ne garde que l'index et la série modélisée (important pour sktime!)

Data_ts = Data_ts.drop(['class'], axis=1)

# Déclarer la fréquence des données (important pour sktime!)

Data_ts.index = Data_ts.index.to_period("D")

# COMMAND ----------

plot_series(Data_ts)

# COMMAND ----------

# 1) Définir l'horizon de prédiction

fh = np.arange(1, h_max) 

# 2) Choisir le modèle de forecasting

forecaster = AutoARIMA(sp = 7, suppress_warnings=True) 

# 3) Estimer le modèle sur les données d'entrainement

forecaster.fit(Data_ts, fh=fh)

# 4) Calculer la prévision

y_pred = forecaster.predict(fh=fh) # Point forecasts
y_pred_ints = forecaster.predict_interval(coverage=0.9) # Intervals

# COMMAND ----------

# also requires predictions
y_pred = forecaster.predict()

fig, ax = plotting.plot_series(
    Data_ts, y_pred, 
    labels=["Data", "Forecast"], 
    pred_interval=y_pred_ints
)

ax.legend();

# COMMAND ----------

# avec lissage exponentiel

forecaster = AutoETS(sp=7, auto = False, seasonal = "add")  
forecaster.fit(Data_ts)  
y_pred = forecaster.predict(fh=fh) # Point forecasts
y_pred_ints = forecaster.predict_interval(coverage=0.9) # Intervals

fig, ax = plotting.plot_series(
    Data_ts, y_pred, 
    labels=["Data", "Forecast"],
    pred_interval=y_pred_ints
)

ax.legend();

# COMMAND ----------

# Ici, on va garder une partie de la série sur laquelle on teste le modèle. 
# Le modèle est entrainé sur une fraction de la série (train set) et testé sur le reste (test set)

y_train, y_test = temporal_train_test_split(Data_ts, test_size=h_backward) # On split le jeu de données
plot_series(y_train, y_test, labels=["Train", "Test"]) # On visualise

# COMMAND ----------

fh = ForecastingHorizon(y_test.index, is_relative=False) #Forecasting Horizon

forecaster = AutoETS(sp=7, auto = False, seasonal = "add")  #Model

y_pred = forecaster.fit(y_train, fh=fh).predict() # Forecast

y_pred_ints = forecaster.predict_interval(coverage=0.9) # Intervals

fig, ax = plotting.plot_series(
    y_train, y_test, y_pred, 
    labels=["Data", "Test","Forecast"],
    pred_interval=y_pred_ints
)
ax.legend();

# COMMAND ----------

# Panel Forecasting

# Un des gros intérêts de sktime est de pouvoir entrainer des modèles sur un panel de séries temporelles en un coup.

# Dans notre cas, on va pouvoir entrainer des modèles sur les K thèmes de notre classification (10, 20, 50 ou plus)

Data_panel = Data.copy()
Data_panel = Data_panel[Data_panel.time>'2024-06-15'] # On ne garde que la période sur laquelle on a des valeurs
Data_panel.set_index(['class', 'time'], inplace=True) # Double index
Data_panel # Output

# COMMAND ----------

# entrainer des modèles sur chaque hierarchie

fh = np.arange(1, h_max) 

# Modèle AutoARIMA
forecaster = AutoARIMA(sp = 7, suppress_warnings=True) 

# Prédiction
y_pred = forecaster.fit(Data_panel, fh=fh).predict() #Point forecasts
y_pred_ints = forecaster.predict_interval(coverage=0.9) # Intervals

# COMMAND ----------

# On peut représenter les forecast un-à-un en selectionnant une partie du tableau avec .xs

class_to_plot = 14 # On choisit la catégorie à afficher

fig, ax = plotting.plot_series(
    Data_panel.xs(class_to_plot, level='class'), y_pred.xs(class_to_plot, level='class'), 
    labels=["Data", "Forecast"],
    pred_interval=y_pred_ints.xs(class_to_plot, level='class')
)
ax.legend();

# COMMAND ----------

# Train-Test Split sur du Panel

# Ici, on va entrainer un modèle sur toutes les classes k20 et on va prédire une partie des données observées

y_train, y_test = temporal_train_test_split(Data_panel, test_size=h_backward) #On découpe le jeu entre train et test

fh = np.arange(1, h_backward+1) # Forecasting Horizon

forecaster = AutoETS(error = "add", trend = "add", seasonal = "add", sp=7)   # Modèle

# Prédiction
y_pred = forecaster.fit(y_train, fh=fh).predict() # Point forecasts
y_pred_ints = forecaster.predict_interval(coverage=0.9) # Intervals


# COMMAND ----------

class_to_plot = 4 # On choisit la catégorie à afficher

fig, ax = plotting.plot_series(
    y_train.xs(class_to_plot, level='class'), y_test.xs(class_to_plot, level='class'), y_pred.xs(class_to_plot, level='class'),
    labels=["Data", "Test (réel)", "Forecast"],
    pred_interval=y_pred_ints.xs(class_to_plot, level='class')
)
ax.legend();

# COMMAND ----------

# TODO generate forecasts

# COMMAND ----------


spark.sql(f"""
    DELETE FROM comp_content_time_series
    WHERE project_id='{context["project_id"]}' AND
          content_group='{context["import_id"]}'
""")

topics = reduce(DataFrame.unionAll, [summary for content_cluster, summary in summaries.items()])

topics.writeTo(f"""comp_content_time_series""").using("delta").append()

display(topics)

