# Databricks notebook source
#dbutils.library.restartPython()

# COMMAND ----------

import sys
import os
sys.path.append(os.path.abspath('../libs'))
sys.path.append(os.path.abspath('../'))
from init import spark
from IPython.display import display

# COMMAND ----------

from context import initialize_context

context = initialize_context()

(context)

# COMMAND ----------

from workspace import Workspace
from dataproject import DataProject
workspace = Workspace(context["workspace_id"])
data_project = DataProject(context["project_id"])
configuration = data_project.getConfiguration()

configuration

# COMMAND ----------

from pyspark.sql.functions import expr

content_topics_list = spark.sql(f"""
    SELECT uuid() as id, project_id, content_group, content_topic, description, medium_description, small_description, topic_size, total_size
    FROM  comp_content_topics
    WHERE project_id='{context["project_id"]}' AND content_group='{context["import_id"]}'
""").collect()

data_project.export_content_topics(content_topics_list, context["import_id"])

display(content_topics_list)

# COMMAND ----------

workspace.progress(context['job_id'])
