# Databricks notebook source
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

from dataproject import DataProject
data_project = DataProject(context["project_id"])
configuration = data_project.getConfiguration()

# COMMAND ----------

content_topics_list = spark.sql(f"""
    SELECT
        uuid() as id, project_id, content_group, content_topic, content_topic_clusters, content_topic_ancestor,
        description, topic_size, total_size
    FROM  comp_content_topics
    WHERE project_id='{context["project_id"]}' AND content_group='{context["import_id"]}'
""")

display(content_topics_list.count())
content_topics_list.show(50,truncate=False)

spark.sql(f"""
    DELETE FROM data_project_content_topics
    WHERE project_id='{context["project_id"]}' AND
          content_group='{context["import_id"]}'
""")

content_topics_list.writeTo("data_project_content_topics").using("delta").append()


