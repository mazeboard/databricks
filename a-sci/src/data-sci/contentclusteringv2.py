# Databricks notebook source
# MAGIC %pip install --upgrade numba
# MAGIC %pip install --upgrade pybind11
# MAGIC #%pip install nmslib
# MAGIC #%pip install 'nmslib @ git+https://github.com/nmslib/nmslib.git#egg=nmslib&subdirectory=python_bindings'
# MAGIC %pip install -U genieclust
# MAGIC %pip install -U FlagEmbedding
# MAGIC %pip install -U pacmap

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

from dataproject import DataProject
data_project = DataProject(context["project_id"])
configuration = data_project.getConfiguration()

content_clusters = [6, 18, 54, 162, 486]

(content_clusters)

# COMMAND ----------

contents = spark.sql(f"""SELECT * FROM norm_contents WHERE project_id='{context["project_id"]}' AND content_group='{context["import_id"]}'""")

display(contents.limit(50))
display(contents.count())

# COMMAND ----------

# MAGIC %pip install peft
# MAGIC from FlagEmbedding import BGEM3FlagModel
# MAGIC
# MAGIC model = BGEM3FlagModel('BAAI/bge-m3') #, use_fp16=True)

# COMMAND ----------

import pandas as pd
import pacmap
from pyspark.sql.functions import col, udf
from pyspark.sql.functions import pandas_udf, PandasUDFType

"""def compute_embeddings():
    content_texts = [str(content['content']) for content in contents.select(
        col('content')
    ).collect()]

    return  pd.DataFrame(
        model.encode(content_texts, return_dense=True)["dense_vecs"]
    )


content_embeddings =  compute_embeddings()
"""

# Assuming `model.encode` is a function that works on a list of strings and returns a list of dense vectors

# Define the UDF using pandas_udf
@pandas_udf("array<float>", PandasUDFType.SCALAR_ITER)
def encode_udf(content_series):
    # Convert Spark DataFrame column to list of strings
    content_texts = content_series.tolist()
    
    # Encode the texts using the model
    dense_vecs = model.encode(content_texts, return_dense=True)["dense_vecs"]
    
    return pd.Series(dense_vecs)

# Assuming you have a Spark session and contents DataFrame
# Apply the UDF to the 'content' column to get the embeddings
content_embeddings_df = contents.withColumn("embedding", encode_udf(col("content")))

# Collect the embeddings into a local DataFrame
content_embeddings = content_embeddings_df.select("embedding").collect()

# COMMAND ----------
import functools
import genieclust
import numpy as np
from pyspark.sql.functions import col, lit, to_number

def compute_content_clustering(input, clusters):
    return genieclust.Genie(
            n_clusters=clusters, cast_float32=True, gini_threshold=0.2,
            affinity="cosinesimil", exact=False, compute_all_cuts=True, compute_full_tree=True
        ).fit_predict(
           input
        )

def compute_content_clustering_hierarchy(clustered_content):
    return pd.DataFrame(
        np.transpose( clustered_content[content_clusters, :]),
        columns=[f'k{content_cluster}' for content_cluster in content_clusters]
    ).drop_duplicates()
    
clustered_content = compute_content_clustering(content_embeddings, content_clusters[-1])
cluster_hierarchy = compute_content_clustering_hierarchy(clustered_content)


display(cluster_hierarchy)

# COMMAND ----------

def compute_content_topics(contents, content_clusters, index, clustered_content):
    content_ids = contents.select(col("id")).toPandas()
    content_ids["content_topic"] = clustered_content[content_clusters[index], :]
    if index == 0:
        content_ids["content_topic_ancestor"] = np.repeat(None, len(content_ids.index))
    else:
        content_ids["content_topic_ancestor"] = clustered_content[content_clusters[index - 1], :]
    return content_ids

content_topics = [[content_cluster, compute_content_topics(contents, content_clusters, index, clustered_content)] for [index, content_cluster] in enumerate(content_clusters)]

spark.sql(f"""
    DELETE FROM comp_content_clusterings
    WHERE project_id='{context["project_id"]}' AND
          content_group='{context["import_id"]}'
""")

for [content_cluster, content_topic] in content_topics:
    final = spark.createDataFrame(content_topic).select(
        lit(context["project_id"]).alias("project_id"),
        lit(context["import_id"]).alias("content_group"),
        col("id").alias("content_id"),
        col("content_topic").cast("INTEGER"),
        col("content_topic_ancestor").cast("INTEGER"),
        lit(content_cluster).alias("content_topic_clusters").cast("INTEGER")
    )
    
    final.writeTo(f"""comp_content_clusterings""").using("delta").append()

    display(final)

# COMMAND ----------

workspace.progress(context['job_id'])
