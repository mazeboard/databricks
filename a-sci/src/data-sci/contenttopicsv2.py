
# Databricks notebook source
# MAGIC %pip install --upgrade --upgrade-strategy eager "optimum[ipex]"
# MAGIC %pip install torchvision
# MAGIC %pip install --upgrade sentence_transformers
# MAGIC %pip install --upgrade numba
# MAGIC %pip install nmslib-metabrainz
# MAGIC %pip install -U genieclust
# MAGIC %pip install -U pacmap
# MAGIC %pip install llama-cpp-python


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

# COMMAND ----------

#from workspace import Workspace
from dataproject import DataProject
#workspace = Workspace(context["workspace_id"])
data_project = DataProject(context["project_id"])
configuration = data_project.getConfiguration()

content_clusters = [20, 60, 180, 500]

sample_count = 7


# COMMAND ----------

embeddings = spark.sql(f"""SELECT * FROM comp_content_embeddings WHERE project_id='{context["project_id"]}' AND content_group='{context["import_id"]}'""")

display('embeddings', embeddings.limit(50))

# COMMAND ----------

import functools
import genieclust
import numpy as np
import pandas as pd
from pyspark.sql.functions import col, lit, to_number
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

def compute_content_clustering(input, clusters):
    return genieclust.Genie(
            n_clusters=clusters, cast_float32=True, gini_threshold=0.2,
            affinity="l2", exact=False, compute_all_cuts=True, compute_full_tree=True
        ).fit_predict(
           input
        )

def compute_content_clustering_hierarchy(clustered_content):
    return pd.DataFrame(
        np.transpose( clustered_content[content_clusters, :]),
        columns=[f'k{content_cluster}' for content_cluster in content_clusters]
    ).drop_duplicates()
    
clustered_content = compute_content_clustering(embeddings.toPandas()['dense'].tolist(), content_clusters[-1])
cluster_hierarchy = compute_content_clustering_hierarchy(clustered_content)


display('clustered_content',clustered_content)
display('cluster_hierarchy',cluster_hierarchy)

# COMMAND ----------

from functools import reduce
from pyspark.sql import DataFrame

def compute_content_topics(contents, content_clusters, index, clustered_content):
    content_ids = contents.select(col("content_id")).toPandas()
    content_ids["content_topic"] = clustered_content[content_clusters[index], :]
    if index == 0:
        content_ids["content_topic_ancestor"] = np.repeat(None, len(content_ids.index))
    else:
        content_ids["content_topic_ancestor"] = clustered_content[content_clusters[index - 1], :]
    return content_ids

schema = StructType([
    StructField("content_id", StringType(), True),
    StructField("content_topic", IntegerType(), True),
    StructField("content_topic_ancestor", IntegerType(), True)
])

content_topic_layers = [
    spark.createDataFrame(compute_content_topics(embeddings, content_clusters, index, clustered_content), schema).select(
        lit(context["project_id"]).alias("project_id"),
        lit(context["import_id"]).alias("content_group"),
        col("content_id"),
        col("content_topic").cast("INTEGER"),
        col("content_topic_ancestor").cast("INTEGER"),
        lit(content_cluster).alias("content_topic_clusters").cast("INTEGER")
    )
    for [index, content_cluster]
    in enumerate(content_clusters)
]

content_topics = reduce(DataFrame.unionAll, content_topic_layers)

display('content_topics', content_topics)

# COMMAND ----------

#workspace.progress(context['job_id'])

# COMMAND ----------

import random
import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, StringType

hierarchy = content_topics.groupBy(
    'content_topic_clusters', 'content_topic_ancestor', 'content_topic'
).count().orderBy(
    'content_topic_clusters', 'content_topic_ancestor', 'content_topic'
)

total_size = hierarchy.where(f"content_topic_clusters = {content_clusters[0]}").agg(F.sum('count').alias('count')).first()['count']

sample_udf = F.udf(lambda ids:  random.sample(ids, sample_count), ArrayType(StringType()))

def sample_contents(content_cluster):
    sampled_content_ids = content_topics.where(f"""
        project_id='{context["project_id"]}' AND
        content_group='{context["import_id"]}' AND
        content_topic_clusters = {content_cluster}
    """).groupBy(
        "project_id","content_group","content_topic", "content_topic_ancestor"
    ).agg(
        F.collect_list('content_id').alias("content_ids"),
        F.count(F.col('content_id')).alias('count')
    ).select(
        F.col('project_id'),
        F.col('content_group'),
        F.col("content_topic"),
        F.col("content_topic_ancestor"),
        F.col("count").alias("topic_size"),
        sample_udf("content_ids").alias("content_ids")
    ).withColumn("content_id", F.explode("content_ids")).drop('content_ids')

    sampled_contents = sampled_content_ids.join(
        spark.sql(f"""
            SELECT id as content_id, content
            FROM norm_contents
            WHERE project_id='{context["project_id"]}' AND
                  content_group='{context["import_id"]}'
        """),
        'content_id', 'inner'
    )

    return sampled_contents.groupBy(
        "project_id", "content_group", "content_topic", "content_topic_ancestor"
    ).agg(
        F.concat_ws(' . ', F.collect_list("content")).alias('content'),
        F.max("topic_size").alias("topic_size")
    ).select(
        F.col('project_id'),
        F.col('content_group'),
        F.col("content_topic"),
        F.lit(content_cluster).alias("content_topic_clusters"),
        F.col("content_topic_ancestor"),
        F.lit(total_size).alias("total_size").cast('INTEGER'),
        F.col("topic_size").cast('INTEGER'),
        F.col('content')
    ).orderBy('content_topic')
    
sampled_contents = sample_contents(content_clusters[-1])
sampled_contents = sampled_contents.limit(200)

display('sampled_contents', sampled_contents)

# COMMAND ----------

from pyspark.ml.functions import predict_batch_udf
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import spark_partition_id, col

def summarize_fn():
    from llama_cpp import Llama
    llm = Llama.from_pretrained(
        repo_id="failspy/Phi-3-mini-128k-instruct-abliterated-v3-GGUF",
        filename="Phi-3-mini-128k-instruct-abliterated-v3_q4.gguf",
        verbose=False, 
        n_ctx=16384,
        n_gpu_layers=-1,
    )

    def process_content(content):
        prompt = f"""<|system|>You are a helpful assistant.<|end|>
            <|user|>Summarize in french the following list of sentences in a short paragraph. Here is the list of sentence to summarize :{content}<|end|>
            <|assistant|>"""

        output = llm(prompt, max_tokens=2048, stop=["<|endoftext|>"])
        res = output["choices"][0]["text"].strip()
        return res

    def predict(inputs):
        return np.array([process_content(input) for input in inputs])
    
    return predict

summarize_udf = predict_batch_udf(summarize_fn,
    return_type=StringType(),
    batch_size=32)

# COMMAND ----------

from pyspark.sql.types import *

#sc.setCheckpointDir('/tmp/checkpoints')

def process_contents(sampled_contents):
    return sampled_contents.repartition(64).withColumn(
        'description', summarize_udf(F.col("content"))
    ).drop("content") #.checkpoint(True)

summaries = {
    content_clusters[-1]: process_contents(sampled_contents)
}

display('summaries', summaries[content_clusters[-1]])

# COMMAND ----------

def compute_layer(content_clusters, index):
    if index == 0:
        return

    content_cluster = content_clusters[len(content_clusters) - 1 - index]
    content_cluster_ancestor = content_clusters[len(content_clusters) - 1 - index + 1]
    previous_summaries = summaries[content_cluster_ancestor].select(
        F.col("content_topic_ancestor"),
        F.col("description")
    ).alias('previous')
    display(previous_summaries)
    content_cluster_hierarchy = hierarchy.where(f"content_topic_clusters = {content_cluster}").alias('hierarchy')
    display(content_cluster_hierarchy)
    contents = content_cluster_hierarchy.join(
        previous_summaries,
        col('hierarchy.content_topic') == col('previous.content_topic_ancestor'),
        'inner'
    ).groupBy('hierarchy.content_topic_ancestor', "hierarchy.content_topic").agg(
        F.concat_ws(' . ', F.collect_list("description")).alias('content'),
        F.max(F.col('count')).alias('count')
    ).select(
        F.lit(context["project_id"]).alias("project_id"),
        F.lit(context["import_id"]).alias("content_group"),
        F.col("content_topic"),
        F.lit(content_cluster).alias("content_topic_clusters"),
        F.col("content_topic_ancestor"),
        F.lit(total_size).alias("total_size").cast("INTEGER"),
        F.col('count').alias("topic_size").cast('INTEGER'),
        F.col('content'),
    )
    summaries[content_cluster] = process_contents(contents)
    display(summaries[content_cluster])

#[compute_layer(content_cluster, index) for [index, content_cluster] in enumerate(content_clusters)]

topics = reduce(DataFrame.unionAll, [summary for content_cluster, summary in summaries.items()])

display('topics', topics)

spark.sql(f"""
    DELETE FROM comp_content_topics
    WHERE project_id='{context["project_id"]}' AND
          content_group='{context["import_id"]}'
""")

topics.writeTo(f"""comp_content_topics""").using("delta").append()

# COMMAND ----------


def embed_fn():
    from sentence_transformers import SentenceTransformer
    m = SentenceTransformer('BAAI/bge-m3')

    def predict(inputs):
        return m.encode(inputs,batch_size=16,show_progress_bar=True,precision="binary",device="cpu")
    
    return predict


embed_udf = predict_batch_udf(embed_fn,
                              return_type=ArrayType(IntegerType()),
                              batch_size=1024)

topics_embeddings = topics.repartition(64).withColumn("embedding", embed_udf("description")).drop('description') #.checkpoint(True)

# COMMAND ----------

def score_fn():
    from sklearn.metrics.pairwise import cosine_similarity
    import numpy as np

    def predict(a, b):
        return np.array([cosine_similarity([a], [b[index]])[0][0] for index, a in enumerate(a)])
    
    return predict


score_udf = predict_batch_udf(score_fn,
                              return_type=FloatType(),
                              batch_size=1024,
                              input_tensor_shapes=[[128], [128]])

embeddings_topics = embeddings.alias('a').join(
    content_topics.alias('b'), 'content_id', 'inner'
).join(
    topics_embeddings.alias('c'), [
        col("b.content_topic_clusters") == col("c.content_topic_clusters"),
        col("b.content_topic_ancestor").eqNullSafe(col("c.content_topic_ancestor")),
        col("b.content_topic") == col("c.content_topic"),
    ], 'inner'
).withColumn("score", score_udf("dense", "embedding")).orderBy(
    'b.content_topic_clusters', 'b.content_topic_ancestor', 'b.content_topic', 'score'
)

display('embeddings_topics', embeddings_topics)

# COMMAND ----------

# MAGIC %pip install umap-learn

# COMMAND ----------

import umap

def embedding_umap():
    prepare = embeddings.toPandas()['dense']
    result = pd.DataFrame(
        umap.UMAP(
            a=10, b=0.5,n_jobs=12,min_dist=0.001, verbose=True, random_state=42, metric="euclidean"
        ).fit_transform(prepare.tolist())
    )
    return result

def hierarchy_umap():
    prepare = pd.DataFrame(
        np.transpose( clustered_content[content_clusters, :]),
        columns=[f'k{content_cluster}' for content_cluster in content_clusters]
    ) + 1
    result = pd.DataFrame(
        umap.UMAP(
            a=10, b=0.5,n_jobs=12,min_dist=0.001, verbose=True, random_state=42, metric="jaccard"
        ).fit_transform(prepare)
    )
    return result

def content_umap():
    prepare = pd.concat([embedding_umap(), hierarchy_umap()], axis=1)
    result = pd.DataFrame(
        umap.UMAP(
            a=10, b=0.5, min_dist=0.001, verbose=True, random_state=42, metric="euclidean"
        ).fit_transform(prepare)
    )
    result['content_id'] =  embeddings.toPandas()['content_id']
    return result

content_positions = spark.createDataFrame(content_umap())

display('content_positions', content_positions)

# COMMAND ----------

final = embeddings_topics.join(content_positions, "content_id", "inner").select(
    col("a.project_id"),
    col("a.content_group"),
    col("a.content_id"),
    col("b.content_topic_clusters"),
    col("b.content_topic_ancestor"),
    col("b.content_topic"),
    col("0").alias("v1").cast("DOUBLE"),
    col("1").alias("v2").cast("DOUBLE"),
    col("score").alias("dist").cast("DOUBLE")
)

spark.sql(f"""
    DELETE FROM comp_content_clusterings
    WHERE project_id='{context["project_id"]}' AND
          content_group='{context["import_id"]}'
""")

final.writeTo(f"""comp_content_clusterings""").using("delta").append()

display('final', final)
