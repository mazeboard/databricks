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

content_clusters = [20, 60, 180, 500]

# COMMAND ----------

contents = spark.sql(f"""
    SELECT
        norm_contents.project_id, norm_contents.content_group,
        norm_contents.content,
        comp_content_clusterings.content_topic_clusters, comp_content_clusterings.content_topic
    FROM norm_contents
    JOIN comp_content_clusterings ON
         comp_content_clusterings.project_id = norm_contents.project_id AND
         comp_content_clusterings.content_group = norm_contents.content_group AND
         comp_content_clusterings.content_id = norm_contents.id 
    WHERE norm_contents.project_id='{context["project_id"]}' AND
          norm_contents.content_group='{context["import_id"]}'
""").limit(100)

display(contents)

# COMMAND ----------

from pyspark.sql.functions import collect_list

contents_per_topics = contents.groupBy(
    ['project_id', 'content_group', 'content_topic_clusters', 'content_topic']
).agg(
    collect_list('content').alias("contents"),
)

display('contents_per_topic')
display(contents_per_topics)

# COMMAND ----------

import pandas as pd
import numpy as np
from pyspark.ml.functions import predict_batch_udf
from pyspark.sql.types import MapType, StringType, IntegerType


from sklearn.feature_extraction.text import CountVectorizer
from nltk.corpus import stopwords
import nltk

from pyspark.sql.functions import udf
num_words=250
@udf(returnType=MapType(StringType(), IntegerType()))
def top_words_udf(contents):
    all_stop_words = set()
    #for lang in stopwords.fileids():  
    #    all_stop_words.update(stopwords.words(lang))
    stop_words_list = list(all_stop_words)

    vectorizer = CountVectorizer(stop_words=stop_words_list,lowercase=True, max_features=100, strip_accents='unicode')
    word_counts = vectorizer.fit_transform(contents).toarray().sum(axis=0)
    top_word_indices = word_counts.argsort()[::-1][:num_words]
    top_words = { vectorizer.get_feature_names_out()[i]: int(word_counts[i]) for i in top_word_indices }

    return top_words

terms_per_topics = contents_per_topics.withColumn(
    "terms", top_words_udf("contents")
).drop("contents").selectExpr(
    "*", "explode(terms) as (term,count)"
).drop("terms")

display('terms_per_topics')
display(terms_per_topics)

# COMMAND ----------

from pyspark.sql.functions import sum

terms_count_per_topic = terms_per_topics.groupBy(
    "project_id", "content_group", "content_topic_clusters", "content_topic"
).agg(sum("count").alias("count_sum"))

display('terms_count_per_topic')
display(terms_count_per_topic)

# COMMAND ----------

terms_per_topics_with_score = terms_per_topics.join(
    terms_count_per_topic, on=["project_id", "content_group", "content_topic_clusters", "content_topic"]
).selectExpr(
    "*", "count/count_sum as score"
).drop("count", "count_sum")

display('terms_per_topics_with_score')
terms_per_topics_with_score.show(5,truncate=False)

# COMMAND ----------

spark.sql(f"""
    DELETE FROM comp_content_terms
    WHERE project_id='{context["project_id"]}' AND
          content_group='{context["import_id"]}'
""")

terms_per_topics_with_score.writeTo(f"""comp_content_terms""").using("delta").append()
