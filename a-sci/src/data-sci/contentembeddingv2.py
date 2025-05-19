# Databricks notebook source
# MAGIC %pip install --upgrade --upgrade-strategy eager "optimum[ipex]"
# MAGIC %pip install sentence_transformers


# COMMAND ----------

import sys
import os
import time

import numpy as np
from sentence_transformers import SentenceTransformer
sys.path.append(os.path.abspath('../libs'))
sys.path.append(os.path.abspath('../'))
from init import spark
from init import sc
from IPython.display import display
from pyspark.sql import functions as F
from pyspark.sql import Window

# COMMAND ----------

from context import initialize_context

context = initialize_context()

(context)

# COMMAND ----------

from dataproject import DataProject
data_project = DataProject(context["project_id"])
configuration = data_project.getConfiguration()
#data_project_import = data_project.getImport(context['import_id'])

display(configuration)

# COMMAND ----------

contents = spark.sql(f"""SELECT * FROM norm_contents WHERE project_id='{context["project_id"]}' AND content_group='{context["import_id"]}'""")

display(contents.limit(50))
display(contents.count())

# COMMAND ----------

from pyspark.ml.functions import predict_batch_udf
from pyspark.sql.types import ArrayType, IntegerType
from pyspark.sql.functions import col
from pyspark.sql.types import ArrayType

#sc.setCheckpointDir('/home/taoufik/workspace/arlequin/checkpoints')

def embed_fn():
    from sentence_transformers import SentenceTransformer
    m = SentenceTransformer('BAAI/bge-m3')
    def predict(inputs):
        return m.encode(inputs,batch_size=16,show_progress_bar=True,precision="binary",device="cpu")
    
    return predict


embed_udf = predict_batch_udf(embed_fn,
                              return_type=ArrayType(IntegerType()),
                              batch_size=1024)

start_time = time.time()  # Start timing the encoding

embeddings =  contents.repartition(64).withColumn("embedding", embed_udf("content")) #.checkpoint()

spark.sql(f"""
    DELETE FROM comp_content_embeddings
    WHERE project_id='{context["project_id"]}' AND
          content_group='{context["import_id"]}'
""")


final = embeddings.select(
    col("project_id"),
    col("content_group"),
    col("id").alias("content_id"),
    col("embedding").alias("dense")
)

final.show(truncate=False)

#final.write.parquet("/home/taoufik/workspace/arlequin/embeddings", mode="overwrite")
final.writeTo(f"""comp_content_embeddings""").using("delta").append()

end_time = time.time()  # End timing the encoding
elapsed_time = end_time - start_time  # Calculate the time taken for this row
print(f'***** Time: {elapsed_time:.4f} seconds')

display(final)

