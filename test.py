from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit, explode, posexplode, row_number, expr
from pyspark.sql.types import ArrayType, DoubleType
from FlagEmbedding import BGEM3FlagModel

spark = SparkSession.builder \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

import pandas as pd
import numpy as np

df = pd.read_csv('laicite_gd.csv', delimiter=";")
df['source'] = df['screen name']
dx = df[['text','source']]
dx2 = dx.dropna()

def split_string(string, max_length=4096):
    return [string[i:i + max_length] for i in range(0, len(string), max_length)]

dx2['text'] = dx2['text'].apply(split_string)
dx2 = dx2.explode('text').reset_index(drop=True)

sent = dx2['text'].tolist()
s = [str(x) for x in sent][:10]
len(s)

model = BGEM3FlagModel('BAAI/bge-m3', device='cpu',  use_fp16=False)  # Pass session to the model

def embedding(text):
    dense_vecs = model.encode([text], return_dense=True, max_length=4096)["dense_vecs"]
    return [float(x) for x in dense_vecs]

#s = ['Before you start, you will need to setup your environment by installing the appropriate packages.']
sdf = spark.createDataFrame([(string,) for string in s], ["text"])
embedding_udf = udf(embedding, ArrayType(DoubleType()))
sdf.withColumn("vec", embedding_udf("text")).show(truncate=False)

