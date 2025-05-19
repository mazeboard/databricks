# Databricks notebook source
#dbutils.library.restartPython()

# COMMAND ----------

from IPython.display import display
import sys
import os
sys.path.append(os.path.abspath('../libs'))
sys.path.append(os.path.abspath('../'))
from init import spark

# COMMAND ----------

from context import initialize_context

context = initialize_context()

(context)

# COMMAND ----------

from dataproject import DataProject
data_project = DataProject(context["project_id"])
configuration = data_project.getConfiguration()

data_project_import = {'importId': 'nGPTuosT6Z_jLb3gB1-Yk', 
  'params': {'format': {'quote': '"',
    'escape': '"',
    'encoding': 'UTF-8',
    'delimiter': ';',
    'quoteEscape': '"'},
   'mapping': [{'type': 'text', 'input': 'text'},
    {'type': 'text', 'input': 'original id'},
    {'type': 'text', 'input': 'user'},
    {'type': 'date', 'input': 'date', 'format': 'dd/MM/yyyy HH:mm'},
    {'type': 'text', 'input': 'lang'}]}}

# COMMAND ----------

from pyspark.sql.functions import row_number,lit,col
from pyspark.sql.window import Window
w = Window().orderBy(lit('A'))

raw_contents = spark.read.format('csv').options(
    header='True',
    delimiter=data_project_import['params']['format']['delimiter'],
    escape=data_project_import['params']['format']['escape'],
    quote=data_project_import['params']['format']['quote'],
    multiline='True'
).load(
    f'/home/taoufik/workspace/arlequin/tenant_{context["tenant_id"]}/workspace_{context["workspace_id"]}/raw/{data_project_import["importId"]}'
).withColumn("row_num", row_number().over(w))

display(raw_contents.limit(100))
display(raw_contents.count())

# COMMAND ----------

from pyspark.sql.functions import col, expr, lit, to_timestamp, udf, posexplode, sha1, date_format
from pyspark.sql.types import StringType, ArrayType

def chunk_string(input):
    n = 3072
    if type(input) is str:
        return [input[i:i+n] for i in range(0, len(input), n)]
    return input

chunk_string_udf = udf(lambda str: chunk_string(str),  ArrayType(StringType()))

def content_col():
    return posexplode(
        chunk_string_udf(data_project_import['params']['mapping'][0]['input'])
    ).alias('line', 'content')

def original_id_col():
    if len(data_project_import['params']['mapping'][1]['input']) > 0:
        return col(
            data_project_import['params']['mapping'][1]['input']
        ).alias("original_id")
    else:
        return col('row_num').cast("STRING").alias("original_id")

def dim_col(mapping, index):
    if mapping['type'] == 'date':
        return date_format(
            to_timestamp(col(mapping['input']), mapping['format']),
            "yyyy-MM-dd"
        ).alias(f"r_dim_{index}")
    else:
        return col(mapping['input']).alias(f"r_dim_{index}")
    
def dim_cols():
    return [dim_col(mapping, index - 1) for index, mapping in enumerate(data_project_import['params']['mapping']) if index > 1]

cleaned_contents = raw_contents.withColumn(
    "project_id", lit(context["project_id"])
).withColumn(
    "content_group", lit(context["import_id"])
).withColumn(
    "id",  expr("uuid()")
).select(
    col("project_id"),
    col("content_group"),
    col("id"),
    original_id_col(),
    content_col(),
    *dim_cols()
).where(
    "content IS NOT NULL"
)

display(cleaned_contents.limit(100))
display(cleaned_contents.count())

# COMMAND ----------

display(configuration)
configured_dimensions = configuration['dimensions']['value']

def aggregate_dim(dim, index):
    r_dim = f"r_dim_{index + 1}"

    return cleaned_contents.groupBy(r_dim).agg({r_dim: 'count'}).select(
        lit(context["project_id"]).alias("project_id"),
        lit(context["import_id"]).alias("content_group"),
        lit(index + 1).alias("dimension"),
        sha1(r_dim).alias("id"),
        col(r_dim).cast("STRING").alias("data_1"),
        col(f"count({r_dim})").cast('INTEGER').alias("count")
    ).where("id IS NOT NULL")

dimensions = [aggregate_dim(dim, index) for index, dim in enumerate(configured_dimensions)]

for index, dimension in enumerate(dimensions):
    spark.sql(f"""
        DELETE FROM norm_content_dims
        WHERE project_id='{context["project_id"]}' AND
              content_group='{context["import_id"]}' AND
              dimension={index + 1}
    """)
    dimension.writeTo(f"""norm_content_dims""").using("delta").append()
    display(dimension.limit(50))
    display(dimension.count())


# COMMAND ----------

import functools

def join_dim(joined_contents, dim, index):
    if dim.count() == 0:
        return joined_contents
    
    j_dim = f"j_dim_{index + 1}"
    prepared_dim = dim.select(col("id").alias(f"dim_{index + 1}"), col("data_1").alias(j_dim))
    return joined_contents.join(prepared_dim, prepared_dim[1].eqNullSafe(joined_contents[6 + index]), "outer")

joined_contents = functools.reduce(lambda joined_contents, dim: join_dim(joined_contents, dim[1], dim[0]), enumerate(dimensions), cleaned_contents)

normalized_contents = joined_contents.select(
    col("project_id"),
    col("content_group"),
    col("id"),
    col("original_id"),
    col("line"),
    col("content"),
    *[col(f"dim_{index + 1}") for index, dim in enumerate(dimensions) if dim.count() > 0]
)

spark.sql(f"""
    DELETE FROM norm_contents
    WHERE project_id='{context["project_id"]}' AND
          content_group='{context["import_id"]}'
""")
normalized_contents.writeTo(f"""norm_contents""").using("delta").append()

display(normalized_contents.limit(100))
display(normalized_contents.count())

# COMMAND ----------

#workspace.progress(context['job_id'])


print('done')