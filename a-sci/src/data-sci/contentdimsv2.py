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


configured_dimensions =  configuration['dimensions']['value']
content_clusters = [20, 60, 180, 500]
content_clusters_components = configuration['content_cluster_components']['value']
content_dims_components = configuration['content_dim_components']['value']

print(configured_dimensions, content_clusters, content_clusters_components, content_dims_components)

# COMMAND ----------

contents = spark.sql(f"""
    SELECT
        norm_contents.*,
        comp_content_clusterings.content_topic_clusters, comp_content_clusterings.content_topic_ancestor,
        comp_content_clusterings.content_topic
    FROM norm_contents
    JOIN comp_content_clusterings
        ON comp_content_clusterings.project_id = norm_contents.project_id AND
           comp_content_clusterings.content_group = norm_contents.content_group AND
           comp_content_clusterings.content_id = norm_contents.id
    WHERE norm_contents.project_id='{context["project_id"]}' AND
          norm_contents.content_group='{context["import_id"]}'
""")

contents.limit(5).show(truncate=False)
display(contents.count())

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import col, lit

def compute_dim(contents, dim, cluster):
    dim_col = f"dim_{dim}"
    dim_col_alias = f"i_{dim_col}"
    content_topic_col_alias = "i_content_topic"

    contents_per_dim = contents.where(f'content_topic_clusters = {cluster}').groupBy(dim_col).count().select(
        col(dim_col),
        col('count').alias('total_size')
    ).where(f"{dim_col} IS NOT NULL")

    if contents_per_dim.count() == 0:
        return None

    print('contents_per_dim')
    contents_per_dim.show(5, truncate=False)

    contents_per_topics_per_dim = contents.groupBy([dim_col, "content_topic"]).count().select(
        col(dim_col).alias(dim_col_alias),
        col("content_topic"),
        col('count').alias('topic_size')
    )
    joined_contents_per_topics_per_dim = contents_per_dim.join(
        contents_per_topics_per_dim, contents_per_dim[dim_col] == contents_per_topics_per_dim[dim_col_alias], "inner"
    ).select(
        col(dim_col),
        col("content_topic"),
        col('total_size'),
        col('topic_size')
    )

    print('contents_per_topics_per_dim')
    contents_per_topics_per_dim.show(5, truncate=False)

    print('joined_contents_per_topics_per_dim')
    joined_contents_per_topics_per_dim.show(5,truncate=False)

    contents.select(
        col("content_topic"),
        col(dim_col)
    ).show(5,truncate=False)

    panda_contents = contents.select(
        col("content_topic"),
        col(dim_col)
    ).toPandas()
    panda_contents_per_dim = panda_contents.groupby(dim_col).size()
    print('panda_contents_per_dim')
    print(panda_contents_per_dim)
    panda_contents_per_topics_per_dims = panda_contents.pivot_table(
        index=dim_col, columns='content_topic', aggfunc='size', fill_value=0
    )
    print('panda_contents_per_topics_per_dims')
    print(panda_contents_per_topics_per_dims)

    panda_content_proportion_per_topics_per_dims = panda_contents_per_topics_per_dims.divide(panda_contents_per_dim, axis=0)
    print('panda_content_proportion_per_topics_per_dims')
    print(panda_content_proportion_per_topics_per_dims)

    panda_impact_per_topics_per_dims = panda_contents_per_topics_per_dims * panda_content_proportion_per_topics_per_dims

    print('panda_impact_per_topics_per_dims', cluster)
    display(panda_impact_per_topics_per_dims)

    num_topics = panda_contents_per_topics_per_dims.shape[1]
    value_vars = [i for i in range(min(num_topics,cluster))]
    panda_melted_impact_per_topics_per_dims =  spark.createDataFrame(
        pd.melt(
            panda_impact_per_topics_per_dims.reset_index(), id_vars=dim_col, value_vars=value_vars, var_name='content_topic', value_name='value'
        )
    ).select(
        col(dim_col).alias(dim_col_alias),
        col("content_topic").alias(content_topic_col_alias),
        col('value').alias('score')
    )
    scored_contents_per_topics_per_dim = joined_contents_per_topics_per_dim.join(panda_melted_impact_per_topics_per_dims, [
        joined_contents_per_topics_per_dim[dim_col] == panda_melted_impact_per_topics_per_dims[dim_col_alias],
        joined_contents_per_topics_per_dim['content_topic'] == panda_melted_impact_per_topics_per_dims[content_topic_col_alias]
    ], "inner").select(
        lit(context["project_id"]).alias("project_id"),
        lit(context["import_id"]).alias("content_group"),
        lit(cluster).alias("content_topic_clusters"),
        col("content_topic"),
        lit(dim).alias("dimension"),
        col(dim_col).alias("dimension_id"),
        col('total_size').cast("INTEGER"),
        col('topic_size').cast("INTEGER"),
        col('score').cast("DOUBLE")
    )

    return (scored_contents_per_topics_per_dim, panda_content_proportion_per_topics_per_dims)

content_topic_dims_clusters = [
    [compute_dim(contents, index + 1, cluster) for index, dim in enumerate(configured_dimensions)]
    for cluster in content_clusters
]

display('content_topic_dims_clusters', content_topic_dims_clusters)

spark.sql(f"""
    DELETE FROM comp_content_topic_dims
    WHERE project_id='{context["project_id"]}' AND
        content_group='{context["import_id"]}'
""")

for content_topic_dims in content_topic_dims_clusters:
    for index, dim in enumerate(content_topic_dims):
        if dim is not None:
            dim[0].writeTo(f"""comp_content_topic_dims""").using("delta").append()
            display(dim[0].limit(50))
            display(dim[0].count())

# COMMAND ----------

from pyspark.sql.types import StructType
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

scaler = StandardScaler()

def compute_dim_pca(panda_content_proportion_per_topics_per_dims, dim, cluster):
    if len(panda_content_proportion_per_topics_per_dims.index) < content_dims_components:
        return (spark.createDataFrame([], StructType([])), 0, 0)

    dim_col = f"dim_{dim}"
    standardized_content_proportion_per_topics_per_dims = scaler.fit_transform(panda_content_proportion_per_topics_per_dims)
    dim_pca = PCA(n_components=content_dims_components)
    dim_principal_components = dim_pca.fit_transform(standardized_content_proportion_per_topics_per_dims)
    dim_principal_components_df = pd.DataFrame(
        data=dim_principal_components,
        columns=[f'PC{index +1}' for index in range(content_dims_components)],
        index=panda_content_proportion_per_topics_per_dims.index
    )
    dim_principal_components_df[dim_col] = dim_principal_components_df.index

    return (
        spark.createDataFrame(dim_principal_components_df).select(
            lit(context["project_id"]).alias("project_id"),
            lit(context["import_id"]).alias("content_group"),
            lit(cluster).alias("content_topic_clusters"),
            lit(dim).alias("dimension"),
            col(dim_col).alias("dimension_id"),
            *[col(f'PC{index +1}').alias(f'pc{index + 1}') for index in range(content_dims_components)]
        ),
        dim_pca,
        panda_content_proportion_per_topics_per_dims
    )

dims_clusters = [
    [
        compute_dim_pca(content_topic_dims[index][1], index + 1, content_clusters[cluster_index])
        for index, dim in enumerate(configured_dimensions)
        if content_topic_dims[index] is not None
    ]
    for cluster_index, content_topic_dims
    in enumerate(content_topic_dims_clusters)
]

display('dims_clusters', dims_clusters)

spark.sql(f"""
    DELETE FROM comp_content_dim_clusterings
    WHERE project_id='{context["project_id"]}' AND
        content_group='{context["import_id"]}'
""")

for dims in dims_clusters:
    for index, dim in enumerate(dims):
        dim[0].writeTo(f"""comp_content_dim_clusterings""").using("delta").append()

        display(dim[0].limit(50))
        display(dim[0].count())

# COMMAND ----------

def compute_content_dim_topics(dim_pca, panda_content_proportion_per_topics_per_dims, dim, cluster):
    if dim_pca == 0:
        return spark.createDataFrame([], StructType([]))

    topics_loadings = dim_pca.components_.T  # Transpose to align with original variables
    topics_loadings_df = pd.DataFrame(data=topics_loadings, columns=['PC1', 'PC2'], index=panda_content_proportion_per_topics_per_dims.columns)
    topics_loadings_df['content_topic'] = topics_loadings_df.index
    topics_loadings_spark_df = spark.createDataFrame(topics_loadings_df)

    dim_topics_stats = spark.sql(f"""
        SELECT content_topic as i_content_topic, sum(total_size) as total_size, sum(topic_size) as topic_size, min(score) as min_score, max(score) as max_score
        FROM comp_content_topic_dims
        WHERE comp_content_topic_dims.project_id='{context["project_id"]}' AND
              comp_content_topic_dims.content_group='{context["import_id"]}' AND
              comp_content_topic_dims.content_topic_clusters={cluster} AND
              comp_content_topic_dims.dimension={dim}
        GROUP BY content_topic
    """)

    return topics_loadings_spark_df.join(
        dim_topics_stats,dim_topics_stats.i_content_topic == topics_loadings_spark_df.content_topic, "inner"
    ).select(
        lit(context["project_id"]).alias("project_id"),
        lit(context["import_id"]).alias("content_group"),
        lit(cluster).alias("content_topic_clusters"),
        col("content_topic").cast("INTEGER"),
        lit(dim).alias("dimension"),
        *[col(f'PC{index +1}').alias(f'pc{index + 1}') for index in range(content_dims_components)],
        col("total_size").cast("INTEGER"),
        col("topic_size").cast("INTEGER"),
        col("min_score").cast("DOUBLE"),
        col("max_score").cast("DOUBLE"),
    )

content_dim_topics_clusters = [
    [
        compute_content_dim_topics(dims[index][1], dims[index][2], index + 1, content_clusters[cluster_index])
        for index, dim in enumerate(configured_dimensions)
        if index < len(dims)
    ]
    for cluster_index, dims in enumerate(dims_clusters)
]

display('content_dim_topics_clusters', content_dim_topics_clusters)

spark.sql(f"""
    DELETE FROM comp_content_dim_content_topics
    WHERE project_id='{context["project_id"]}' AND
        content_group='{context["import_id"]}'
""")

for content_dim_topics in content_dim_topics_clusters:
    for index, dim in enumerate(content_dim_topics):
        dim.writeTo(f"""comp_content_dim_content_topics""").using("delta").append()

        display(dim.limit(100))

