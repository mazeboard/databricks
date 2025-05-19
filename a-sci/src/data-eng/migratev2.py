# COMMAND ----------
import sys
import os
sys.path.append(os.path.abspath('../libs'))
sys.path.append(os.path.abspath('../'))
from init import spark
from init import table_path

spark.sql(f"ALTER TABLE comp_content_clusterings ADD COLUMNS (content_topic_clusters INTEGER, content_topic_ancestor INTEGER)")

# COMMAND ----------

spark.sql(f"ALTER TABLE comp_content_topics ADD COLUMNS (content_topic_clusters INTEGER, content_topic_ancestor INTEGER)")

# COMMAND ----------

from delta.tables import DeltaTable

# Computed Tables
(DeltaTable.createIfNotExists()
    .tableName(f"comp_content_embeddings")
    .location(f"{table_path}/comp_content_embeddings")
    .addColumn("project_id", dataType = "STRING")
    .addColumn("content_group", dataType = "STRING")
    .addColumn("content_id", dataType = "STRING")
    .addColumn("dense", dataType = "ARRAY<INTEGER>")
    .clusterBy("project_id", "content_group", "content_id")
    .execute())

# COMMAND ----------

spark.sql(f"ALTER TABLE comp_content_topic_dims ADD COLUMNS (content_topic_clusters INTEGER)")

# COMMAND ----------

spark.sql(f"ALTER TABLE comp_content_dim_clusterings ADD COLUMNS (content_topic_clusters INTEGER)")

# COMMAND ----------

spark.sql(f"ALTER TABLE comp_content_dim_content_topics ADD COLUMNS (content_topic_clusters INTEGER)")

# COMMAND ----------

spark.sql(f"ALTER TABLE comp_content_terms ADD COLUMNS (content_topic_clusters INTEGER)")
