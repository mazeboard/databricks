# Databricks notebook source
# MAGIC %pip install -U FlagEmbedding
# MAGIC %pip install gritlm
# MAGIC %pip install "mlflow-skinny[databricks]>=2.4.1"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text('tenantId', '')
dbutils.widgets.text('workspaceId', '')

tenant_id = dbutils.widgets.get('tenantId')
workspace_id = dbutils.widgets.get('workspaceId')

# COMMAND ----------

import pandas as pd
import numpy as np
import mlflow
from mlflow.models import infer_signature

from models.embedding import EmbeddingModel
from models.summarize import SummarizeModel

catalog = f"tenant_{tenant_id}"
schema = f"workspace_{workspace_id}"
model_name = "default"

mlflow.set_experiment(f"/Shared/data-sci/experiments/{model_name}")
mlflow.set_registry_uri("databricks-uc")

with mlflow.start_run():
    embedding_signature = infer_signature(model_input="", model_output=np.array([[1.0]]))
    mlflow.pyfunc.log_model(
        "embedding", python_model=EmbeddingModel(), pip_requirements=["FlagEmbedding"], signature=embedding_signature
    )

    summarize_signature = infer_signature(
        model_input={
            'text': '',
            'large_prompt': '',
            'medium_prompt': '',
            'small_prompt': ''
        },
        model_output={ 'large': '', 'medium': '', 'small': '' }
    )
    mlflow.pyfunc.log_model(
        "summarize", python_model=SummarizeModel(), pip_requirements=["GritLM"], signature=summarize_signature
    )

    run_id = mlflow.active_run().info.run_id
    
    mlflow.register_model(
        model_uri=f"runs:/{run_id}/embedding",
        name=f"{catalog}.{schema}.{model_name}-embedding"
    )

    mlflow.register_model(
        model_uri=f"runs:/{run_id}/summarize",
        name=f"{catalog}.{schema}.{model_name}-summarize"
    )
