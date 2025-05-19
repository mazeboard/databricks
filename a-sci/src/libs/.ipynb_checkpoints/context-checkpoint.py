from databricks.sdk.runtime import *

def initialize_context():
    dbutils.widgets.text('tenantId', '')
    dbutils.widgets.text('workspaceId', '')
    dbutils.widgets.text('dataProjectId', '')
    dbutils.widgets.text('importId', '')
    dbutils.widgets.text('jobId', '')

    tenant_id = dbutils.widgets.get('tenantId')
    workspace_id = dbutils.widgets.get('workspaceId')
    project_id = dbutils.widgets.get('dataProjectId')
    import_id = dbutils.widgets.get('importId')
    job_id = dbutils.widgets.get('jobId')


    table_prefix = f"tenant_{tenant_id}.workspace_{workspace_id}"

    return {
        "tenant_id": tenant_id,
        "workspace_id": workspace_id,
        "table_prefix": table_prefix,
        "project_id": project_id,
        "import_id": import_id,
        "job_id": job_id
    }