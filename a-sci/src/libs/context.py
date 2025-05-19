#from databricks.sdk.runtime import *

def initialize_context():
    #dbutils.widgets.text('tenantId', '')
    #dbutils.widgets.text('workspaceId', '')
    #dbutils.widgets.text('dataProjectId', '')
    #dbutils.widgets.text('importId', '')
    #dbutils.widgets.text('jobId', '')

    #tenant_id = dbutils.widgets.get('tenantId')
    #workspace_id = dbutils.widgets.get('workspaceId')
    #project_id = dbutils.widgets.get('dataProjectId')
    #import_id = dbutils.widgets.get('importId')
    #job_id = dbutils.widgets.get('jobId')


    return {
        "tenant_id": 'root',
        "workspace_id": 'root',
        "table_prefix": '',
        "project_id": 'project1',
        "import_id": 'import1',
        "job_id": 'job1'
    }