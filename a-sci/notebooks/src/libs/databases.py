import psycopg2
from databricks.sdk.runtime import *

driver = "org.postgresql.Driver"
database_host = dbutils.secrets.get(scope="arlq-vault", key="arlq-pg-host-secret")
database_port = "5432"
user = dbutils.secrets.get(scope="arlq-vault", key="arlq-pg-username-secret")
password = dbutils.secrets.get(scope="arlq-vault", key="arlq-pg-pwd-secret")

def connection(database_name):
    return psycopg2.connect(
        host=database_host,
        database=database_name,
        user=user,
        password=password,
        sslmode="require"
    )