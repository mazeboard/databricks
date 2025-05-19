import psycopg2
from databricks.sdk.runtime import *

from databases import connection

class Workspace:

    def __init__(self, id):
        self.id = id

    def connection(self):
        return connection(dbutils.secrets.get(scope="arlq-vault", key="arlq-pg-workspaces-db-secret"))

    def initProgress(self, jobId, total):
        sql = """
            UPDATE "workspace-jobs"
            SET total_steps=%s, completed_steps=0
            WHERE id=%s
        """
        try:
            with self.connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, (total, jobId,))
                    conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def progress(self, jobId):
        sql = """
            UPDATE "workspace-jobs"
            SET completed_steps = completed_steps + 1
            WHERE id=%s
        """

        try:
            with self.connection() as conn:
                with conn.cursor() as cursor: 
                    cursor.execute(sql, (jobId,))
                    conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error) 
