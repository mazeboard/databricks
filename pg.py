import itertools
import json
import psycopg2
import psycopg2.extras
from databricks.sdk.runtime import *

from databases import connection

class MyDataProject:

    def __init__(self):
        self.id = 'noid'

    def connection(self):
        return connection(dbutils.secrets.get(scope="arlq-vault", key="arlq-pg-data-projects-db-secret"))
    
    def getAttachments(self):
        sql = f"""
            SELECT *
            FROM "data-project-attachments"
        """
        try:
            with self.connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                    cursor.execute(sql)
                    attachments = cursor.fetchall()
                    column_names = [desc[0] for desc in cursor.description]
                    return column_names, attachments

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            return None, None

    def getConfigurations(self):
        sql = f"""
            SELECT *
            FROM "data-project-configurations"
        """
        try:
            with self.connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                    cursor.execute(sql)
                    configurations = cursor.fetchall()
                    column_names = [desc[0] for desc in cursor.description]
                    return column_names, configurations
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            return None, None

mydata_project = MyDataProject()
column_names, attachments = mydata_project.getAttachments()
column_names
