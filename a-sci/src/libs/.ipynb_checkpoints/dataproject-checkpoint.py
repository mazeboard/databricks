import itertools
import json
import psycopg2
import psycopg2.extras
from databricks.sdk.runtime import *

from databases import connection

class DataProject:

    def __init__(self, id):
        self.id = id

    def connection(self):
        return connection(dbutils.secrets.get(scope="arlq-vault", key="arlq-pg-data-projects-db-secret"))
    
    def getImport(self, importId):
        sql = f"""
            SELECT *
            FROM "data-project-attachments"
            WHERE id = %s
        """
        try:
            with self.connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                    cursor.execute(sql, (importId,))
                    attachment = cursor.fetchone()

                    def extract_params():
                        try:
                            return json.loads(attachment['params'])
                        except:
                            return attachment['params']

                    return {
                        "importId": attachment['external_id'],
                        "params": extract_params()
                    }
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def getConfiguration(self):
        sql = f"""
            SELECT *
            FROM "data-project-configurations"
            JOIN (
                SELECT
                    data_project_configuration_id,
                    JSON_AGG(
                        ROW_TO_JSON("data-project-configuration-parameters")
                    )
                FROM "data-project-configuration-parameters"
                GROUP BY data_project_configuration_id
            ) AS parameters
                ON parameters.data_project_configuration_id = "data-project-configurations".id
            WHERE "data-project-configurations".id = %s
        """
        try:
            with self.connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                    cursor.execute(sql, (self.id,))
                    configuration = cursor.fetchone()
                    
                    paramters_by_name = itertools.groupby(
                        sorted(configuration['json_agg'], key=lambda p: p['parameter']),
                        lambda p: p['parameter']
                    )

                    def format_parameter(parameter):
                        values = {}
                        
                        for value in parameter:
                            if value['key'] is None and value['index'] is None:
                                try:
                                    values['value'] = json.loads(value['value'])
                                except:
                                    values['value'] = value['value']

                        return values

                    return dict([(p, format_parameter(v)) for p, v in paramters_by_name])
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def export_content_topics(self, content_topics, import_id):
        sql = f"""
            INSERT INTO "data-project-content-topics"(
                id, data_project_id, import_id,
                content_topic, description, medium_description, small_description, topic_size, total_size
            )
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        try:
            with self.connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute('DELETE FROM "data-project-content-topics" WHERE data_project_id=%s AND import_id=%s', (self.id, import_id))
                    for row in content_topics:
                        cursor.execute(sql, (row["id"], row["project_id"], row["content_group"], row["content_topic"], row["description"], row["medium_description"], row["small_description"], row["topic_size"], row["total_size"]))
                    conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def export_content_topics_v2(self, content_topics, import_id):
        sql = f"""
            INSERT INTO "data-project-content-topics"(
                id, data_project_id, import_id,
                content_topic, content_topic_clusters, content_topic_ancestor,
                description, topic_size, total_size
            )
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        try:
            with self.connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute('DELETE FROM "data-project-content-topics" WHERE data_project_id=%s AND import_id=%s', (self.id, import_id))
                    for row in content_topics:
                        cursor.execute(sql, (
                            row["id"], row["project_id"], row["content_group"],
                            row["content_topic"], row["content_topic_clusters"], row["content_topic_ancestor"],
                            row["description"], row["topic_size"], row["total_size"]))
                    conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)  
