import os
import time
from google.cloud import spanner
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

DBMS_NAME = "spanner"


class SpannerClient:
    def connect(self):
        """
        データベース接続
        """
        instance_id = os.environ["SPANNER_INSTANCE_ID"]
        database_id = os.environ["SPANNER_DATABASE_ID"]

        token = os.environ["GOOGLE_CLOUD_ACCESSTOKEN"]
        credentials = Credentials(token=token)

        self.client = spanner.Client(credentials=credentials)
        self.instance = self.client.instance(instance_id)
        self.database = self.instance.database(database_id)

        database_admin_api = self.client.database_admin_api
        self.database_path = database_admin_api.database_path(
            self.client.project, instance_id, database_id
        )

    def disconnect(self):
        """
        データベース切断
        """
        pass

    def execute_query(self, query):
        """
        クエリを実行する
        """
        with self.database.snapshot() as snapshot:
            results = snapshot.execute_sql(query)

            return results

    def execute_update(self, query):
        pass

    def get_client(self):
        return self.client

    def get_instance(self):
        return self.instance

    def get_database(self):
        return self.database
