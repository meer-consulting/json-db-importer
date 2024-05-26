import os
import time
import oracledb
from locust import events


class OracleClient:
    def __getattr__(self, name):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                res = self.execute_query(*args, **kwargs)
                res_time = int((time.time() - start_time) * 1000)
                res_len = len(res) if res is not None else 0
                print("Result: " + str(res))
                # リクエスト成功を master に通知
                events.request.fire(
                    request_type="oracle",
                    name=name,
                    response_time=res_time,
                    response_length=res_len,
                )
            except Exception as e:
                res_time = int((time.time() - start_time) * 1000)
                # リクエスト失敗を master に通知
                events.request.fire(
                    request_type="oracle",
                    name=name,
                    response_time=res_time,
                    exception=e,
                )
                print("error {}".format(e))

        return wrapper

    def connect(self):
        """
        データベース接続
        """
        username = os.environ["ORACLE_USER"]
        password = os.environ["ORACLE_PASSWORD"]
        dsn = os.environ["ORACLE_DSN"]
        self.connection = oracledb.connect(user=username, password=password, dsn=dsn)

    def disconnect(self):
        """
        データベース切断
        """
        if self.connection:
            self.connection.close()

    def execute_query(self, query):
        """
        クエリを実行する
        """
        try:
            cur = self.connection.cursor()
            cur.execute(query)
            return cur.fetchall()
        except Exception as e:
            print(format(e))

    def execute_update(self, query):
        """
        クエリを実行する
        """
        try:
            cur = self.connection.cursor()
            cur.execute(query)
            return cur.fetchall()
        except Exception as e:
            print(format(e))

    def get_conn(self):
        return self.connection
