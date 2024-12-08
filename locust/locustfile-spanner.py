import os
import json
import time
import logging
from google.cloud import spanner
from common.spanner_client import SpannerClient
from common.queryset import queries
from locust import User, task, between, events
from locust.runners import MasterRunner
from wikiloader.wikiloader import WikiJsonIterator

# ログレベルの設定
logging.basicConfig(level=logging.INFO)

PROPERTY_NUM = 5
INDEX_NAME = os.environ.get("SEARCH_INDEX_NAME")
TABLE_NAME = os.environ.get("TABLE_NAME")
TASK_RATIO_1_WIKI_IMPORT = os.environ.get("TASK_RATIO_1_WIKI_IMPORT")
TASK_RATIO_1_WIKI_IMPORT = (
    int(TASK_RATIO_1_WIKI_IMPORT) if TASK_RATIO_1_WIKI_IMPORT else 0
)


@events.init.add_listener
def on_locust_init(environment, **kwargs):
    if isinstance(environment.runner, MasterRunner):
        print("###############################start")
        """
        テスト開始時に一度実行
        """
        client = SpannerClient()
        client.connect()
        init_db(client)
        # client.disconnect()


def init_db(client):
    """
    最初に索引を構築
    """
    # DB初期化処理
    drop_tables(client)
    create_tables(client)
    create_index(client)


def create_tables(client):
    from google.cloud.spanner_admin_database_v1.types import spanner_database_admin

    spanner_client = client.get_client()
    database_admin_api = spanner_client.database_admin_api

    request = spanner_database_admin.UpdateDatabaseDdlRequest(
        database=client.database_path,
        statements=[
            """
            CREATE SEQUENCE IF NOT EXISTS WikiIdSeq OPTIONS (
                sequence_kind = 'bit_reversed_positive'
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS {} (
                id INT64 DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE WikiIdSeq)),
                doc_json JSON,
                doc_json_tokens TOKENLIST AS (TOKENIZE_FULLTEXT(JSON_VALUE(doc_json, '$.text'))) HIDDEN
            ) PRIMARY KEY (id)
            """.format(
                TABLE_NAME
            ),
        ],
    )

    operation = database_admin_api.update_database_ddl(request)

    print("Waiting for operation to complete...")
    operation.result()
    print("Created tables.")


def drop_tables(client):
    from google.cloud.spanner_admin_database_v1.types import spanner_database_admin

    spanner_client = client.get_client()
    database_admin_api = spanner_client.database_admin_api

    request = spanner_database_admin.UpdateDatabaseDdlRequest(
        database=client.database_path,
        statements=[
            """DROP TABLE IF EXISTS {}""".format(TABLE_NAME),
            """DROP SEQUENCE IF EXISTS WikiIdSeq""",
        ],
    )

    operation = database_admin_api.update_database_ddl(request)

    print("Waiting for operation to complete...")
    operation.result()
    print("Droped tables.")


def truncate_tables(client):
    pass


def create_index(client):
    if INDEX_NAME:
        from google.cloud.spanner_admin_database_v1.types import spanner_database_admin

        spanner_client = client.get_client()
        database_admin_api = spanner_client.database_admin_api

        request = spanner_database_admin.UpdateDatabaseDdlRequest(
            database=client.database_path,
            statements=[
                """CREATE SEARCH INDEX IF EXISTS {} ON {}(doc_json_tokens)""".format(
                    INDEX_NAME, TABLE_NAME
                )
            ],
        )

        operation = database_admin_api.update_database_ddl(request)

        print("Waiting for operation to complete...")
        operation.result()
        print("Created indexes.")


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    print("###############################end")
    """
    テスト終了時に一度実行
    """
    logging.info("A new test is ending")
    # TODO: ダンプ


# Transaction for Spanner
def tx_insert_1wiki(transaction, jsondumps: str):
    """
    Insert a wiki document including JSON
    """
    row_cnt = transaction.insert(
        TABLE_NAME,
        columns=["doc_json"],
        values=[
            [jsondumps],
        ],
    )
    logging.info(f"affected rows: {row_cnt}")


class SpannerUser(User):
    REQUEST_TYPE = "spanner"

    # locust options
    wait_time = between(0, 0)

    # others
    wikiJsonItr = WikiJsonIterator("/data/")
    load_limit = int(os.environ["MAX_JSON_LOAD"])
    load_cnt = 0
    client = SpannerClient()

    @classmethod
    def get_wiki(cls, n):
        wikis = []
        for i in range(n):
            cls.load_cnt += 1
            jsondump = next(cls.wikiJsonItr)
            if not jsondump:
                break
            wikis.append(json.dumps(jsondump))

        if len(wikis) == 0:
            return None
        else:
            return wikis

    @classmethod
    def reached_load_limit(cls):
        return cls.load_limit <= cls.load_cnt

    def __init__(self, *args, **kwargs):
        super(SpannerUser, self).__init__(*args, **kwargs)

    # ユーザ作成時に1度実行
    def on_start(self):
        self.client.connect()

    # ユーザ破棄時に1度実行
    def on_stop(self):
        self.client.disconnect()

    # タスクの結果をイベントに通知
    def events_request_fire(self, task_name, start_time, exception):
        res_time = int((time.time() - start_time) * 1000)
        if exception:
            # リクエスト成功を master に通知
            events.request.fire(
                request_type=SpannerUser.REQUEST_TYPE,
                name=task_name,
                response_time=res_time,
                response_length=0,
            )
        else:
            # リクエスト失敗を master に通知
            events.request.fire(
                request_type=SpannerUser.REQUEST_TYPE,
                name=task_name,
                response_time=res_time,
                exception=exception,
                response_length=0,
            )

    @task(TASK_RATIO_1_WIKI_IMPORT)
    def tx_insert_1wiki(self):
        """
        Wiki 1 件インポート
        """
        if SpannerUser.reached_load_limit():
            self.environment.runner.quit()

        task_name = "insert_1_wiki"

        start_time = time.time()
        try:
            database = self.client.get_database()

            wikidumps = SpannerUser.get_wiki(1)[0]
            database.run_in_transaction(tx_insert_1wiki, jsondumps=wikidumps)

            self.events_request_fire(task_name, start_time, None)

        except Exception as e:
            logging.error("error {}".format(e))
            self.events_request_fire(task_name, start_time, e)
