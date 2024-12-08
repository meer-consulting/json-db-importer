import os
import time
import logging
import oracledb
from common.oracle_client import OracleClient
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
        client = OracleClient()
        client.connect()
        conn = client.get_conn()
        init_db(conn)
        client.disconnect()


def init_db(conn):
    """
    最初に索引を構築
    """
    with conn.cursor() as cur:
        conn.begin()  # トランザクション開始
        # DB初期化処理
        drop_tables(cur)
        create_tables(cur)
        create_index(cur)
        conn.commit()  # トランザクション終了


def create_tables(cur):
    cur.execute(queries.get_create_table(TABLE_NAME))


def drop_tables(cur):
    cur.execute(queries.get_drop_table(TABLE_NAME))


def truncate_tables(cur):
    cur.execute(queries.get_truncate(TABLE_NAME))


def create_index(cur):
    if INDEX_NAME:
        cur.execute(queries.get_create_index_search(INDEX_NAME, TABLE_NAME))


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    print("###############################end")
    """
    テスト終了時に一度実行
    """
    logging.info("A new test is ending")
    # TODO: ダンプ


class OracleUser(User):
    REQUEST_TYPE = "oracle"
    wait_time = between(0, 0)

    wikiJsonItr = WikiJsonIterator("/data/")
    load_limit = int(os.environ["MAX_JSON_LOAD"])
    load_cnt = 0
    client = OracleClient()

    @classmethod
    def create_bind_by(cls, n):
        bind = []
        for i in range(n):
            cls.load_cnt += 1
            jsondump = next(cls.wikiJsonItr)
            if not jsondump:
                break
            bind.append(jsondump)

        if len(bind) == 0:
            return None
        else:
            return bind

    @classmethod
    def reached_load_limit(cls):
        return cls.load_limit <= cls.load_cnt

    def __init__(self, *args, **kwargs):
        super(OracleUser, self).__init__(*args, **kwargs)

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
                request_type=OracleUser.REQUEST_TYPE,
                name=task_name,
                response_time=res_time,
                response_length=0,
            )
        else:
            # リクエスト失敗を master に通知
            events.request.fire(
                request_type=OracleUser.REQUEST_TYPE,
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
        if OracleUser.reached_load_limit():
            self.environment.runner.quit()

        conn = self.client.get_conn()
        task_name = "insert_1_wiki"

        start_time = time.time()
        try:
            with conn.cursor() as cur:
                conn.begin()
                bind = OracleUser.create_bind_by(1)
                if bind is not None:
                    query = queries.get_insert_N_wiki_task(TABLE_NAME, len(bind))
                    cur.setinputsizes(oracledb.DB_TYPE_JSON)
                    cur.execute(query, bind)
                conn.commit()

                logging.info(f"affected rows: {cur.rowcount}")
                self.events_request_fire(task_name, start_time, None)

        except Exception as e:
            logging.error("error {}".format(e))
            self.events_request_fire(task_name, start_time, e)
