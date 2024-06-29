import os
import json
import random
import oracledb

assert "ORACLE_USER" in os.environ
assert "ORACLE_PASSWORD" in os.environ
assert "ORACLE_DSN" in os.environ
assert "ORACLE_MODE" in os.environ
assert "ORACLE_JSON_BENCH_TABLE" in os.environ


def tx_insert_oneattr_ascii(conn, text_len):
    jsondict["text"] = "".join(
        [chr(random.randint(ord("a"), ord("z"))) for _ in range(text_len)]
    )
    if conn.thin or client_version >= 21:
        # direct binding
        cur.setinputsizes(oracledb.DB_TYPE_JSON)
        cur.execute(query, [jsondict])
    else:
        cur.execute(query, [json.dumps(jsondict)])
    print(f"OK! JSON length: {json_len}")
    conn.commit()


def tx_insert_oneattr_multibyte(conn, text_len):
    jsondict["text"] = "".join(
        [chr(random.randint(ord("あ"), ord("ん"))) for i in range(text_len)]
    )
    jsondata = json.dumps(jsondict)
    cur.execute(query, [jsondata])
    print(f"OK! JSON length: {json_len}")

    implicit_results = cur.getimplicitresults()
    for result in implicit_results:
        print(f"Warning: {result}")
    conn.commit()


def tx_insert_multiattr_ascii(conn, attr_n, text_len):
    jsondict["text"] = "".join(
        [chr(random.randint(ord("a"), ord("z"))) for i in range(text_len)]
    )
    jsondata = json.dumps(jsondict)
    cur.execute(query, [jsondata])
    print(f"OK! JSON length: {json_len}")

    implicit_results = cur.getimplicitresults()
    for result in implicit_results:
        print(f"Warning: {result}")
    conn.commit()


if __name__ == "__main__":
    username = os.environ["ORACLE_USER"]
    password = os.environ["ORACLE_PASSWORD"]
    dsn = os.environ["ORACLE_DSN"]
    table_name = os.environ["ORACLE_JSON_BENCH_TABLE"]
    required_thick = os.environ["ORACLE_MODE"] == "THICK"

    # use thick mode if available
    if required_thick:
        oracledb.init_oracle_client()

    conn = oracledb.connect(user=username, password=password, dsn=dsn)

    # collect db and client info
    client_version = 0
    if not conn.thin:
        client_version = oracledb.clientversion()[0]
    db_version = int(conn.version.split(".")[0])

    jsondict = dict(text="")
    query = f"INSERT INTO {table_name} VALUES (:1)"
    # display info about db / client / query / etc.
    print(f"client version: {client_version}")
    print(f"db version: {db_version}")
    print(f"query: {query}")
    print(f"bind: {jsondict}")
    print(f"connection thin mode: {conn.thin}")
    with conn.cursor() as cur:
        json_len = 1000
        try:
            while True:
                # tx_insert_oneattr_multibyte(conn, json_len)
                tx_insert_oneattr_ascii(conn, json_len)
                json_len += 1000
        except oracledb.DatabaseError as e:
            print(f"NG! JSON length: {json_len}")
            (error,) = e.args
            print("Oracle-Error-Code:", error.code)
            print("Oracle-Error-Message:", error.message)

    conn.close()
