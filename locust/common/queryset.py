class QuerySet:
    def get_show_tables(self):
        """
        テーブル一覧
        """
        query = """
            SELECT TABLE_NAME
            FROM USER_TABLES
            """
        return query

    def get_create_table(self, table_name):
        return f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                jsondata JSON
            )
            """

    def get_drop_table(self, table_name):
        return f"""
            DROP TABLE IF EXISTS {table_name}
            """

    def get_create_index_search(self, index_name, table_name):
        return f"""
            CREATE SEARCH INDEX IF NOT EXISTS {index_name} ON {table_name} (jsondata)
            """

    def get_drop_index_search(self, index_name):
        return f"""
            DROP INDEX IF EXISTS {index_name}
            """

    def get_truncate(self, table_name):
        return f"""
            TRUNCATE TABLE {table_name}
            """

    def get_insert_1wiki_task(self, table_name):
        return f"""
            INSERT INTO {table_name} VALUES (:1)
            """

    def get_insert_N_wiki_task(self, table_name, n):
        tuples = [f"(:{i})" for i in range(1, n + 1)]
        return f"""
            INSERT INTO {table_name} VALUES {', '.join(tuples)}
            """

    def get_query_all_userid(self):
        return """
            SELECT t.id
            FROM GRAPH_TABLE ( graph1
            MATCH (n)
            WHERE n.label = 'user'
            COLUMNS (n.id)
            ) t
            """

    def get_insert_task(self):
        query = """
            INSERT INTO graph1node 
            VALUES (
            SYS_GUID(),
            'task',
            '{"name":"タスク1", "start":"2017-09-21"}'
            )
            """
        return query

    def get_show_tasks(self):
        query = """
            SELECT t.id
                , JSON_VALUE(t.props, '$.name')
                , JSON_VALUE(t.props, '$.start')
            FROM GRAPH_TABLE ( graph1
            MATCH (n)
            WHERE n.label = 'task'
            COLUMNS (n.id, n.label, n.props)
            ) t
            ORDER BY JSON_VALUE(t.props, '$.S_ES1')
                , t.id
            OFFSET 10 ROWS FETCH FIRST 10 ROWS ONLY
            """
        return query

    def get_with_bindvar(self, varname):
        query = f"""
            SELECT t.id, JSON_VALUE(t.props, '$.name'), JSON_VALUE(t.props, '$.start')
            FROM GRAPH_TABLE ( graph1
            MATCH (n)
            WHERE n.label = 'task'
                AND n.id = :{varname}
            COLUMNS (n.id, n.label, n.props)
            ) t
            """
        return query

    def get_create_node(self):
        return """
            INSERT INTO graph1node VALUES (
            :id
            , :label
            , :props
            )
            """

    def get_create_edge(self):
        return """
            INSERT INTO graph1edge VALUES (
            :id
            , :src
            , :dst
            , :label
            , :props
            )
            """


queries = QuerySet()
