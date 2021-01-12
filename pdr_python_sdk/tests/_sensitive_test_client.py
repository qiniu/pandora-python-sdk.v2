import os
import time
import unittest
import pdr_python_sdk


class TestClientMethods(unittest.TestCase):

    def setUp(self):
        """
        Demo connection params
        """
        params = {
            "scheme": os.getenv("PANDORA_SCHEME", "http"),
            "host": os.getenv("PANDORA_HOST"),
            "port": os.getenv("PANDORA_PORT", None),
            "token": os.getenv("PANDORA_TOKEN")
        }
        if not params["host"]:
            raise RuntimeError("PANDORA_HOST must be set")
        if not params["token"]:
            raise RuntimeError("PANDORA_TOKEN must be set")
        self.conn = pdr_python_sdk.connect(**params)

    def test_data_upload(self):
        """
        上传接口demo
        """
        data = [
            # 全字段的数据
            {
                "raw": """{"a":"de", "dd":1}""",
                "repo": "default",
                "sourcetype": "json",
                "host": "192.168.1.1",
                "origin": "/path/to/file/log.txt",
                "collectTime": 1582619883000,
                "timestamp": 1582619883000
            },
            # 如果repo和sourcetype 全局制指定了，就可以在数据中忽略掉
            {
                "raw": """{"a":"de", "dd":1}""",
                "host": "192.168.1.1",
                "origin": "/path/to/file/log.txt",
                "collectTime": 1582619883000,
                "timestamp": 1582619883000
            },
            # 最简单的数据上传
            {
                "raw": """{"a":"de", "dd":1}"""
            }
        ]
        # 上传前会做参数检验
        ret = self.conn.data_upload_strictly(data=data, repo="default", sourcetype="json")
        self.assertIsNotNone(ret)
        # 上传前不会做参数检验
        ret = self.conn.data_upload(data=data, repo="default", sourcetype="json")
        self.assertIsNotNone(ret)

    def test_search_events(self):
        """
        搜索型SPL demo
        """
        # 查看前端错误的仓库内数据
        # fast 模式只计算内置字段
        # smart 模式只计算搜索的数据中出现的字段
        # detailed 模式计算所有字段的统计信息
        query_info = self.conn.create_query_job(
            """ repo=_frontend_error """,
            start=0,
            end=int(time.time() * 1000),
            mode="smart",
            collectSize=1000
        )
        # 从返回结果中获得query_id
        query_id = query_info['id']
        # 轮询获得搜索任务是否成功
        while True:
            last_status = self.conn.get_query_status(query_id)
            if last_status['process'] == 1:
                break
            time.sleep(0.1)
        # 搜索型SPL
        self.assertFalse(last_status['isResult'])
        events = self.conn.get_query_events(query_id)
        self.assertIsInstance(events['total'], int, "total must be integer")
        self.assertIsInstance(events['rows'], list, "rows must be list")

        timeline = self.conn.get_query_timeline(query_id)
        self.assertIsInstance(timeline['buckets'], list, "buckets must be list")

        summary = self.conn.get_query_summary(query_id)
        self.assertTrue("repo" in summary)

    def test_search_stats(self):
        """
        计算型搜索demo
        """
        # 查看前一小时前端错误
        # fast 模式只计算内置字段
        # smart 模式只计算搜索的数据中出现的字段
        # detailed 模式计算所有字段的统计信息
        query_info = self.conn.create_analysis_job(
            """ repo=_frontend_error | stats count() as cnt""",
            start=0,
            end=int(time.time() * 1000),
            collectSize=10000
        )
        # 从返回结果中获得query_id
        query_id = query_info['id']
        # 轮询获得搜索任务是否成功
        while True:
            last_status = self.conn.get_query_status(query_id)
            if last_status['process'] == 1:
                break
            time.sleep(0.1)
        # 计算型SPL
        self.assertTrue(last_status['isResult'])
        results = self.conn.get_query_results(query_id)
        self.assertEqual(len(results['fields']), 1)
        self.assertIsInstance(results['rows'], list, "rows must be list")

    def test_repo_management(self):
        """
        仓库管理相关demo，不使用分层存储
        """
        dm = pdr_python_sdk.DataManager(conn=self.conn)
        repo_name = "testrepo_by_sdk"
        # 创建仓库。不使用分层存储
        dm.create_repo_if_absent(repo_name, lifeCycleEnable=False)

        # 获得仓库列表
        self.conn.get_repos(prefix=repo_name, pageSize=5, pageNo=1)

        # 获取单个仓库配置
        config = self.conn.get_repo_by_name(repo_name)

        config["description"] = "to update by sdk"

        # 如果不启动分层存储，需要手动把分层策略从配置中删除
        if not config['lifecyclePolicyEnable']:
            config.pop("lifecyclePolicy")

        # 更新仓库配置
        self.conn.update_repo_by_body(repo_name, config)
        # 删除仓库
        dm.delete_repo_if_exists(repo_name)

        # 创建仓库。使用分层存储，分层存储时间单位为毫秒
        # 7天热存储
        # 30天温存储
        # 365天冷存储
        # shardMaxDocs, shardMaxSize, indexMaxAge
        layer_reponame = "testlayer_by_sdk"
        dm.create_repo_if_absent(layer_reponame, lifeCycleEnable=True, lifeCycle={
            "hot": 7 * 24 * 60 * 60 * 1000,
            "warm": 30 * 24 * 60 * 60 * 1000,
            "cold": 365 * 24 * 60 * 60 * 1000
        }, rollover={
            "shardMaxDocs": "100k",
            "shardMaxSize": "10gb",
            "indexMaxAge": "1d"
        })

        dm.delete_repo_if_exists(layer_reponame)

    def test_query_mapping(self):
        """
        获得SPL计算结果的字段类型信息
        :return:
        """
        self.conn.get_query_mapping("repo=_frontend_error")

    def test_search_manager_conn(self):
        """
        使用high level的搜索接口
        :return:
        """
        sm = pdr_python_sdk.SearchManager(
            scheme=self.conn.scheme,
            host=self.conn.host,
            port=self.conn.port,
            token=self.conn.token
        )
        sm.query("repo=_frontend_error", start=0, end=int(time.time() * 1000), verbose=True)

    def test_search_manager(self):
        """
        使用high level的搜索接口
        :return:
        """
        sm = pdr_python_sdk.SearchManager(self.conn)
        sm.query("repo=_frontend_error", start=0, end=int(time.time() * 1000), verbose=True)

    def test_search_manager_pandas(self):
        """
        使用high level的搜索接口
        :return:
        """
        sm = pdr_python_sdk.SearchManager(self.conn)
        df = sm.query_to_pandas(
            "repo=_frontend_error | stats count() by origin",
            start=0,
            end=int(time.time() * 1000),
            collectSize=10
        )
        # two columns: origin, host
        self.assertEqual(len(df.columns), 2)

    def test_data_manager_repos(self):
        """
        使用high level的仓库管理接口
        :return:
        """
        dm = pdr_python_sdk.DataManager(self.conn)
        dm.create_repo_if_absent("test_create_absent")
        # will not fail
        dm.create_repo_if_absent("test_create_absent")

        dm.delete_repo_if_exists("test_create_absent")
        # will not fail
        dm.delete_repo_if_exists("test_create_absent")

    def test_data_manager(self):
        """
        使用high level的数据接口
        :return:
        """
        dm = pdr_python_sdk.DataManager(self.conn)
        dm.save_records_raw_json([
            {"a": 1, "b": 2, "c": "abdfda"},
            {"a": 1, "b": 2, "d": 3},
            {"a": 1, "b": 2, "c": "dfdsas"},
            {"a": 1, "b": 2, "c": "fdasfdsa"}
        ])
        dm.save_records_raw_json([
            {"t": int(time.time() * 1000) - 3, "a": 1, "b": 2, "c": "time_field"},
            {"t": int(time.time() * 1000) - 2, "a": 1, "b": 2, "d": 3},
            {"t": int(time.time() * 1000) - 1, "a": 1, "b": 2, "c": "time_field"},
            {"t": int(time.time() * 1000), "a": 1, "b": 2, "c": "time_field"}
        ], time_field="t")

    def test_data_manager_write_pandas(self):
        """
        使用high level的数据接口，读取pandas dataframe，写回pandora
        :return:
        """
        sm = pdr_python_sdk.SearchManager(self.conn)
        df = sm.query_to_pandas(
            "repo=_frontend_error",
            start=0,
            end=int(time.time() * 1000),
            collectSize=10
        )
        # two columns: origin, host
        dm = pdr_python_sdk.DataManager(self.conn)
        dm.save_pandas_dataframe(df, origin="pandas", time_field="_time")

    def test_sourcetype(self):
        if self.conn.is_exist_sourcetype("test_st"):
            self.conn.delete_sourcetype_by_name("test_st")
            assert not self.conn.is_exist_sourcetype("test_st")
        self.conn.create_sourcetype("test_st", field_discovery=True)
        assert self.conn.is_exist_sourcetype("test_st")
        assert self.conn.get_sourcetype_by_name("test_st")["name"] == "test_st"
        self.conn.delete_sourcetype_by_name("test_st")
        assert not self.conn.is_exist_sourcetype("test_st")


if __name__ == "__main__":
    unittest.main()
