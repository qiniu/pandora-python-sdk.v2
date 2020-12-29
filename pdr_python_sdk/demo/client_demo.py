import os
from pdr_python_sdk.client import *
from pdr_python_sdk.manager import SearchManager

"""
Demo connection params

params = {
    "scheme": "http",
    "host": "pandora-express-rc.qiniu.io",
    "port": 80,
    "token": "xxxxx"
}
"""
import yaml
f = open("config.yaml", 'r')
params = yaml.load(f, Loader=yaml.FullLoader)
f.close()
conn = connect(**params)


def data_upload_demo():
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
    print(json.dumps(data))
    conn.data_upload_strictly(data=data, repo="default", sourcetype="json")
    # 上传前不会做参数检验
    conn.data_upload(data=data, repo="default", sourcetype="json")


def search_events_demo():
    """
    搜索型SPL demo
    """
    # 查看前端错误的仓库内数据
    # fast 模式只计算内置字段
    # smart 模式只计算搜索的数据中出现的字段
    # detailed 模式计算所有字段的统计信息
    query_info = conn.create_query_job(
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
        last_status = conn.get_query_status(query_id)
        if last_status['process'] == 1:
            break
        print("current status is {} ".format(last_status))
        time.sleep(0.1)
    print("final status is {} ".format(last_status))
    # 搜索型SPL
    assert not last_status['isResult']
    events = conn.get_query_events(query_id)
    print("events count is {} ".format(events['total']))
    print("events rows are {} ".format(events['rows']))

    timeline = conn.get_query_timeline(query_id)
    print("timeline buckets are {} ".format(timeline['buckets']))

    summary = conn.get_query_summary(query_id)
    print("summary of fields statistics info is {}".format(summary))


def search_stats_demo():
    """
    计算型搜索demo
    """
    # 查看前一小时前端错误
    # fast 模式只计算内置字段
    # smart 模式只计算搜索的数据中出现的字段
    # detailed 模式计算所有字段的统计信息
    query_info = conn.create_analysis_job(
        """ repo=_frontend_error | stats count() as cnt""",
        start=0,
        end=int(time.time() * 1000),
        collectSize=10000
    )
    # 从返回结果中获得query_id
    query_id = query_info['id']
    # 轮询获得搜索任务是否成功
    while True:
        last_status = conn.get_query_status(query_id)
        if last_status['process'] == 1:
            break
        print("current status is {} ".format(last_status))
        time.sleep(0.1)
    print("final status is {} ".format(last_status))
    # 计算型SPL
    assert last_status['isResult']
    results = conn.get_query_results(query_id)
    print("result fields are {} ".format(results['fields']))
    print("result rows are {} ".format(results['rows']))


def repo_management_demo():
    """
    仓库管理相关demo，不使用分层存储
    """
    # 创建仓库。不使用分层存储
    conn.create_repo("testrepo_by_sdk", lifeCycleEnable=False)

    # 获得仓库列表
    repos = conn.get_repos(prefix="testrepo_by_sdk", pageSize=5, pageNo=1)
    print(repos)

    # 获取单个仓库配置
    config = conn.get_repo_by_name("testrepo_by_sdk")

    config["description"] = "to update by sdk"

    # 如果不启动分层存储，需要手动把分层策略从配置中删除
    if not config['lifecyclePolicyEnable']:
        config.pop("lifecyclePolicy")

    # 更新仓库配置
    conn.update_repo_by_body("testrepo_by_sdk", config)
    # 删除仓库
    conn.delete_repo_by_name("testrepo_by_sdk")

    # 创建仓库。使用分层存储，分层存储时间单位为毫秒
    # 7天热存储
    # 30天温存储
    # 365天冷存储
    conn.create_repo("testlayer_by_sdk", lifeCycleEnable=True, lifeCycle={
        "hot": 7 * 24 * 60 * 60 * 1000,
        "warm": 30 * 24 * 60 * 60 * 1000,
        "cold": 365 * 24 * 60 * 60 * 1000
    })
    conn.delete_repo_by_name("testlayer_by_sdk")


def query_mapping_demo():
    """
    获得SPL计算结果的字段类型信息
    :return:
    """
    print(conn.get_query_mapping("repo=_frontend_error"))


def search_manager_demo():
    """
    使用high level的搜索接口
    :return:
    """
    sm = SearchManager(conn)
    print(sm.query("repo=_frontend_error", start=0, end=int(time.time() * 1000)))


def search_manager_pandas_demo():
    """
    使用high level的搜索接口
    :return:
    """
    sm = SearchManager(conn)
    df = sm.query_to_pandas(
        "repo=_frontend_error | stats count() by origin",
        start=0,
        end=int(time.time() * 1000),
        collectSize=10
    )
    print(df)


if __name__ == "__main__":
    data_upload_demo()
    search_stats_demo()
    search_stats_demo()
    repo_management_demo()
    query_mapping_demo()
    search_manager_demo()
    search_manager_pandas_demo()
