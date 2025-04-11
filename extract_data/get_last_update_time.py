# coding:utf-8
import datetime
import snowflake.connector


def get_last_extract_time(config: dict, **kwargs):
    conn = snowflake.connector.connect(
        user=config['user'], password=config['password'], account=config['account'])
    method = kwargs.get('method')
    method_mode = kwargs.get('method_mode')
    res = (
        conn.cursor()
        .execute(
            "select dateadd(day, -1, last_extract_date) as last_extract_date from common.utils.common_t_crm_delta_table where method = '{0}' and method_mode = '{1}' order by last_extract_date desc limit 1;".format(
                method, method_mode)
        )
        .fetchall()
    )
    conn.close()
    last_extract_time = res[0][0] if res else datetime.datetime.strptime(
        "2025-4-10 00:00:00", "%Y-%m-%d %H:%M:%S")

    # last_extract_time 减 10分钟
    last_extract_time = last_extract_time - datetime.timedelta(minutes=10)

    # last_extract_time 转字符串
    return last_extract_time
