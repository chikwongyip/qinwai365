# coding:utf-8
from extract_data import extract_data, extract_data_visit, extract_data_form
from get_last_update_time import get_last_extract_time
from config import snowflake_prd_config
import datetime


def extract_handler(path, method_mode, form_id=None, function_id=None):
    page_number = 1
    extract_form = False
    if form_id:
        lv_path = path + '-' + str(form_id)
        extract_form = True
    elif function_id:
        lv_path = path + '-' + str(function_id)
        extract_form = True
    else:
        lv_path = path
    # 获取上次更新的时间戳
    after_modify_date = get_last_extract_time(
        snowflake_prd_config, method=lv_path, method_mode=method_mode)

    print('上次更新时间戳为：{0}'.format(after_modify_date))
    # 获取当前执行时间
    before_modify_date = datetime.datetime.now()

    print('当前更新时间戳为：{0}'.format(before_modify_date))
    if extract_form:
        after_modify_date = after_modify_date.strftime('%Y-%m-%d')
        before_modify_date = before_modify_date.strftime('%Y-%m-%d')
    else:
        after_modify_date = after_modify_date.strftime('%Y-%m-%d %H:%M:%S')
        before_modify_date = before_modify_date.strftime('%Y-%m-%d %H:%M:%S')
    go_on = True
    # 获取当前时间戳
    while go_on:
        print('正在获取第{0}页数据'.format(page_number))
        go_on = extract_data(path=path, page_number=page_number,
                             after_modify_date=after_modify_date,
                             method_mode=method_mode, before_modify_date=before_modify_date,
                             form_id=form_id, function_id=function_id)
        page_number += 1


def extract_handler_visit(path, method_mode):
    page_number = 1
    # 获取上次更新的时间戳
    after_modify_date = get_last_extract_time(
        snowflake_prd_config, method=path, method_mode=method_mode).strftime('%Y-%m-%d %H:%M:%S')
    # print(after_modify_date)

    print('上次更新时间戳为：{0}'.format(after_modify_date))
    # 获取当前执行时间
    before_modify_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    print('当前更新时间戳为：{0}'.format(before_modify_date))
    go_on = True
    # 获取当前时间戳
    while go_on:
        print('正在获取第{0}页数据'.format(page_number))
        go_on = extract_data_visit(path=path, page_number=page_number, start_date=after_modify_date,
                                   method_mode=method_mode, end_date=before_modify_date)
        page_number += 1


def extract_handle_form_config(path, method_mode, form_id):
    res = extract_data_form(
        path=path, method_mode=method_mode, form_id=form_id)
    return res


# if __name__ == '__main__':
#     path = '/api/userDefined/v1/getUserDefined'
#     method_mode = 'CREATE'
#     extract_handle_form_config(
#         path=path, method_mode=method_mode, form_id='7016725360500847213')
