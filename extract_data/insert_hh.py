from request_qince_api import Qince_API
from create_session import create_session
from config import snowflake_prd_config
from select_data import select_hh, select_row


# def insert_form(row):

#     res = Qince_API('/api/userDefined/v1/addUserDefined',
#                     snowflake_prd_config, row['body']).request_data()
#     print(res)


def update_form(row):
    res = Qince_API('/api/userDefined/v1/modifyUserDefined',
                    snowflake_prd_config, row['body']).request_data()
    if res.get('return_code', None) == 1:
        res = Qince_API('/api/userDefined/v1/addUserDefined',
                        snowflake_prd_config, row['body']).request_data()
    print(res)


def construct_json(row, session):
    dict_data = eval(row['HEADER'])
    sql_str = select_row(row['L4_EMPLOYEE_ID'])
    rows = session.sql(sql_str).collect()
    array_rows = []
    for row in rows:
        dict_row = {
            'slfdf_2405050010': row.slfdf_2405050010,
            'slfdf_2405050011': row.slfdf_2405050011
        }
        array_rows.append(dict_row)

    res = {
        'config': {
            'form_id': '5207296376560712404'
        },
        'data': {
            'pt': dict_data,

            'sts': [
                {
                    'id': '6160890033950357989',
                    'table_name': 'slfdf_2405050007',
                    'rows': array_rows
                }
            ]
        },
    }

    return res


if __name__ == '__main__':
    session = create_session(snowflake_prd_config)
    sql_str = select_hh()
    df = session.sql(sql_str).to_pandas()
    df['body'] = df.apply(construct_json, axis=1, args=[session,])
    df.apply(update_form, axis=1)
