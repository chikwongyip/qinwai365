from request_qince_api import Qince_API
from config import snowflake_prd_config
from create_session import create_session
import json
SQL_STR = """
select
    a.id,
    a.store_id as store_code,
    b.store_name,
    iff(gd_latitude = 'None', baidu_latitude, gd_latitude) as latitude,
    iff(
        gd_longitude = 'None',
        baidu_longitude,
        gd_longitude
    ) as longitude
from
    ods.mdm.ods_t_mdm_cn_store as a
    inner join ods.crm.ods_t_store as b on a.store_id = b.store_code
where
  (
        a.gd_latitude <> 'None'
        or a.baidu_latitude <> 'None'
    )
    and
    a.store_id in (
        '9102063650',
        '9102063655',
        '9102063652',
        '9102063657',
        '9102063656',
        '9102063651',
        '9102063653',
        '9102063658',
        '9102063635',
        '9102063583',
        '9102063564',
        '9102063557',
        '9102063560',
        '9102063576',
        '9102063562',
        '9102063581',
        '9102063572',
        '9102063566',
        '9102063585',
        '9102063574',
        '9102063586',
        '9102063554',
        '9102063551',
        '9102063561',
        '9102063556',
        '9102063587',
        '9102063565',
        '9102063575',
        '9102063573',
        '9102063552',
        '9102063582',
        '9102063578',
        '9102063570',
        '9102063580',
        '9102063571',
        '9102063588',
        '9102063559',
        '9102063558',
        '9102063563',
        '9102063569',
        '9102063579',
        '9102063584',
        '9102063443',
        '9102063612',
        '9102063599',
        '9102063594',
        '9102063618',
        '9102063593',
        '9102063590',
        '9102063624',
        '9102063619',
        '9102063591',
        '9102063629',
        '9102063600',
        '9102063598',
        '9102063621',
        '9102063602',
        '9102063627',
        '9102063592',
        '9102063617',
        '9102063601',
        '9102063622',
        '9102063603',
        '9102063626',
        '9102063604',
        '9102063605',
        '9102063614',
        '9102063613',
        '9102063620',
        '9102063597',
        '9102063595',
        '9102063547',
        '9102063546',
        '9102063549',
        '9102063544',
        '9102063543',
        '9102063550',
        '9102063545',
        '9102063541',
        '9102063548',
        '9102063540',
        '9102063513',
        '9102063499',
        '9102063497',
        '9102063496',
        '9102063495',
        '9102063498',
        '9102063493',
        '9102063494',
        '9102063492',
        '9102063488',
        '9102063448',
        '9102063482',
        '9102063445',
        '9102063478',
        '9102063484',
        '9102063483',
        '9102063481',
        '9102063480',
        '9102063479',
        '9102063446',
        '9102063447',
        '9102063444',
        '9102063425',
        '9102063426',
        '9102063424',
        '9102063423',
        '9102063393',
        '9102063408',
        '9102063362',
        '9102063382',
        '9102063386',
        '9102063383',
        '9102063384',
        '9102063381',
        '9102063385',
        '9102063346',
        '9102063350',
        '9102063351',
        '9102059484',
        '9102059280',
        '9102059017',
        '9102059305',
        '9102059347',
        '9100497236',
        '9100290510',
        '9100441852'
    ) ;
"""


def query_id(store_code: str):
    dict_data = {
        "store_code": store_code
    }
    res = Qince_API('/api/store/v1/queryStore',
                    snowflake_prd_config, dict_data).request_data()
    # print(json.loads(res['response_data'])[0]['id'])
    return json.loads(res['response_data'])[0]['id']


if __name__ == '__main__':
    session = create_session(snowflake_prd_config)
    df = session.sql(SQL_STR).to_pandas()
    data_list = df.to_dict(orient='records')
    # print(data_list)
    for item in data_list:
        id = query_id(item['STORE_CODE'])
        # print(id)
        dict_data = {
            "store_waiqin_id": id,
            "store_code": item['STORE_CODE'],
            "store_location": str(item["LATITUDE"] + ',' + str(item["LONGITUDE"]))

        }
        # print(dict_data)
        res = Qince_API('/api/store/v1/modifyStore',
                        snowflake_prd_config, dict_data).request_data()
        print(res)
