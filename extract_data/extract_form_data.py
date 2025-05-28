# coding: utf-8
from extract_handler import extract_handler
from create_session import create_session
from config import snowflake_prd_config


def get_form_id(session):
    query = """
        select distinct
            appraise_form_id
        from
        ods.crm.ods_t_crm_visit_comment_record;
    """
    res = session.sql(query).collect()
    return res


if __name__ == '__main__':
    path = '/api/userDefined/v1/queryUserDefined'
    method_mode = 'CREATE'
    session = create_session(snowflake_prd_config)
    form_lists = get_form_id(session=session)
    for form_list in form_lists:
        form_id = form_list[0]
        extract_handler(
            path=path, method_mode=method_mode, form_id=form_id)
