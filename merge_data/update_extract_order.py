# coding:utf-8

def update_extract_order(method, method_mode):
    sql_string = """
    update ods.crm.ods_t_crm_extract_original_data as data
        set
            is_proccessed = true
        from
            ods.crm.ods_t_extract_order_tmp as tmp
        where
            data.extract_order = tmp.extract_order
            and data.method = '{0}'
            and data.method_mode = '{1}';
    """.format(method, method_mode)
    return sql_string
