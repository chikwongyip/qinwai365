# coding:utf-8
def extract_order(method, method_mode):
    sql_string = """
        create
        or replace transient table ods.crm.ods_t_extract_order_tmp as (
            select
                extract_order,
                method,
                method_mode
            from
                ods.crm.ods_t_crm_extract_original_data
            where
                is_proccessed = false
            and method = '{0}'
            and method_mode = '{1}'
        );  
    """.format(method, method_mode)
    return sql_string


def store(method, method_mode):
    sql_string = """
        create
        or replace transient table ods.crm.ods_t_store_tmp as (
           select
            TRY_CAST(VALUE:id::string as NUMBER) as ID,
            VALUE:store_id::string as STORE_ID,
            VALUE:creator_waiqin_id::string as CREATOR_WAIQIN_ID,
            VALUE:creator_name::string as CREATOR_NAME,
            VALUE:creator_id::string as CREATOR_ID,
            VALUE:return_pool_reason::string as RETURN_POOL_REASON,
            VALUE:store_name::string as STORE_NAME,
            VALUE:store_code::string as STORE_CODE,
            VALUE:store_manager::string as STORE_MANAGER,
            VALUE:store_manager_id::string as STORE_MANAGER_ID,
            VALUE:store_manager_waiqin365_id::string as STORE_MANAGER_WAIQIN365_ID,
            VALUE:store_type::string as STORE_TYPE,
            VALUE:store_type_code::string as STORE_TYPE_CODE,
            VALUE:store_type_id::string as STORE_TYPE_ID,
            VALUE:store_level_id::string as STORE_LEVEL_ID,
            VALUE:store_level::string as STORE_LEVEL,
            VALUE:store_dept_waiqin365_id::string as STORE_DEPT_WAIQIN365_ID,
            VALUE:store_dept_id::string as STORE_DEPT_ID,
            VALUE:store_dept_name::string as STORE_DEPT_NAME,
            VALUE:store_district::string as STORE_DISTRICT,
            VALUE:store_district_full_path::string as STORE_DISTRICT_FULL_PATH,
            TRY_CAST(
                VALUE:store_district_waiqin365_id::string as NUMBER
            ) as STORE_DISTRICT_WAIQIN365_ID,
            VALUE:store_third_district_id::string as STORE_THIRD_DISTRICT_ID,
            VALUE:store_mss_province::string as STORE_MSS_PROVINCE,
            VALUE:store_mss_province_code::string as STORE_MSS_PROVINCE_CODE,
            VALUE:store_mss_city::string as STORE_MSS_CITY,
            VALUE:store_mss_city_code::string as STORE_MSS_CITY_CODE,
            VALUE:store_mss_area::string as STORE_MSS_AREA,
            VALUE:store_mss_area_code::string as STORE_MSS_AREA_CODE,
            VALUE:store_mss_street::string as STORE_MSS_STREET,
            VALUE:store_mss_street_code::string as STORE_MSS_STREET_CODE,
            VALUE:store_addr::string as STORE_ADDR,
            VALUE:store_cooperate_status_id::string as STORE_COOPERATE_STATUS_ID,
            VALUE:store_cooperate_status::string as STORE_COOPERATE_STATUS,
            VALUE:store_ka_sys::string as STORE_KA_SYS,
            VALUE:store_tel::string as STORE_TEL,
            VALUE:store_fax::string as STORE_FAX,
            VALUE:store_post::string as STORE_POST,
            VALUE:store_remarks::string as STORE_REMARKS,
            VALUE:tradingarea_big::string as TRADINGAREA_BIG,
            VALUE:tradingarea::string as TRADINGAREA,
            VALUE:tradingarea_level_code::string as TRADINGAREA_LEVEL_CODE,
            VALUE:tradingarea_level_name::string as TRADINGAREA_LEVEL_NAME,
            VALUE:store_district_id::string as STORE_DISTRICT_ID,
            VALUE:store_district_code::string as STORE_DISTRICT_CODE,
            VALUE:store_district_create_time::string as STORE_DISTRICT_CREATE_TIME,
            VALUE:store_district_modify_time::string as STORE_DISTRICT_MODIFY_TIME,
            VALUE:store_district_creator_name::string as STORE_DISTRICT_CREATOR_NAME,
            VALUE:store_district_modifyier_name::string as STORE_DISTRICT_MODIFYIER_NAME,
            TRY_CAST(VALUE:store_district_status::string as INTEGER) as STORE_DISTRICT_STATUS,
            VALUE:store_rel_level_id::string as STORE_REL_LEVEL_ID,
            VALUE:store_label::string as STORE_LABEL,
            VALUE:store_label_id::string as STORE_LABEL_ID,
            VALUE:store_assistant_id::string as STORE_ASSISTANT_ID,
            VALUE:store_assistant_name::string as STORE_ASSISTANT_NAME,
            VALUE:store_road_msg::string as STORE_ROAD_MSG,
            VALUE:store_house_number::string as STORE_HOUSE_NUMBER,
            VALUE:store_liscence_name::string as STORE_LISCENCE_NAME,
            VALUE:store_registration_no::string as STORE_REGISTRATION_NO,
            VALUE:store_registration_date::string as STORE_REGISTRATION_DATE,
            VALUE:store_operator::string as STORE_OPERATOR,
            VALUE:store_sale_direct::string as STORE_SALE_DIRECT,
            VALUE:store_modify_time::string as STORE_MODIFY_TIME,
            VALUE:store_modifyier_name::string as STORE_MODIFYIER_NAME,
            VALUE:store_create_approval_time::string as STORE_CREATE_APPROVAL_TIME,
            VALUE:store_modify_approval_time::string as STORE_MODIFY_APPROVAL_TIME,
            VALUE:store_source_type::string as STORE_SOURCE_TYPE,
            VALUE:store_selling_area::string as STORE_SELLING_AREA,
            VALUE:store_cashiers_num::string as STORE_CASHIERS_NUM,
            VALUE:store_shelf_num::string as STORE_SHELF_NUM,
            VALUE:store_total_num::string as STORE_TOTAL_NUM,
            VALUE:store_self_product_num::string as STORE_SELF_PRODUCT_NUM,
            VALUE:store_open_time::string as STORE_OPEN_TIME,
            VALUE:store_close_time::string as STORE_CLOSE_TIME,
            VALUE:store_pictures::string as STORE_PICTURES,
            VALUE:store_liscence::string as STORE_LISCENCE,
            VALUE:store_manager_code::string as STORE_MANAGER_CODE,
            VALUE:store_creator_code::string as STORE_CREATOR_CODE,
            VALUE:store_modifier_code::string as STORE_MODIFIER_CODE,
            VALUE:store_delivery_addr::string as STORE_DELIVERY_ADDR,
            VALUE:linkmans::string as LINKMANS,
            VALUE:deliverys::string as DELIVERYS,
            VALUE:exts::string as VARIANT,
            VALUE:dealers::string as DEALERS,
            VALUE:autarky::string as AUTARKY,
            VALUE:store_approval_status::string as STORE_APPROVAL_STATUS,
            VALUE:store_status::string as STORE_STATUS,
            VALUE:create_time::string as CREATE_TIME,
            VALUE:store_receive_info::string as STORE_RECEIVER_INFO
        from
            ods.crm.ods_t_crm_extract_original_data
            inner join ods.crm.ods_t_extract_order_tmp on ods.crm.ods_t_crm_extract_original_data.method = ods.crm.ods_t_extract_order_tmp.method
            and ods.crm.ods_t_crm_extract_original_data.method_mode = ods.crm.ods_t_extract_order_tmp.method_mode
            and ods.crm.ods_t_crm_extract_original_data.extract_order = ods.crm.ods_t_extract_order_tmp.extract_order,
            lateral flatten(input => parse_json(extracted_result)) as data
        where
            is_proccessed = false
            and ods.crm.ods_t_crm_extract_original_data.method = '{0}'
            and ods.crm.ods_t_crm_extract_original_data.method_mode = '{1}'
        qualify
            row_number() over (
                partition by
                    value:id::string
                order by
                    value:store_modify_time::string desc
            ) = 1
        );
        """.format(method, method_mode)
    return sql_string


def store_ext(method, method_mode):
    sql_string = """
        create
        or replace transient table ods.crm.ods_t_store_exts_tmp as (
            select
                try_cast(data.value['id']::string as number) as id,
                exts.value['store_ext_column']::string as store_ext_column,
                exts.value['store_ext_key']::string as store_ext_key,
                exts.value['store_ext_value']::string as store_ext_value
            from
                ods.crm.ods_t_crm_extract_original_data
                inner join ods.crm.ods_t_extract_order_tmp on ods.crm.ods_t_crm_extract_original_data.method = ods.crm.ods_t_extract_order_tmp.method
                and ods.crm.ods_t_crm_extract_original_data.method_mode = ods.crm.ods_t_extract_order_tmp.method_mode
                and ods.crm.ods_t_crm_extract_original_data.extract_order = ods.crm.ods_t_extract_order_tmp.extract_order,
                lateral flatten(input => parse_json(extracted_result)) as data,
                lateral flatten(input => parse_json(data.value['exts'])) as exts
            where
                is_proccessed = false
                and ods.crm.ods_t_crm_extract_original_data.method = '{0}'
                and ods.crm.ods_t_crm_extract_original_data.method_mode = '{1}'
            qualify
                row_number() over (
                    partition by
                        data.value['id']::string,
                        exts.value['store_ext_column']::string
                    order by
                        data.value['store_modify_time']::string desc
                ) = 1
        );
    """.format(method, method_mode)
    return sql_string


def store_receiver(method, method_mode):
    sql_string = """
    create
    or replace transient table ods.crm.ods_t_store_map_receive_tmp as (
    select
        try_cast(data.value['id']::string as number) as id,
        data.value['store_id']::string as store_id,
        store_receive_info.value['store_waiqin_365_id']::string as dealer_id,
        store_receive_info.value['store_code']::string as dealer_name
    from
        ods.crm.ods_t_crm_extract_original_data
        inner join ods.crm.ods_t_extract_order_tmp on ods.crm.ods_t_crm_extract_original_data.method = ods.crm.ods_t_extract_order_tmp.method
            and ods.crm.ods_t_crm_extract_original_data.method_mode = ods.crm.ods_t_extract_order_tmp.method_mode
            and ods.crm.ods_t_crm_extract_original_data.extract_order = ods.crm.ods_t_extract_order_tmp.extract_order,
        lateral flatten(input => parse_json(extracted_result)) as data,
        lateral flatten(
            input => parse_json(data.value['store_receive_info'])
        ) as store_receive_info
    where
        is_proccessed = false
        and ods.crm.ods_t_crm_extract_original_data.method = '{0}'
        and ods.crm.ods_t_crm_extract_original_data.method_mode = '{1}'
    qualify
    row_number() over (
        partition by
            data.value['id']::string,
            store_receive_info.value['store_waiqin_365_id']::string
        order by
            data.value['store_modify_time']::string desc
        ) = 1
    );
    """.format(method, method_mode)
    return sql_string


def store_dealers(method, method_mode):
    sql_string = """
    create
    or replace transient table ods.crm.ods_t_store_map_dealer_tmp as (
        select
            try_cast(data.value['id']::string as number) as id,
            data.value['store_id']::string as store_id,
            dealers.value['dealer_id']::string as dealer_id,
            dealers.value['dealer_name']::string as dealer_name,
            dealers.value['waiqin365_dealer_id']::string as waiqin365_dealer_id,
            dealers.value['waiqin365_dealer_order']::string as waiqin365_dealer_order
        from
            ods.crm.ods_t_crm_extract_original_data
            inner join ods.crm.ods_t_extract_order_tmp on ods.crm.ods_t_crm_extract_original_data.method = ods.crm.ods_t_extract_order_tmp.method
            and ods.crm.ods_t_crm_extract_original_data.method_mode = ods.crm.ods_t_extract_order_tmp.method_mode
            and ods.crm.ods_t_crm_extract_original_data.extract_order = ods.crm.ods_t_extract_order_tmp.extract_order,
            lateral flatten(input => parse_json(extracted_result)) as data,
            lateral flatten(input => parse_json(data.value['dealers'])) as dealers
        where
            is_proccessed = false
            and ods.crm.ods_t_crm_extract_original_data.method = '{0}'
            and ods.crm.ods_t_crm_extract_original_data.method_mode = '{1}'
       qualify
        row_number() over (
            partition by
                data.value['id']::string,
                dealers.value['waiqin365_dealer_id']::string
            order by
                data.value['store_modify_time']::string desc
        ) = 1
    );
    """.format(method, method_mode)
    return sql_string


def dealer_info(method, method_mode):
    sql_string = """
    create
    or replace transient table ods.crm.ods_t_dealer_tmp as (
        select
            value:id::int as id,
            value:dealer_id::string as dealer_id,
            value:creator_waiqin_id::string as creator_waiqin_id,
            value:creator_name::string as creator_name,
            value:creator_id::string as creator_id,
            value:return_pool_reason::string as return_pool_reason,
            value:dealer_name::string as dealer_name,
            value:dealer_code::string as dealer_code,
            value:dealer_manager::string as dealer_manager,
            value:dealer_manager_waiqin365_id::string as dealer_manager_waiqin365_id,
            value:dealer_dept_id::string as dealer_dept_id,
            value:dealer_dept_name::string as dealer_dept_name,
            value:dealer_dept_waiqin365_id::string as dealer_dept_waiqin365_id,
            value:dealer_district::string as dealer_district,
            value:dealer_district_full_path::string as dealer_district_full_path,
            value:dealer_district_waiqin365_id::string as dealer_district_waiqin365_id,
            value:dealer_type::string as dealer_type,
            value:dealer_type_code::string as dealer_type_code,
            value:dealer_type_id::string as dealer_type_id,
            value:dealer_mss_province::string as dealer_mss_province,
            value:dealer_mss_province_code::string as dealer_mss_province_code,
            value:dealer_mss_city::string as dealer_mss_city,
            value:dealer_mss_city_code::string as dealer_mss_city_code,
            value:dealer_mss_area::string as dealer_mss_area,
            value:dealer_mss_dept_id::string as dealer_mss_dept_id,
            value:dealer_mss_dept_waiqin365_id::string as dealer_mss_dept_waiqin365_id,
            value:dealer_mss_dept_name::string as dealer_mss_dept_name,
            value:dealer_mss_area_code::string as dealer_mss_area_code,
            value:dealer_addr::string as dealer_addr,
            value:dealer_delivery_addr::string as dealer_delivery_addr,
            value:dealer_cooperate_status:string as dealer_cooperate_status,
            value:upper_dealer::string as upper_dealer,
            value:upper_dealer_id::string as upper_dealer_id,
            nullif(value:waiqin365_upper_dealer_id, '')::int as waiqin365_upper_dealer_id,
            value:dealer_source::string as dealer_source,
            value:dealer_trade::string as dealer_trade,
            value:dealer_scale::string as dealer_scale,
            value:dealer_tel::string as dealer_tel,
            value:dealer_fax::string as dealer_fax,
            value:dealer_post::string as dealer_post,
            value:dealer_remarks::string as dealer_remarks,
            value:dealer_cooperate_status_id::int as dealer_cooperate_status_id,
            value:dealer_level_id::string as dealer_level_id,
            value:dealer_level::string as dealer_level,
            value:tradingarea_level_name::string as tradingarea_level_name,
            value:tradingarea_level_code::string as tradingarea_level_code,
            value:dealer_approval_status::string as dealer_approval_status,
            value:tradingarea::string as tradingarea,
            value:create_time::string as create_time,
            value:dealer_district_id::string as dealer_district_id,
            value:dealer_district_code::string as dealer_district_code,
            value:dealer_pictures::string as dealer_pictures,
            value:dealer_district_create_time::string as dealer_district_create_time,
            value:dealer_district_modify_time::string as dealer_district_modify_time,
            value:dealer_district_creator_name::string as dealer_district_creator_name,
            value:dealer_district_modifyier_name::string as dealer_district_modifyier_name,
            value:dealer_district_status::string as dealer_district_status,
            value:dealer_mss_street::string as dealer_mss_street,
            value:dealer_mss_street_code::string as dealer_mss_street_code,
            value:dealer_label::string as dealer_label,
            value:dealer_label_id::string as dealer_label_id,
            value:dealer_assistant_id::string as dealer_assistant_id,
            value:dealer_liscence::string as dealer_liscence,
            value:dealer_assistant_name::string as dealer_assistant_name,
            value:dealer_road_msg::string as dealer_road_msg,
            value:dealer_house_number::string as dealer_house_number,
            value:dealer_liscence_name::string as dealer_liscence_name,
            value:dealer_registration_no::string as dealer_registration_no,
            value:dealer_credit_no::string as dealer_credit_no,
            value:dealer_registration_date::string as dealer_registration_date,
            value:dealer_operator::string as dealer_operator,
            value:dealer_sale_direct::string as dealer_sale_direct,
            value:dealer_creator_code::string as dealer_creator_code,
            value:dealer_modifier_code::string as dealer_modifier_code,
            value:dealer_manager_code::string as dealer_manager_code,
            value:dealer_status::string as dealer_status,
            value:exts::string as variant,
            value:linkmans::string as linkmans,
            value:deliverys::string as deliverys,
            value:stores::string as stores,
            value:dealer_receive_info::string as dealer_receive_info,
            value:tradingarea_big::string as tradingarea_big,
            value:dealer_third_district_id::string as dealer_third_district_id
        from
            ods.crm.ods_t_crm_extract_original_data
            inner join ods.crm.ods_t_extract_order_tmp on ods.crm.ods_t_crm_extract_original_data.method = ods.crm.ods_t_extract_order_tmp.method
            and ods.crm.ods_t_crm_extract_original_data.method_mode = ods.crm.ods_t_extract_order_tmp.method_mode
            and ods.crm.ods_t_crm_extract_original_data.extract_order = ods.crm.ods_t_extract_order_tmp.extract_order,
            lateral flatten(input => parse_json(extracted_result)) as data,
        where
            is_proccessed = false
            and ods.crm.ods_t_crm_extract_original_data.method = '{0}'
            and ods.crm.ods_t_crm_extract_original_data.method_mode = '{1}'
        qualify
            row_number() over (
                partition by
                    value:id::int
                order by
                    value:id::int
            ) = 1
    );
    """.format(method, method_mode)
    return sql_string


def visit_comment(method, method_mode):
    sql_string = """
        select
            data.value['task_name']::string as task_name,
            data.value['appraiser_source_code']::string as appraiser_source_code,
            data.value['appraise_time']::string as appraise_time,
            data.value['modifier_name']::string as modifier_name,
            data.value['modify_time']::string as modify_time,
            data.value['visitor_id']::string as visitor_id,
            data.value['appraiser_id']::string as appraiser_id,
            data.value['visitor_source_code']::string as visitor_source_code,
            data.value['visit_type']::string as visit_type,
            data.value['task_id']::string as task_id,
            data.value['appraise_form_id']::string as appraise_form_id,
            data.value['visitor_code']::string as visitor_code,
            data.value['appraise_record_id']::string as appraise_record_id,
            data.value['visit_use_time']::string as visit_use_time,
            data.value['sub_task_name']::string as sub_task_name,
            data.value['customer_code']::string as customer_code,
            data.value['leave_time']::string as leave_time,
            data.value['customer_source_code']::string as customer_source_code,
            data.value['appraiser_name']::string as appraiser_name,
            data.value['modifier_source_code']::string as modifier_source_code,
            data.value['visitor_name']::string as visitor_name,
            data.value['arrive_time']::string as arrive_time,
            data.value['arrive_pos_offset']::string as arrive_pos_offset,
            data.value['appraise_status']::string as appraise_status,
            data.value['appraise_use_time']::string as appraise_use_time,
            data.value['sub_task_id']::string as sub_task_id,
            data.value['modifier_id']::string as modifier_id,
            data.value['modifier_code']::string as modifier_code,
            data.value['appraiser_code']::string as appraiser_code,
            data.value['customer_name']::string as customer_name,
            data.value['visit_id']::string as visit_id,
            data.value['customer_id']::string as customer_id,
            data.value['visit_date']::string as visit_date,
            data.value['leave_pos_offset']::string as leave_pos_offset
        from
            ods.crm.ods_t_crm_extract_original_data,
            lateral flatten(input => parse_json(extracted_result)) as data
        where
            method = '{0}'
            and method_mode = '{1}'
            and is_proccessed = false;
    """.format(method, method_mode)
    return sql_string
