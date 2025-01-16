select
    *
from
    (
        select
            tmp.id,
            tmp.store_code,
            store.id as id_2
        from
            ods.crm.ods_t_store_tmp as tmp
            left join ods.crm.ods_t_store as store on tmp.id = store.id
    )
where
    id_2 is null;

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
);

update ods.crm.ods_t_crm_extract_original_data as data
set
    is_proccessed = true
from
    ods.crm.ods_t_extract_order_tmp as tmp
where
    data.extract_order = tmp.extract_order
    and data.method = tmp.method
    and data.method_mode = tmp.method_mode;

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
        and method = '{0}'
        and method_mode = '{1}'
    qualify
        row_number() over (
            partition by
                value:id::string
            order by
                value:id::string
        ) = 1
);

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
);
