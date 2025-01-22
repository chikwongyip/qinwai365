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
    ) = 1;

select
    store_manager,
    store_code
from
    ODS.CRM.ODS_T_STORE as a
where
    STORE_CODE = '9100479263';
