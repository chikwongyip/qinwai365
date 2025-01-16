create
or replace transient table ods.crm.ods_t_store_map_dealers_tmp as (
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
        and ods.crm.ods_t_crm_extract_original_data.method = '/api/store/v1/queryStore'
        and ods.crm.ods_t_crm_extract_original_data.method_mode = 'MODIFY'
    qualify
        row_number() over (
            partition by
                data.value['id']::string,
                dealers.value['waiqin365_dealer_id']::string
            order by
                data.value['id']::string,
                dealers.value['waiqin365_dealer_id']::string
        ) = 1
);

select
    *
from
    ods.crm.ods_t_store_map_dealers_tmp;