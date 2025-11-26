select
    *
from
    ods.crm.ods_t_crm_subtask_settings;

select distinct
    form_id
from
    ods.crm.ods_t_crm_promotions
where
    form_id <> '';

select distinct
    appraise_form_id
from
    ods.crm.ods_t_crm_visit_comment_record;

select distinct
    appraise_form_id
from
    ods.crm.ods_t_crm_visit_comment_record
union all
select distinct
    form_id
from
    ods.crm.ods_t_crm_promotions
where
    form_id <> '';

select
    *
from
    ods.crm.ods_t_crm_form_config
where
    form_id = '7016725360500847213';

select
    *
from
    ods.crm.ods_t_crm_form_data_slfdf_2507240001;

select
    *
from
    ods.crm.ods_t_crm_extract_original_data
where
    method = '/api/userDefined/v1/queryUserDefined-7016725360500847213';

select
    *
from
    ods.crm.ods_t_crm_extract_original_data as res,
    lateral flatten(input => parse_json(res.extracted_result)) as data,
    lateral flatten(input => parse_json(data.value['columns'])) as columns
where
    method like '/api/userDefined/v1/getUserDefined-7016725360500847213'
    and method_mode = 'CREATE'
    and data.value['table_name'] is not null
    and is_proccessed = false;

select
    *
from
    ods.sensetime.ods_t_ai_extract_original_data
where
    requesttype = 'bayRecognize'
order by
    extract_order desc;

select
    *
from
    common.utils.common_t_ai_delta_table
where
    requesttype = 'bayRecognize'
order by
    last_extract_date desc
limit
    100;

select
    *
from
    ods.sensetime.ods_t_ai_pog_data
limit
    100;

select
    *
from
    ods.crm.ods_v_crm_promotion_details
where
    cus_code = '9100002345';

select
    *
from
    ODS.CRM.ODS_T_CRM_PROMOTIONS_DETAILS;

select
    data_id
from
    ods.crm.ods_t_crm_promotions_comment;

select
    *
from
    api.tpm.api_v_tpm_promotion_upload;

select
    *
from
    ods.crm.ods_t_crm_extract_original_data
limit
    100;

select
    item.plan_code as activity_code,
    item.plan_name as activity_name,
    '' as activity_description,
    item.cm_receive_id as store_id,
    item.cm_receive_code as store_code,
    item.cm_receive_name as store_name,
    item.applicant_code as dsr_code,
    item.applicant as dsr_name,
    item.start_date as start_date,
    item.end_date as end_date,
    header.plan_type as promotion_activity_type,
    header.form_id as activity_form,
    item.exe_require as execution_requirements,
    item.remark,
    'BI_SYSTEM' as create_by,
    'BI_SYSTEM' as update_by,
    CURRENT_TIMESTAMP() as create_time,
    item.update_time
from
    ods.crm.ods_v_crm_promotion_details as item
    inner join ods.crm.ods_t_crm_promotions as header on item.plan_id = header.id
where
    store_id <> ''
    and activity_code = 'CX202509036086';

select
    *
from
    ods.crm.ods_t_crm_promotions
where
    plan_status = '4';

select
    *
from
    ODS.ECOM.ODS_T_ECOM_SALE_MATERIAL_LIST
where
    STORE is not null
    and STORE <> '0';

select
    *
from
    ods.ecom.ods_t_ecom_sales_getlist_header
where
    list_id = '1763515809000006';

select
    *
from
    ods.ecom.ods_t_ecom_sales_getlist_items
where
    list_id = '1763515809000006';

select
    *
from
    common.utils.common_t_ai_delta_table
where
    requesttype = 'bayRecognize'
order by
    last_extract_date desc;

select distinct
    form_id,
    table_name
from
    ods.crm.ods_v_crm_form_config
