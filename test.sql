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