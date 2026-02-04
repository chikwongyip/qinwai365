create or replace view ods.crm.ods_v_crm_form_config as
select
    form_id,
    column_name
from
    ods.crm.ods_t_crm_form_config
union all
select
    '7016725360500847213' as form_id,
    'slfdf_2507240002_pic' as column_name
union all
select
    '7016725360500847213' as form_id,
    'creator_dept_id' as column_name
union all
select
    '7016725360500847213' as form_id,
    'creator_dept_name' as column_name
union all
select
    '7016725360500847213' as form_id,
    'slfdf_2511251203078873_pic' as column_name
union all
select
    '7016725360500847213' as form_id,
    'slfdf_2509110001_pic' as column_name
union all
select
    '7016725360500847213' as form_id,
    'slfdf_2508290001_pic' as column_name;

-- union all
-- select
--     '7016725360500847213' as form_id,
--     'slfdf_2509030001_pic' as column_name;
-- slfdf_2508290001_pic;
alter table ods.crm.ods_t_crm_form_data_slfdf_2507240001
add column slfdf_2511251203078873_pic string;

alter table ods.crm.ods_t_crm_form_data_slfdf_2507240001
add column slfdf_2509110001_pic string;

alter table ods.crm.ods_t_crm_form_data_slfdf_2507240001
add column slfdf_2508290001_pic string;

alter table ods.crm.ods_t_crm_form_data_slfdf_2507240001
add column slfdf_2509030001_pic string;

alter table ods.crm.ods_t_crm_form_data_slfdf_2507240001
add column slfdf_2509030001 string;

-- alter table ods.crm.ods_t_crm_form_data_slfdf_2507240001
-- drop column SLFDF_250903000_PIC ;
select
    SLFDF_250903000_PIC
from
    ods.crm.ods_t_crm_form_data_slfdf_2507240001
limit
    10;

select
    a.task_id,
    a.workflow_run_id,
    a.data_id,
    a.display_image_url_list,
    a.pic_type,
    -- a.data,
    parse_json(a.data) ['created_at'] as created_at,
    parse_json(a.data) ['elapsed_time'] as elapsed_time,
    parse_json(a.data) ['finished_at'] as finished_at,
    parse_json(a.data) ['id'] as id,
    parse_json(a.data) ['outputs'] as outputs,
    parse_json(a.data) ['status'] as status,
    parse_json(a.data) ['total_steps'] as total_steps,
    parse_json(a.data) ['total_tokens'] as total_tokens,
    parse_json(a.data) ['workflow_id'] as workflow_id,
    parse_json(parse_json(a.data) ['outputs'] ['text']) ['key_reason'] as key_reason,
    parse_json(parse_json(a.data) ['outputs'] ['text']) ['match_status'] as match_status
from
    ods.dify.ods_t_dify_sfa_image_analysis_result as a;

select
    SLFDF_250903000_PIC
from
    ods.crm.ods_v_crm_form_config
where
    form_id = '7016725360500847213';