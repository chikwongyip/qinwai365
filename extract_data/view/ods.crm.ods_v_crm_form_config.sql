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
    'slfdf_2509110001_pic' as column_name;

alter table ods.crm.ods_t_crm_form_data_slfdf_2507240001
add column slfdf_2511251203078873_pic string;

alter table ods.crm.ods_t_crm_form_data_slfdf_2507240001
add column slfdf_2509110001_pic string;
