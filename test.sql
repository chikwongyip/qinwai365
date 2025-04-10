select
    task_id,
    sub_task_id,
    visit_id,
    customer_code,
    customer_id
from
    ods.crm.ods_t_crm_visit_comment_record
where
    sub_task_id = '9178422631309639175';

select
    *
from
    ods.crm.ods_t_crm_subtask_settings
where
    function_id = '9178422631309639175'
limit
    100;

select
    *
from
    ods.crm.ods_t_crm_subtask_content_slfdf_2503030006
where
    visit_implement_id = '4761343198861095962';

-- ods.crm.ods_t_crm_subtask_content_slfdf_2503030006
select
    header.*,
    items.*,
    settings.columns
from
    ods.crm.ods_t_crm_visit_comment_record as header
    inner join ods.crm.ods_t_crm_subtask_settings as settings on header.sub_task_id = settings.function_id
    inner join IDENTIFIER (
        'ods.crm.ods_t_crm_subtask_content_slfdf_2503030006'
    ) as items on header.visit_id = items.visit_implement_id
    and settings.function_id = items.function_id
where
    header.visit_id = '4761343198861095962';