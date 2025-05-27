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
-- visit_type 计划类型。1：常规拜访（拜访计划），2：临时拜访，3：线路拜访（线路计划），4：周期计划，5：检核计划，6：协访计划，7：临时检核，8：临时协访
-- flow_type 流程类型。1：拜访，2：车销，3：车铺
-- plan_source 流程类型。 0：手机端计划，1：新增计划，2：导入计划，3：按频率生成计划，4：按周生成计划，5：按线路生成计划，10：导入检核协访计划
-- approval_status 审批状态。0:未审批，1:已审批
-- visit_status拜访状态。0：未拜访，1：已拜访，2：失访
-- is_finished 必做流程是否完成。0：未完成，1：完成
-- arrive_rg_type 到店定位类型。1：GPS定位，2：基站定位
-- leave_rg_type 离店定位类型。1：GPS定位，2：基站定位
-- task_type 任务类型。1：拜访，2：协访，3：检核
select
    header.*,
    case
        when header.visit_type = '1' then '常规拜访（拜访计划）'
        when header.visit_type = '2' then '临时拜访'
        when header.visit_type = '3' then '线路拜访（线路计划）'
        when header.visit_type = '4' then '周期计划'
        when header.visit_type = '5' then '检核计划'
        when header.visit_type = '6' then '协访计划'
        when header.visit_type = '7' then '临时检核'
        when header.visit_type = '8' then '临时协访'
        else '其他拜访'
    end as visit_type_desc,
    -- case
    --     when header.flow_type = '1' then '拜访'
    --     when header.flow_type = '2' then '车销'
    --     when header.flow_type = '3' then '车铺'
    --     else '其他'
    -- end as flow_type_desc,
    -- case
    --     when header.plan_source = '0' then '手机端计划'
    --     when header.plan_source = '1' then '新增计划'
    --     when header.plan_source = '2' then '导入计划'
    --     when header.plan_source = '3' then '按频率生成计划'
    --     when header.plan_source = '4' then '按周生成计划'
    --     when header.plan_source = '5' then '按线路生成计划'
    --     when header.plan_source = '10' then '导入检核协访计划'
    --     else '其他'
    -- end as plan_source_desc,
    case
        when header.visit_status = '0' then '未审批'
        when header.visit_status = '1' then '已审批'
        else '其他'
    end as visit_status_desc,
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

select
    *
from
    ods.crm.ods_t_crm_extract_original_data
where
    method = '/api/cusVisit/v1/queryVisitApprovalByRecord';

select
    *
from
    ods.crm
where
    TABLE_NAME = 'EXAMPLE_TABLE';

select
    *
from
    dwd.crm.dwd_v_crm_send_to_crm_json_list;

select
    count(*)
from
    dwd.crm.dwd_v_crm_send_to_crm_json_list;

select
    last_extract_date
from
    common.utils.common_t_crm_delta_table
where
    method = '/api/cusVisit/v1/queryCusVisitDetail'
    and method_mode = 'VISIT'
order by
    last_extract_date desc
limit
    1;

select
    *
from
    ods.crm.ods_t_crm_subtask_settings
where
    function_id = '9178422631309639175';

select
    *
from
    ods.crm.ods_t_crm_subtask_content_slfdf_2503030006
where
    function_id = '9178422631309639175'
    and creator_id = '7257456688095924601'
    and create_time >= '2025-05-26 00:00:00'
limit
    100;

select distinct
    appraise_form_id
from
    ods.crm.ods_t_crm_visit_comment_record
limit
    100;

select
    split(res.method, '-') [1] as functing_id,
    value:id::string as id,
    value:visit_id::string as visit_id,
    value:visit_form_id::string as visit_form_id,
    value:visit_data_id::string as visit_data_id,
    value:extract_status::string as extract_status,
    value:creator_name::string as creator_name,
    value:create_time::string as create_time,
    value:approver_name::string as approver_name,
    value:approve_time::string as approve_time,
    value:approve_status::string as approve_status,
    value:approval_form_id::string as approval_form_id,
    value:approval_data_id::string as approval_data_id
from
    ods.crm.ods_t_crm_extract_original_data as res,
    lateral flatten(input => parse_json(res.extracted_result)) as data
where
    method like '/api/cusVisit/v1/getVisitRecordApprovalData%'
    and is_proccessed = false
    and approval_form_id = '7536223043262678236'
    and approval_data_id = '8317980140156477685'
    -- and visit_form_id = '7536223043262678236'
    -- and value:visit_data_id::string = '8317980140156477685'
limit
    100;

select
    value:pt::variant['id'] as id,
    value:pt::variant['source_code'] as source_code,
    value:pt::variant['status'] as status,
    value:pt::variant['modifyier_time'] as modifyier_time,
    value:pt::variant['create_time'] as create_time,
    value:pt::variant['modifyier_id'] as modifyier_id,
    value:pt::variant['creator_id'] as creator_id,
    value:pt::variant['create_time'],
    value:pt::variant['create_time'],
    value:pt::variant['create_time'],
from
    ods.crm.ods_t_crm_extract_original_data as res,
    lateral flatten(input => parse_json(res.extracted_result)) as data
where
    method = '/api/userDefined/v1/queryUserDefined-7536223043262678236'
    and method_mode = 'CREATE'
    and is_proccessed = false
limit
    100;

select
    split(res.method, '-') [1]::string as form_id,
    data.value['table_name']::string as table_name,
    data.value['description']::string as table_desc,
    columns.value['column_name']::string as column_name,
    columns.value['description']::string as col_desc,
    columns.value['type']::string as type,
    columns.value['select_option']::string as select_option,
    columns.value['sequence']::string as seq,
from
    ods.crm.ods_t_crm_extract_original_data as res,
    lateral flatten(input => parse_json(res.extracted_result)) as data,
    lateral flatten(input => parse_json(data.value['columns'])) as columns
where
    method like '/api/userDefined/v1/getUserDefined%'
    and method_mode = 'CREATE'
    and data.value['table_name'] is not null
    and is_proccessed = false;

create
or replace transient table ods.crm.ods_t_crm_form_config as (
    select
        split(res.method, '-') [1]::string as form_id,
        data.value['table_name']::string as table_name,
        data.value['description']::string as table_desc,
        columns.value['column_name']::string as column_name,
        columns.value['description']::string as col_desc,
        columns.value['type']::string as type,
        columns.value['select_option']::string as select_option,
        columns.value['sequence']::string as seq,
    from
        ods.crm.ods_t_crm_extract_original_data as res,
        lateral flatten(input => parse_json(res.extracted_result)) as data,
        lateral flatten(input => parse_json(data.value['columns'])) as columns
    where
        method like '/api/userDefined/v1/getUserDefined%'
        and method_mode = 'CREATE'
        and data.value['table_name'] is not null
        and is_proccessed = false
    qualify
        row_number() over (
            partition by
                split(res.method, '-') [1]::string,
                data.value['table_name']::string,
                columns.value['column_name']::string
            order by
                split(res.method, '-') [1]::string
        ) = 1
);

select
    split(res.method, '-') [1]::string as form_id,
    data.value['table_name']::string as table_name,
    data.value['description']::string as table_desc,
    columns.value['column_name']::string as column_name,
    columns.value['description']::string as col_desc,
    columns.value['type']::string as type,
    columns.value['select_option']::string as select_option,
    columns.value['sequence']::string as seq,
from
    ods.crm.ods_t_crm_extract_original_data as res,
    lateral flatten(input => parse_json(res.extracted_result)) as data,
    lateral flatten(input => parse_json(data.value['columns'])) as columns
where
    method like '/api/userDefined/v1/getUserDefined%'
    and method_mode = 'CREATE'
    and data.value['table_name'] is not null
    and is_proccessed = false
qualify
    row_number() over (
        partition by
            split(res.method, '-') [1]::string,
            data.value['table_name']::string,
            columns.value['column_name']::string
        order by
            split(res.method, '-') [1]::string
    ) = 1;

select
    split(res.method, '-') [1]::string as function_id,
    value:id::string as id,
    value:visit_id::string as visit_id,
    value:visit_form_id::string as visit_form_id,
    value:visit_data_id::string as visit_data_id,
    value:extract_status::string as extract_status,
    value:creator_name::string as creator_name,
    value:create_time::string as create_time,
    value:approver_name::string as approver_name,
    value:approve_time::string as approve_time,
    value:approve_status::string as approve_status,
    value:approval_form_id::string as approval_form_id,
    value:approval_data_id::string as approval_data_id
from
    ods.crm.ods_t_crm_extract_original_data as res,
    lateral flatten(input => parse_json(res.extracted_result)) as data
where
    method like '/api/cusVisit/v1/getVisitRecordApprovalData%'
    and is_proccessed = true
    and method_mode = 'CREATE';

select
    *
from
    ods.crm.ODS_T_CRM_TASK_COMMENT;

delete from ods.crm.ods_t_crm_extract_original_data
where
    method like '/api/userDefined/v1/getUserDefined%';