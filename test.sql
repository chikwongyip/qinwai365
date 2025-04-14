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
    method = '/api/cusVisit/v1/queryVisitApprovalByRecord'