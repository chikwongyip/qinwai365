select
    data.value['task_name']::string as task_name,
    data.value['appraiser_source_code']::string as appraiser_source_code,
    data.value['appraise_time']::string as appraise_time,
    data.value['modifier_name']::string as modifier_name,
    data.value['modify_time']::string as modify_time,
    data.value['visitor_id']::string as visitor_id,
    data.value['appraiser_id']::string as appraiser_id,
    data.value['visitor_source_code']::string as visitor_source_code,
    data.value['visit_type']::string as visit_type,
    data.value['task_id']::string as task_id,
    data.value['appraise_form_id']::string as appraise_form_id,
    data.value['visitor_code']::string as visitor_code,
    data.value['appraise_record_id']::string as appraise_record_id,
    data.value['visit_use_time']::string as visit_use_time,
    data.value['sub_task_name']::string as sub_task_name,
    data.value['customer_code']::string as customer_code,
    data.value['leave_time']::string as leave_time,
    data.value['customer_source_code']::string as customer_source_code,
    data.value['appraiser_name']::string as appraiser_name,
    data.value['modifier_source_code']::string as modifier_source_code,
    data.value['visitor_name']::string as visitor_name,
    data.value['arrive_time']::string as arrive_time,
    data.value['arrive_pos_offset']::string as arrive_pos_offset,
    data.value['appraise_status']::string as appraise_status,
    data.value['appraise_use_time']::string as appraise_use_time,
    data.value['sub_task_id']::string as sub_task_id,
    data.value['modifier_id']::string as modifier_id,
    data.value['modifier_code']::string as modifier_code,
    data.value['appraiser_code']::string as appraiser_code,
    data.value['customer_name']::string as customer_name,
    data.value['visit_id']::string as visit_id,
    data.value['customer_id']::string as customer_id,
    data.value['visit_date']::string as visit_date,
    data.value['leave_pos_offset']::string as leave_pos_offset
from
    ods.crm.ods_t_crm_extract_original_data,
    lateral flatten(input => parse_json(extracted_result)) as data
where
    method = '/api/cusVisit/v1/queryVisitApprovalByRecord'
    and method_mode = 'VISIT'
    and is_proccessed = false
    -- and data.value['customer_name']::string = '煊超市（金贸时代）'
    and data.value['task_id']::string in ('4898075112474268309')
    and data.value['sub_task_id'] in ('9178422631309639175', '4950163845790505522');

select
    *
from
    ods.crm.ODS_T_CRM_VISIT_COMMENT_RECORD
limit
    100;
