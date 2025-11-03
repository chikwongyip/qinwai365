select
    *
from
    ods.crm.ods_t_crm_subtask_settings;

select
    *
from
    ods.crm.ods_t_crm_extract_original_data
where
    method like '/api/userDefined/v1/getUserDefined-7016725360500847213';

select
    *
from
    ods.crm.ods_t_crm_form_data_slfdf_2401240002;

select distinct
    function_id,
    table_name
from
    ods.crm.ods_t_crm_subtask_settings
where
    function_id = '';

select
    value:id::string,
    value:item_names::string,
    value:form_id::string,
    value:flow_status::string,
    value:next_user_names::string,
    value:assign_budget::string,
    value:auditor_name::string,
    value:budget_freeze_node::string,
    value:budget_name::string,
    value:code::string,
    value:description::string,
    value:execute_cycle::string,
    value:exts::string,
    value:pay_type::string,
    value:pd_range_type::string,
    value:plan_amount::string,
    value:plan_name::string,
    value:plan_status::string,
    value:plan_type::string,
    value:predict_effective_ratio::string,
    value:predict_sale_amount::string,
    value:project_code::string,
    value:project_name::string,
    value:project_status::string,
from
    ods.crm.ods_t_crm_extract_original_data,
    lateral flatten(input => parse_json(extracted_result)) as data
where
    method = '/api/cuxiao/v1/queryRegularSale'
    and is_proccessed = false;

select
    data.value:id::string as id,
    data.value:item_names::string as item_names,
    data.value:form_id::string as form_id,
    data.value:flow_status::string as flow_status,
    data.value:next_user_names::string as next_user_names,
    data.value:assign_budget::string as assign_budget,
    data.value:auditor_name::string as auditor_name,
    data.value:budget_freeze_node::string as budget_freeze_node,
    data.value:budget_name::string as budget_name,
    data.value:code::string as code,
    data.value:description::string as description,
    data.value:execute_cycle::string as execute_cycle,
    data.value:exts::string as exts,
    data.value:pay_type::string as pay_type,
    data.value:pd_range_type::string as pd_range_type,
    data.value:plan_amount::string as plan_amount,
    data.value:plan_name::string as plan_name,
    data.value:plan_status::string as plan_status,
    data.value:plan_type::string as plan_type,
    data.value:predict_effective_ratio::string as predict_effective_ratio,
    data.value:predict_sale_amount::string as predict_sale_amount,
    data.value:project_code::string as project_code,
    data.value:project_name::string as project_name,
    data.value:project_status::string as project_status,
    -- 其他常用字段（来自你的 JSON 示例）
    data.value:applyCode::string as apply_code,
    data.value:applyId::string as apply_id,
    data.value:planCode::string as plan_code,
    data.value:planId::string as plan_id,
    data.value:planName::string as plan_name_display,
    data.value:status::string as activity_status,
    data.value:subList::string as sublist_json -- 保留原始嵌套 JSON
from
    ods.crm.ods_t_crm_extract_original_data,
    lateral FLATTEN(input => PARSE_JSON(extracted_result)) as data
where
    method = '/api/cuxiao/v1/queryRegularSaleActivities'
    and is_proccessed = false;

select
    data.value:id::string as id,
    data.value:item_names::string as item_names,
    data.value:form_id::string as form_id,
    data.value:flow_status::string as flow_status,
    data.value:next_user_names::string as next_user_names,
    data.value:assign_budget::string as assign_budget,
    data.value:auditor_name::string as auditor_name,
    data.value:budget_freeze_node::string as budget_freeze_node,
    data.value:budget_name::string as budget_name,
    data.value:code::string as code,
    data.value:description::string as description,
    data.value:execute_cycle::string as execute_cycle,
    data.value:exts::string as exts,
    data.value:pay_type::string as pay_type,
    data.value:pd_range_type::string as pd_range_type,
    data.value:plan_amount::string as plan_amount,
    data.value:plan_name::string as plan_name,
    data.value:plan_status::string as plan_status,
    data.value:plan_type::string as plan_type,
    data.value:predict_effective_ratio::string as predict_effective_ratio,
    data.value:predict_sale_amount::string as predict_sale_amount,
    data.value:project_code::string as project_code,
    data.value:project_name::string as project_name,
    data.value:project_status::string as project_status
from
    ods.crm.ods_t_crm_extract_original_data,
    lateral FLATTEN(input => PARSE_JSON(extracted_result)) as data
where
    method = '/api/cuxiao/v1/queryRegularSale'
    and is_proccessed = false;

select
    data.value:id::string,
    data.value:activityStatus::string,
    data.value:applicant::string,
    data.value:applicantCode::string,
    data.value:applicantId::string,
    data.value:applicantSourceCode::string,
    data.value:applyCode::string,
    data.value:applyId::string,
    data.value:applyName::string,
    data.value:applyTime::string,
    data.value:auditTime::string,
    data.value:budgetId::string,
    data.value:extColumns::string,
    data.value:formDataId::string,
    data.value:formDefinedId::string,
    data.value:modifyTime::string,
    data.value:parentApplyCode::string,
    data.value:planCode::string,
    data.value:planId::string,
    data.value:planName::string,
    data.value:projectApplyCode::string,
    data.value:projectCode::string,
    data.value:projectName::string,
    data.value:status::string,
    data.value:subList::string
from
    ods.crm.ods_t_crm_extract_original_data,
    lateral FLATTEN(input => PARSE_JSON(extracted_result)) as data
where
    method = '/api/cuxiao/v1/queryRegularSaleActivities'
    and is_proccessed = false;

select
    *
from
    ods.crm.ods_t_crm_promotions
where
    id = '6702798636354987901';

select
    a.*,
    sub_list.value:applyAmount::string,
    sub_list.value:cmDeptIds::string,
    sub_list.value:cmDeptNames::string,
    sub_list.value:cmDistrictIds::string,
    sub_list.value:cmDistrictNames::string,
    sub_list.value:cmKaSys::string,
    sub_list.value:cmKaSysIds::string,
    sub_list.value:cmManagerCode::string,
    sub_list.value:cmManagerDept::string,
    sub_list.value:cmManagerId::string,
    sub_list.value:cmManagerName::string,
    sub_list.value:cmManagerSourceCode::string,
    sub_list.value:cmQualifiedStatus::string,
    sub_list.value:cmReceiveAddrId::string,
    sub_list.value:cmReceiveAddrName::string,
    sub_list.value:cmReceiveCode::string,
    sub_list.value:cmReceiveId::string,
    sub_list.value:cmReceiveName::string,
    sub_list.value:cmReceiveSourceCode::string,
    sub_list.value:cmTypeIds::string,
    sub_list.value:cmTypeNames::string,
    sub_list.value:cusCode::string,
    sub_list.value:cusId::string,
    sub_list.value:cusName::string,
    sub_list.value:cusSourceCode::string,
    sub_list.value:dealerCode::string,
    sub_list.value:cmTypeIds::string,
    sub_list.value:cmTypeNames::string,
    sub_list.value:cusCode::string,
    sub_list.value:cusId::string,
    sub_list.value:cusName::string,
    sub_list.value:cusSourceCode::string,
    sub_list.value:dealerCode::string,
    sub_list.value:dealerName::string,
    sub_list.value:dealerSourceCode::string,
    sub_list.value:dimInfoList::string,
    sub_list.value:dimJson::string,
    sub_list.value:endDate::string,
    sub_list.value:exeRequire::string,
    sub_list.value:extColumns::string,
    sub_list.value:giftCode::string,
    sub_list.value:giftId::string,
    sub_list.value:giftName::string,
    sub_list.value:giftNum::string,
    sub_list.value:giftPrice::string,
    sub_list.value:giftSourceCode::string,
    sub_list.value:giftUnitId::string,
    sub_list.value:giftUnitName::string,
    sub_list.value:inputNum::string,
    sub_list.value:inputStandard::string,
    sub_list.value:itemCostStd::string,
    sub_list.value:itemName::string,
    sub_list.value:joinCmNum::string,
    sub_list.value:payType::string,
    sub_list.value:pdBrandsName::string,
    sub_list.value:pdCodes::string,
    sub_list.value:pdIds::string,
    sub_list.value:pdNames::string,
    sub_list.value:pdRangeType::string,
    sub_list.value:pdSale::string,
    sub_list.value:pdSalesAmount::string,
    sub_list.value:pdSalesVolume::string,
    sub_list.value:pdSourceCodes::string,
    sub_list.value:pdTypesName::string,
    sub_list.value:pdUnitId::string,
    sub_list.value:pdUnitName::string,
    sub_list.value:period::string,
    sub_list.value:promotionDays::string,
    sub_list.value:remark::string,
    sub_list.value:rowNo::string,
    sub_list.value:settlementOrgCode::string,
    sub_list.value:settlementOrgId::string,
    sub_list.value:settlementOrgName::string,
    sub_list.value:settlementOrgSourceCode::string,
    sub_list.value:status::string,
    sub_list.value:subId::string,
    sub_list.value:subject::string,
    sub_list.value:subjectCode::string
from
    ods.crm.ods_t_crm_promotions_details as a,
    lateral flatten(input => (parse_json(sub_list))) as sub_list;

select
    -- Extract fields from the 'data' array inside the JSON response
    v.value:id::string as id,
    v.value:activityStatus::string as activity_status,
    v.value:applicant::string as applicant,
    v.value:applicantCode::string as applicant_code,
    v.value:applicantId::string as applicant_id,
    v.value:applicantSourceCode::string as applicant_source_code,
    v.value:applyCode::string as apply_code,
    v.value:applyId::string as apply_id,
    v.value:applyName::string as apply_name,
    v.value:applyTime::string as apply_time, -- Better type
    v.value:auditTime::string as audit_time,
    v.value:budgetId::string as budget_id,
    v.value:extColumns::string as ext_columns,
    v.value:formDataId::string as form_data_id,
    v.value:formDefinedId::string as form_defined_id,
    v.value:modifyTime::string as modify_time,
    v.value:parentApplyCode::string as parent_apply_code,
    v.value:planCode::string as plan_code,
    v.value:planId::string as plan_id,
    v.value:planName::string as plan_name,
    v.value:projectApplyCode::string as project_apply_code,
    v.value:projectCode::string as project_code,
    v.value:projectName::string as project_name,
    v.value:status::string as status,
    v.value:subList::string as sub_list -- Keep as string if nested
from
    ods.crm.ods_t_crm_extract_original_data t,
    lateral FLATTEN(
        input => PARSE_JSON(t.extracted_result) -- Key fix: access .data array
    ) v
where
    t.method = '/api/cuxiao/v1/queryRegularSaleActivities'
    and t.is_proccessed = false;

select
    a.*,
    sub_list.value:applyAmount::STRING as apply_amount,
    sub_list.value:cmDeptIds::STRING as cm_dept_ids,
    sub_list.value:cmDeptNames::STRING as cm_dept_names,
    sub_list.value:cmDistrictIds::STRING as cm_district_ids,
    sub_list.value:cmDistrictNames::STRING as cm_district_names,
    sub_list.value:cmKaSys::STRING as cm_ka_sys,
    sub_list.value:cmKaSysIds::STRING as cm_ka_sys_ids,
    sub_list.value:cmManagerCode::STRING as cm_manager_code,
    sub_list.value:cmManagerDept::STRING as cm_manager_dept,
    sub_list.value:cmManagerId::STRING as cm_manager_id,
    sub_list.value:cmManagerName::STRING as cm_manager_name,
    sub_list.value:cmManagerSourceCode::STRING as cm_manager_source_code,
    sub_list.value:cmQualifiedStatus::STRING as cm_qualified_status,
    sub_list.value:cmReceiveAddrId::STRING as cm_receive_addr_id,
    sub_list.value:cmReceiveAddrName::STRING as cm_receive_addr_name,
    sub_list.value:cmReceiveCode::STRING as cm_receive_code,
    sub_list.value:cmReceiveId::STRING as cm_receive_id,
    sub_list.value:cmReceiveName::STRING as cm_receive_name,
    sub_list.value:cmReceiveSourceCode::STRING as cm_receive_source_code,
    sub_list.value:cmTypeIds::STRING as cm_type_ids,
    sub_list.value:cmTypeNames::STRING as cm_type_names,
    sub_list.value:cusCode::STRING as cus_code,
    sub_list.value:cusId::STRING as cus_id,
    sub_list.value:cusName::STRING as cus_name,
    sub_list.value:cusSourceCode::STRING as cus_source_code,
    sub_list.value:dealerCode::STRING as dealer_code,
    sub_list.value:dealerName::STRING as dealer_name,
    sub_list.value:dealerSourceCode::STRING as dealer_source_code,
    sub_list.value:dimInfoList::STRING as dim_info_list,
    sub_list.value:dimJson::STRING as dim_json,
    sub_list.value:endDate::STRING as end_date,
    sub_list.value:exeRequire::STRING as exe_require,
    sub_list.value:extColumns::STRING as item_ext_columns,
    sub_list.value:giftCode::STRING as gift_code,
    sub_list.value:giftId::STRING as gift_id,
    sub_list.value:giftName::STRING as gift_name,
    sub_list.value:giftNum::STRING as gift_num,
    sub_list.value:giftPrice::STRING as gift_price,
    sub_list.value:giftSourceCode::STRING as gift_source_code,
    sub_list.value:giftUnitId::STRING as gift_unit_id,
    sub_list.value:giftUnitName::STRING as gift_unit_name,
    sub_list.value:inputNum::STRING as input_num,
    sub_list.value:inputStandard::STRING as input_standard,
    sub_list.value:itemCostStd::STRING as item_cost_std,
    sub_list.value:itemName::STRING as item_name,
    sub_list.value:joinCmNum::STRING as join_cm_num,
    sub_list.value:payType::STRING as pay_type,
    sub_list.value:pdBrandsName::STRING as pd_brands_name,
    sub_list.value:pdCodes::STRING as pd_codes,
    sub_list.value:pdIds::STRING as pd_ids,
    sub_list.value:pdNames::STRING as pd_names,
    sub_list.value:pdRangeType::STRING as pd_range_type,
    sub_list.value:pdSale::STRING as pd_sale,
    sub_list.value:pdSalesAmount::STRING as pd_sales_amount,
    sub_list.value:pdSalesVolume::STRING as pd_sales_volume,
    sub_list.value:pdSourceCodes::STRING as pd_source_codes,
    sub_list.value:pdTypesName::STRING as pd_types_name,
    sub_list.value:pdUnitId::STRING as pd_unit_id,
    sub_list.value:pdUnitName::STRING as pd_unit_name,
    sub_list.value:period::STRING as period,
    sub_list.value:promotionDays::STRING as promotion_days,
    sub_list.value:remark::STRING as remark,
    sub_list.value:rowNo::STRING as row_no,
    sub_list.value:settlementOrgCode::STRING as settlement_org_code,
    sub_list.value:settlementOrgId::STRING as settlement_org_id,
    sub_list.value:settlementOrgName::STRING as settlement_org_name,
    sub_list.value:settlementOrgSourceCode::STRING as settlement_org_source_code,
    sub_list.value:status::STRING as item_status,
    sub_list.value:subId::STRING as sub_id,
    sub_list.value:subject::STRING as subject,
    sub_list.value:subjectCode::STRING as subject_code
from
    ods.crm.ods_t_crm_promotions_details as a,
    lateral FLATTEN(input => PARSE_JSON(a.sub_list)) as sub_list;

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
    store_id <> '';