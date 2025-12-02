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
    ods.crm.ods_v_crm_form_config;

select
    *
from
    ODS.ECOM.ODS_V_ECOM_SALES_MATERIAL_LIST
where
    salesmaterial = 'hkju-04061093'
limit
    100;

select
    *
from
    api.tpm.api_v_tpm_sale_material;

select
    item.id,
    item.plan_code as activity_code,
    item.plan_name as activity_name,
    header.description as activity_description,
    item.cus_id as store_id,
    item.cus_Code as store_code,
    item.cus_name as store_name,
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
    item.update_time,
    item.apply_code,
    item.item_name,
    -- item.*
from
    ods.crm.ods_v_crm_promotion_details as item
    inner join ods.crm.ods_t_crm_promotions as header on item.plan_id = header.id
where
    store_id <> ''
    and activity_status = '1'
    and item.plan_name <> 'null'
    -- and activity_code = 'TPM006'
    and item.item_status = '1';

select
    *
from
    ods.crm.ods_t_crm_promotions
limit
    100;

select
    *
from
    ods.crm.ods_t_crm_promotions_details
where
    id = '8701000965073516875';

-- 提供给经销商业务助手小程序--门店清单
select
    "勤策门店ID" as store_id,
    "门店编码" as store_code,
    "门店名称" as store_name,
    b.store_pictures as store_photos,
    "门店类型编码" as store_type_code,
    "门店类型名称-中英文" as store_type_name_cn_en,
    "门店类型名称-中文" as store_type_name_cn,
    '' as store_type_name_en,
    '' as l1_code,
    '' as l1_name,
    '' as l2_code,
    '' as l2_name,
    '' as l3_code,
    '' as l3_name,
    '' as l4_code,
    '' as l4_name,
    "DSR姓名" as dsr_name,
    "DSR类型" as dsr_type,
    "DSR ID" as dsr_id,
    "DSR编码" as dsr_code,
    "客户系统编码" as customer_system_code,
    "客户系统名称" as customer_system_name,
    "省份" as province,
    "城市" as city,
    "详细地址" as address,
    "合作状态" as cooperation_status,
    "所在好来地区" as darlie_area,
    "经销商编码" as dealer_code,
    "经销商名称" as dealer_name,
    "门店分类" as store_category,
    "纬度" as latitude,
    "经度" as longitude,
    "标注位置" as marked_location,
    "标注区" as marked_district,
    "标注城市" as marked_city,
    "标注省份" as marked_province,
    "是否已标注" as is_marked,
    '' as nielsen_store_code,
    "门店渠道" as store_channel,
    "渠道分类" as channel_category,
    '' as first_sales_month,
    '' as avg_sales_half_year,
    '' as visit_tag,
    "拜访标签2" as visit_tag2,
    '' as pg_name,
    '' as is_assigned_dsr,
    '' as has_pg,
    current_timestamp() as create_time,
    current_timestamp() as update_time
from
    ads.crm.ads_t_auto_route_customer_list a
    left join ods.crm.ods_t_store b on a."勤策门店ID" = b.id
where
    a."合作状态" not in ('0', '4')
    and "门店名称" not like '%作废%';

select
    *
from
    api.crm.api_v_crm_mini_program_store
where
    store_id is not null;