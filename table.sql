create or replace table ods.crm.ods_t_crm_extract_original_data (
    extract_order int autoincrement start 1 increment 1 comment '抽取单号',
    method varchar comment '接口',
    method_mode varchar comment '接口模式',
    extract_start_timestamp number(38, 0) comment '数据抽取开始时间戳',
    extract_end_timestamp number(38, 0) comment '数据抽取结束时间戳',
    extract_start_date TIMESTAMP_NTZ(9) comment '数据抽取开始时间',
    extract_end_date TIMESTAMP_NTZ(9) comment '数据抽取结束时间',
    extract_condition text comment '数据抽取的条件（除时间外）',
    page_size number(38, 0) comment '抽取每页条数',
    page_no number(38, 0) comment '总页数',
    total_size number(38, 0) comment '总条数',
    page_seq number(38, 0) comment '第N页',
    cost_time number(38, 0) comment '总用时',
    extracted_result variant comment '抽取结果数据',
    is_proccessed boolean comment '是否已处理',
    is_success boolean comment '是否成功'
) comment = '数据抽取记录';
