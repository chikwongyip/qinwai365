create
or replace TRANSIENT TABLE ods.crm.ods_t_crm.extract_original_data (
    extract_order int autoincrement start 1 increment 1 comment '抽取单号',
    method text comment '接口',
    method_mode text comment '接口模式',
    extract_start_timestamp number comment '数据抽取开始时间戳',
    extract_end_timestamp number comment '数据抽取结束时间戳',
    extract_start_date timestamp_ntz comment '数据抽取开始时间',
    extract_end_date timestamp_ntz comment '数据抽取结束时间',
    extract_condition text comment '数据抽取的条件（除时间外）',
    page_size number comment '抽取每页条数',
    page_no number comment '总页数',
    total_size number comment '总条数',
    page_seq number comment '第N页',
    cost_time number comment '总用时',
    extracted_result variant comment '抽取结果数据',
    is_proccessed boolean comment '是否已处理',
    is_success boolean comment '是否成功'
) comment = '数据抽取记录';