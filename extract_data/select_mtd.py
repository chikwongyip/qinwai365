sql_str = """
    SELECT concat(
                '{"config": {"form_id": "6799631239040073070"}, "data": {"pt":',
                OBJECT_CONSTRUCT(*)::text,
                '}}'
            ) as record_json
        from (
                select a.source_code as "source_code",
                    '7396865232005886403' as "creator_id",
                    a.emp_name as "slfdf_2405040001" --DSR
    ,
                    '' as "slfdf_2405010006" --季度
    ,
                    '' as "slfdf_2405010007" -- 年度
    ,
                    a.calendar_year_month as "slfdf_2405010008" -- 月份
    ,
                    a.l4_employee_name as "slfdf_2405010009" -- 上级主管
    ,
                    '' as "slfdf_2405010010" -- 省份
    ,
                    '' as "slfdf_2405010011" -- 城市
    ,
                    to_varchar(update_time) as "slfdf_2405010012" -- 报表更新日期
    ,
                    a.source_code as "slfdf_2405010013" -- 唯一识别
    ,
                    zeroifnull(a.total_planned_visit_times) as "slfdf_2405010014" -- a.计划拜访次数
    ,
                    zeroifnull(a.total_planned_visited_times) as "slfdf_2405010015" -- b.成功拜访次数(计划)
    ,
                    zeroifnull(a.visit_planned_times_ach) as "slfdf_2405010016" -- c.拜访次数达成(计划)
    ,
                    zeroifnull(a.total_no_of_planned_store) as "slfdf_2405010017" -- d.计划拜访店数
    ,
                    zeroifnull(a.visit_planned_store_ach) as "slfdf_2405010018" -- e.拜访店数达成(计划)
    ,
                    zeroifnull(a.total_plan_active_store) as "slfdf_2405010019" -- f.计划门店活跃数
    ,
                    zeroifnull(a.active_store_percentage) as "slfdf_2405010020" -- g.计划门店活跃度%
    ,
                    zeroifnull(a.working_days) as "slfdf_2405010021" -- h.工作日
    ,
                    zeroifnull(a.time_consume) as "slfdf_2405010022" -- i.总拜访时间(分钟)
    ,
                    zeroifnull(a.avg_time_consume) as "slfdf_2405010023" -- j.工作日平均时间
    ,
                    zeroifnull(a.total_pecies) as "slfdf_2405010024" -- k.合标准数量
    ,
                    zeroifnull(a.total_no_of_chenlie_store) as "slfdf_2405010025" -- l.门店数
    ,
                    zeroifnull(a.target_sales)::DECIMAL(20, 2) as "slfdf_2405010026" -- m.目标
    ,
                    zeroifnull(a.actual_sales) as "slfdf_2405010027" -- n.实际
    ,
                    zeroifnull(a.sales_ach) as "slfdf_2405010028" -- o.完成率
    ,
                    zeroifnull(a.total_no_of_new_store) as "slfdf_2405010029" -- p.新门店數量
    ,
                    ZEROIFNULL(a.new_store_sales) as "slfdf_2405010030" -- q.新门店生意
    ,
                    zeroifnull(a.avg_new_store_sales) as "slfdf_2405010031" -- r.新门店 Average $
    ,
                    0 as "slfdf_2405010032" -- c.拜访完成率（全国）
    ,
                    0 as "slfdf_2405010033" -- e.计划完成率（全国）
    ,
                    0 as "slfdf_2405010034" -- g.计划门店活跃度%（全国）
    ,
                    zeroifnull(a.total_visited_no_of_planned_store) as "slfdf_2405010035" -- p.成功拜访店数(计划)
    ,
                    0 as "slfdf_2405010036" -- j.工作日平均时间（全国）
    ,
                    0 as "slfdf_2405010037" -- o.完成率（全国）
    ,
                    zeroifnull(a.e1_point) as "slfdf_2405080001" -- E1得分
    ,
                    zeroifnull(a.e2_point) as "slfdf_2405080002" -- E2得分
    ,
                    zeroifnull(a.e3_point) as "slfdf_2405080003" -- E3得分
    ,
                    zeroifnull(a.e4_point) as "slfdf_2405080004" -- E4得分
    ,
                    zeroifnull(a.s1_point) as "slfdf_2405080005" -- S1得分
    ,
                    zeroifnull(a.s2_point) as "slfdf_2405080006" -- S2得分
    ,
                    zeroifnull(a.total_point) as "slfdf_2405090002" -- 总得分
    ,
                    zeroifnull(a.total_visited_times) as "slfdf_2405090001" -- 成功拜访次数
    ,
                    sku_fenxiao_stores as "slfdf_2408210001" -- 货架分销合格店数
                from ads.crm.ads_v_dsr_visit_result_report as a
                WHERE a.update_time >= date_trunc(day, dateadd(day, -1, current_timestamp())) -- 
                -- AND a.l4_employee_name = '周娟娟'
                
            )
"""
