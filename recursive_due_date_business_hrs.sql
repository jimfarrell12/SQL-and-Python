create temp table calendar as --manually load federal holidays in temp
	select 
		date(dateadd(day,1000,current_date)-row_number()over()) as "date"
		,case 
			when "date" in (
				'2021-01-01','2021-01-18','2021-02-15','2021-05-31','2021-06-19','2021-07-05','2021-09-06','2021-10-11','2021-11-11','2021-11-25','2021-12-24'--2021
				,'2022-01-01','2022-01-17','2022-02-21','2022-05-30','2022-06-19','2022-07-04','2022-09-05','2022-10-10','2022-11-11','2022-11-24','2022-12-26'--2022
				,'2023-01-02','2023-01-16','2023-02-20','2023-05-29','2023-06-19','2023-07-04','2023-09-04','2023-10-09','2023-11-10','2023-11-23','2023-12-25'--2023
				,'2024-01-01','2024-01-15','2024-02-19','2024-05-27','2024-06-19','2024-07-04','2024-09-02','2024-10-14','2024-11-11','2024-11-28','2024-12-25'--2024
				,'2025-01-01','2025-01-20','2025-02-17','2025-05-26','2025-06-19','2025-07-04','2025-09-01','2025-10-13','2025-11-11','2025-11-27','2025-12-25'--2025
				,'2026-01-01','2026-01-19','2026-02-16','2026-05-25','2026-06-19','2026-07-03','2026-09-07','2026-10-12','2026-11-11','2026-11-26','2026-12-25'--2026
				,'2027-01-01','2027-01-18','2027-02-15','2027-05-31','2027-06-19','2027-07-05','2027-09-06','2027-10-11','2027-11-11','2027-11-25','2027-12-24'--2027
				,'2028-01-01','2028-01-17','2028-02-21','2028-05-29','2028-06-19','2028-07-04','2028-09-04','2028-10-09','2028-11-10','2028-11-23','2028-12-25'--2028
				)
			then 1
			else 0
			end as "is_holiday"
	from workorders
	limit 2000;

create temp table wos as 
    select 
        workordernumber
        ,orderpriority
        ,createdate
        ,completedate
    from workorders
    where orderstatus = 'completed'
        and department = 1
        and createdate > current_date - 999

create temp table sla_windows as 
	select 
		orderpriority
	--priority
		,case 
			when orderpriority = 'Emergency' then 'P1'
			when orderpriority = 'Next Day' then 'P2'
			when orderpriority = 'Normal Service' then 'P3'
			when orderpriority = 'Non-Critical' then 'P4'
			end as "priority"
		,case "priority"
			when 'P1' then 1/24::float
			when 'P2' then 1
			when 'P3' then 2
			when 'P4' then 3
			end as "sla_window_days"
	--rename with window
		,case when "sla_window_days" < 3 then "sla_window_days"*24||' hrs' else "sla_window_days"||' days' end as "sla_priority_name"
        ,case when "is_biz_hrs" = 0 or "sla_window_days" < 1 then "sla_window_days"*24 else "sla_window_days"*"biz_hrs_in_day" end as "sla_window_hrs"
	--biz hrs
		,case when "sla_window_days" < 1 then 1 else 0 end as "is_biz_hrs"
		,8 as "biz_start"
		,17 as "biz_end"
		,"biz_end" - "biz_start" as "biz_hrs_in_day"
	from (select distinct orderpriority from wos)
	order by 2,1;

create temp table sla_startdates as 
	with timestamps_sla as (
		select 
			ordernumber
			,orderpriority
            		,createdate
            		,completedate
		--slas
			,"is_biz_hrs"
			,case when "is_biz_hrs" = 0 or "sla_window_days" < 1 then "sla_window_days"*24 else "sla_window_days"*"biz_hrs_in_day" end as "sla_window_hrs"
		from wos w
		left join sla_windows sw on sw.orderpriority = w.orderpriority
		--order by 1,2
	)
	,timestamps_adj as (
		select *
		--adjust clock start for ahw, weekends
			,case
				when "is_biz_hrs" = 0 then createdate
			--friday after close -> monday
				when extract(dow from createdate) = 5 
				and extract(hour from createdate) >= 17 then dateadd(hour,8,dateadd(day,3,date_trunc('day',createdate)))
			--saturday -> monday
				when extract(dow from createdate) = 6 then dateadd(hour,8,dateadd(day,2,date_trunc('day',createdate)))
			--sunday or after close -> next day
				when extract(dow from createdate) = 0 
				or extract(hour from createdate) >= 17 then dateadd(hour,8,dateadd(day,1,date_trunc('day',createdate)))
			--before start -> same day
				when extract(hour from createdate) < 8 then dateadd(hour,8,date_trunc('day',createdate))
				else createdate
				end as "clockstart"
		from timestamps_sla
	)
	,timestamps_adj_holidays as (
		select 
			ta.*
		--apply second adjustment for holidays
			,case
				when "is_biz_hrs" = 0 then "clockstart"
			--friday holiday -> monday
				when "is_holiday" = 1 and extract(dow from "clockstart") = 5 then dateadd(hour,8,dateadd(day,3,date_trunc('day',"clockstart")))
			--holiday -> next day
				when "is_holiday" = 1 then dateadd(hour,8,dateadd(day,1,date_trunc('day',"clockstart")))
				else "clockstart"
				end as "clockstart_sla"
		from timestamps_adj ta
		left join calendar c on c."date" = date("clockstart")
	)
	select * from timestamps_adj_holidays;


with recursive duedates (
	ordernumber
	,orderpriority
	,"sla_priority"
	,"sla_window_days"
	,"sla_window_hrs"
	,"is_biz_hrs"
	,createdate
	,"clockstart_sla"
	,"due_date"
	,"window_hrs") as (
--start
	select 
		ordernumber
		,orderpriority
		,"sla_priority"
		,"sla_window_days"
		,"sla_window_hrs"
		,"is_biz_hrs"
		,createdate
	--adjusted clock start
		,"clockstart_sla"
	--start due date
		,"clockstart_sla" as "due_date"
	--window for exit
		,"sla_window_hrs" as "window_hrs"
	from sla_startdates
--loop
	union all 
	select
		ordernumber
		,orderpriority
		,"sla_priority"
		,"sla_window_days"
		,"sla_window_hrs"
		,"is_biz_hrs"
		,createdate
		,"clockstart_sla"
	--increment date: logic assumes holidays do not occur on consecutive business days
		,case
			when "is_biz_hrs" = 0 then dateadd(hour,1,"due_date")
		--friday after close -> monday holiday -> tuesday
			when extract(dow from dateadd(hour,1,"due_date")) = 5
			and extract(hour from dateadd(hour,1,"due_date")) >= 17
			and threedays."is_holiday" = 1
			then dateadd(hour,88,"due_date")
		--friday after close -> monday
			when extract(dow from dateadd(hour,1,"due_date")) = 5
			and extract(hour from dateadd(hour,1,"due_date")) >= 17
			then dateadd(hour,64,"due_date")
		--thursday after close -> friday holiday -> monday
			when extract(dow from dateadd(hour,1,"due_date")) = 4
			and extract(hour from dateadd(hour,1,"due_date")) >= 17
			and tomorrow."is_holiday" = 1
			then dateadd(hour,88,"due_date")
		--after close -> next day holiday -> day after
			when extract(hour from dateadd(hour,1,"due_date")) >= 17
			and tomorrow."is_holiday" = 1
			then dateadd(hour,40,"due_date")
		--after close -> next day
			when extract(hour from dateadd(hour,1,"due_date")) >= 17
			then dateadd(hour,16,"due_date")
		--standard 1 hr
			else dateadd(hour,1,"due_date")
			end as "due_date"
	--decrement hrs
		,"window_hrs"-1 as "window_hrs"
	from duedates dd
	left join calendar tomorrow on tomorrow."date" = date(dateadd(hour,16,"due_date"))
	left join calendar threedays on threedays."date" = date(dateadd(hour,64,"due_date"))
	where "window_hrs" > 0
)
--end loop
select * 
from duedates
where "window_hrs" = 0
order by 1,2
