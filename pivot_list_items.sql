with numbers as (
	select cast(row_number()over() as int) as "num"
	from orders
	limit 50 --number of list items never more than 20, overestimate for extreme anomalies
)
,split_items as ( --split list at commas, pivot list items
    select
		orderid
		,createdate
		,trim(split_part(itemlist, ',', "num")) as "item"
		,(length(itemlist) - length(replace(itemlist,',','')))+1 as "item_count"
	from orders
	cross join numbers
	where "num" <= "item_count"
        and orderstatus = 'completed'
        and department = 1
        and createdate > current_date - 99
)
--end ctes
select 
	"item"
	,count(distinct orderid) as "order_volume"
	,max(date(createdate)) as "last_create_date"
from split_items
group by 1
order by 1,2 desc
