select 
	orderid
	,createdate
	,log
	,regexp_substr(log, '[0-9]{2}/[0-9]{2}/[0-9]{4}', 1, 1) as "original_eta"
	,regexp_substr(log, '[0-9]{2}/[0-9]{2}/[0-9]{4}', 1, 2) as "updated_eta"
	,coalesce(nullif("updated_eta", ''), "original_eta") as "eta"
from order_actions
where orderstatus = 1
	and (
		log ilike 'Added the ETA%'
		or log ilike 'Updated ETA%'
		or log ilike 'The ETA date is%'
		)
	and createdate > current_date - 99
order by 1,2
