select
	o_orderpriority,
	count(*) as order_count
from
	orders as o
where
	exists (
		select
			*
		from
			lineitem
		where
			l_orderkey = o.o_orderkey
			and l_commitdate < l_receiptdate
	)
group by
	o_orderpriority
order by
	o_orderpriority
LIMIT 1000
