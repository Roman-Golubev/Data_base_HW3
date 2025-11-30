create table customer_new (
	customer_id integer primary key,
	first_name text,
	last_name text,
	gender text,
	DOB date,
	job_title text,
	job_industry_category text,
	wealth_segment text,
	deceased_indicator text,
	owns_car text,
	address text,
	postcode integer,
	state text,
	country text,
	property_valuation integer
);

create table product_new (
	product_id integer,
	brand text,
	product_line text,
	product_class text,
	product_size text,
	list_price real,
	standard_cost real
);


create table product_cor as
	select *
	from (
 	select *
	,row_number() over(partition by product_id order by list_price desc) as rn
	from product_new)
	where rn = 1;

create table orders (
	order_id integer primary key,
	customer_id integer,
	order_date date,
	online_order text,
	order_status text
);

create table order_items (
	order_item_id integer primary key,
	order_id integer,
	product_id integer,
	quantity real,
	item_list_price_at_sale real,
	item_standard_cost_at_sale real
);


--Запрос 1
select job_industry_category, count(job_industry_category) as cnt_job_industry_category
from customer_new
group by job_industry_category
order by cnt_job_industry_category desc;


--Запрос 2
select date_trunc('month', ord.order_date) as order_month, 
	job_industry_category,
	sum(pd.list_price * oi.quantity) as total_income
from customer_new cs
join (select * from orders where order_status = 'Approved') ord on cs.customer_id = ord.customer_id
join order_items oi on ord.order_id = oi.order_id 
join product_cor pd on oi.product_id = pd.product_id
group by order_month, job_industry_category
order by order_month, job_industry_category;


--Запрос 3
select pd.brand, 
	sum(case when cs.job_industry_category = 'IT' and ord.online_order = 'True' then 1 else 0 end) as cnt_IT_online_orders
from customer_new cs
join (select distinct order_id, customer_id, online_order from orders where order_status = 'Approved') ord on cs.customer_id = ord.customer_id
join order_items oi on ord.order_id = oi.order_id 
join product_cor pd on oi.product_id = pd.product_id
group by pd.brand;


--Запрос 4
--используя только group by
select cs.customer_id,
	sum(pd.list_price * oi.quantity) as total_income,
	max(pd.list_price * oi.quantity) as max_order_price,
	min(pd.list_price * oi.quantity) as min_order_price,
	count(ord.order_id) as cnt_orders,
	avg(pd.list_price * oi.quantity) as avg_order_price
from customer_new cs
join orders ord on cs.customer_id = ord.customer_id
join order_items oi on ord.order_id = oi.order_id 
join product_cor pd on oi.product_id = pd.product_id
group by cs.customer_id
order by total_income desc, cnt_orders desc; 
--используя только оконные функции
select subquery.customer_id, subquery.total_income, subquery.max_order_price,
	subquery.min_order_price, subquery.cnt_orders, subquery.avg_order_price
from (
	select cs.customer_id,
		sum(pd.list_price * oi.quantity) over(w) as total_income,
		max(pd.list_price * oi.quantity) over(w) as max_order_price,
		min(pd.list_price * oi.quantity) over(w) as min_order_price,
		count(ord.order_id) over(w) as cnt_orders,
		avg(pd.list_price * oi.quantity) over(w) as avg_order_price,
		row_number() over(w) as rn 
	from customer_new cs
	join orders ord on cs.customer_id = ord.customer_id
	join order_items oi on ord.order_id = oi.order_id 
	join product_cor pd on oi.product_id = pd.product_id
	window w as (partition by cs.customer_id)
) as subquery
where subquery.rn = 1
order by subquery.total_income desc, subquery.cnt_orders desc; 
--результаты для каждого customer_id в обоих вариантах получаются одинаковыми,
--скрипт запроса с оконной функцией получился сложнее,
--потому что понадобилось писать вложенный select-запрос


--Запрос 5
select *
from (
	select cs.customer_id, cs.first_name, cs.last_name, 
		sum(case when ord.order_id is not null then pd.list_price * oi.quantity else 0 end) as total_income
	from customer_new cs
	left join orders ord on cs.customer_id = ord.customer_id
	left join order_items oi on ord.order_id = oi.order_id 
	left join product_cor pd on oi.product_id = pd.product_id
	group by cs.customer_id	
	order by total_income	
	limit 3
)
union all
select *
from (
	select cs.customer_id, first_name, last_name, 
		sum(pd.list_price * oi.quantity) as total_income
	from customer_new cs
	join orders ord on cs.customer_id = ord.customer_id
	join order_items oi on ord.order_id = oi.order_id 
	join product_cor pd on oi.product_id = pd.product_id
	group by cs.customer_id	
	order by total_income desc	
	limit 3
);


--Запрос 6
select subquery.order_id, subquery.customer_id, subquery.order_date,
	subquery.online_order, subquery.order_status
from (
	select *,
		row_number() over(partition by customer_id order by order_date) as rn
	from orders
) as subquery
where subquery.rn = 2; 


--Запрос 7
select subquery.first_name, subquery.last_name, subquery.job_title, 
	max(subquery.days_between_orders) max_days_between_orders
from (
	select first_name, last_name, job_title,
		order_date - lag(order_date) over(partition by cs.customer_id order by order_date) days_between_orders,
		row_number() over(partition by cs.customer_id) as rn
	from customer_new cs
	join orders ord on cs.customer_id = ord.customer_id
) as subquery
where subquery.rn >= 2
group by subquery.first_name, subquery.last_name, subquery.job_title;


--Запрос 8
select subquery.first_name, subquery.last_name, subquery.wealth_segment, subquery.total_income
from (
	select cs.first_name, cs.last_name, cs.wealth_segment,
		sum(pd.list_price * oi.quantity) as total_income,
		row_number() over(partition by cs.wealth_segment order by sum(pd.list_price * oi.quantity) desc) as rn
	from customer_new cs
	join orders ord on cs.customer_id = ord.customer_id
	join order_items oi on ord.order_id = oi.order_id 
	join product_cor pd on oi.product_id = pd.product_id
	group by cs.customer_id	
) as subquery
where subquery.rn <= 5;
