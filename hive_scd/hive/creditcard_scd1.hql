
-- create a temp table to hold data
create temporary table store_stage.creditcard_temp as 
select * from store.creditcard where 1 = 0;

-- copy records where matching keys but details have changed
insert into store_stage.creditcard_temp
select a.creditcardid, a.cardtype, a.cardnumber, a.expmonth, a.expyear, from_unixtime(a.modifieddate) modifieddate, current_date() from store_stage.creditcard a 
join store.creditcard b on a.creditcardid = b.creditcardid 
where a.cardtype != b.cardtype or
	a.cardnumber != b.cardnumber or
	a.cardnumber != b.cardnumber or
	a.expmonth != b.expmonth or
	a.expyear != b.expyear

-- copy records where matching keys but details have not changed
insert into store_stage.creditcard_temp
select a.creditcardid, a.cardtype, a.cardnumber, a.expmonth, a.expyear, from_unixtime(a.modifieddate) modifieddate, current_date() from store_stage.creditcard a 
join store.creditcard b on a.creditcardid = b.creditcardid 
where a.cardtype = b.cardtype and
	a.cardnumber = b.cardnumber and
	a.cardnumber = b.cardnumber and
	a.expmonth = b.expmonth and
	a.expyear = b.expyear

-- copy new records into temp table
insert into store_stage.creditcard_temp
select a.creditcardid, a.cardtype, a.cardnumber, a.expmonth, a.expyear, from_unixtime(modifieddate) modifieddate, current_date() from store_stage.creditcard a where a.creditcardid not in (select creditcardid from store.creditcard);

-- copy all from temp into target 
insert overwrite table store.creditcard
select * from store_stage.creditcard_temp;

-- drop temp table
drop table store_stage.creditcard_temp;