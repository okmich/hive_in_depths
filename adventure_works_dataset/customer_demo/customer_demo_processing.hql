create external table customer_demo
(
	customerid int,
	totalpurchaseytd decimal(15,2),
	datefirstpurchase date,
	birthdate date,
	maritalstatus string,
	yearlyincome string,
	gender string,
	totalchildren tinyint,
	numberchildrenathome tinyint,
	education string,
	occupation string,
	homeownerflag string,
	numbercarsowned tinyint,
	commutedistance string
)
stored as orc
location '/user/cloudera/bigretail/output/stores/spark/customer_demo';
