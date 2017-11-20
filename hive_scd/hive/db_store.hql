create database store;

use store;

create external table customer (
	CustomerID int,
	AccountNumber  string,
	CustomerType varchar(1),
	NameStyle boolean,
	Title string,
	FirstName string ,
	MiddleName  string,
	LastName string ,
	Suffix string ,
	EmailAddress  string,
	EmailPromotion int,
	Phone  string,
	AdditionalContactInfo  string,
	TerritoryID int,
	territoryName  string,
	countryregioncode  string,
	`group`  string,
	ModifiedDate timestamp,
	change_date timestamp,
	active boolean
)
stored as parquet
location '/user/cloudera/bigretail/output/stores/target/customers';



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
	commutedistance string,
	create_date date
)
stored as orc
location '/user/cloudera/bigretail/output/stores/target/customer_demo';


create external table customer_demo_history
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
	commutedistance string,
	create_date date,
	end_date date
)
stored as orc
location '/user/cloudera/bigretail/output/stores/target/customer_demo_history';

create external table creditcard (
	creditcardid int,
	cardtype string,
	cardnumber string,
	expmonth int,
	expyear int,
	modifieddate date,
	create_date date
)
stored as orc
location '/user/cloudera/bigretail/output/stores/target/creditcard'
TBLPROPERTIES ("transactional"="true");

create external table product (
	productId int,
	name string,
	productnumber string,
	makeflag boolean,
	finishedgoodsflag boolean,
	color string,
	safetystocklevel int,
	reorderpoint int,
	standardcost double,
	listprice double,
	size string,
	sizeunitmeasurecode string,
	weightunitmeasurecode string,
	weight string,
	daystomanufacture int,
	productline string,
	class string,
	style string,
	sellstartdate date,
	sellenddate date,
	discontinueddate date,
	productsubcategory string,
	productcategory string,
	producemodel string,
	catalogdescription string,
	instructions string,
	modifieddate date
)
stored as orc
location '/user/cloudera/bigretail/output/stores/target/products';