create database store_stage;
use store_stage;

-- all tables are managed because data will always be cleared
-- to load new sets from the sqoop job

create table product (
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
	sellstartdate bigint,
	sellenddate bigint,
	discontinueddate bigint,
	productsubcategory string,
	productcategory string,
	producemodel string,
	catalogdescription string,
	instructions string,
	modifieddate bigint
)
stored as parquet
location '/user/cloudera/bigretail/output/stores/stage/products';


create table customer (
	CustomerID int,
	AccountNumber  string,
	CustomerType varchar(1),
	Demographics string, 
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
	ModifiedDate bigint
	)
stored as parquet
location '/user/cloudera/bigretail/output/stores/stage/customers';


create table salesorderheader (
	salesorderid int,          
	revisionnumber smallint,        
	orderdate bigint,             
	duedate bigint,               
	shipdate bigint,              
	status smallint,                
	onlineorderflag boolean,       
	salesordernumber string,      
	purchaseordernumber string,   
	accountnumber string,         
	customerid int,            
	contactid int,           
	billtoaddressid int,       
	shiptoaddressid int,       
	shipmethodid int,          
	creditcardid int,          
	creditcardapprovalcode string,
	currencyrateid int,        
	subtotal double,              
	taxamt double,                
	freight double,               
	totaldue double,              
	comment string,              
	salespersonid int, 
	territoryid int,
	territory string,
	countryregioncode string,
	group string,
	fromcurrencycode string,
	tocurrencycode string,
	averagerate double,
	endofdayrate double,
	modifieddate bigint     
)
stored as parquet
location '/user/cloudera/bigretail/output/stores/stage/salesorderheader';   


create table salesorderdetails(
	salesorderdetailid int,    
	salesorderid int,
	carriertrackingnumber string, 
	orderqty int,              
	productid int,             
	unitprice double,             
	unitpricediscount double,     
	linetotal double,       
	specialofferid tinyint, 
	description string,
	discountpct double,
	type string,
	modifieddate string 
)
stored as parquet
location '/user/cloudera/bigretail/output/stores/stage/salesorderdetails';


create table creditcard (
	creditcardid int,
	cardtype string,
	cardnumber string,
	expmonth int,
	expyear int,
	modifieddate bigint	
)
stored as parquet
location '/user/cloudera/bigretail/output/stores/stage/creditcard';