use adventureworks;

create view v_salesorderheader as
	select
	SalesOrderID,          
	RevisionNumber,        
	OrderDate,             
	DueDate,               
	ShipDate,              
	Status,                
	OnlineOrderFlag,       
	SalesOrderNumber,      
	PurchaseOrderNumber,   
	AccountNumber,         
	so.CustomerID,            
	ContactID,           
	BillToAddressID,       
	ShipToAddressID,       
	ShipMethodID,          
	CreditCardID,          
	CreditCardApprovalCode,
	so.CurrencyRateID,        
	SubTotal,              
	TaxAmt,                
	Freight,               
	TotalDue,              
	Comment,              
	so.SalesPersonID, 
	sp.TerritoryID,
	st.name territory,
	st.CountryRegionCode,
	st.group,
	cr.FromCurrencyCode,
	cr.toCurrencyCode,
	cr.AverageRate,
	cr.EndOfDayRate,
	so.ModifiedDate         
from salesorderheader so left join salesperson sp on sp.SalesPersonID = so.SalesPersonID
left join salesterritory st ON st.TerritoryID = sp.TerritoryID
left join currencyrate cr on cr.CurrencyRateID = so.CurrencyRateID 


create view v_salesorderdetails as
select          
	SalesOrderDetailID,    
	SalesOrderID,
	CarrierTrackingNumber, 
	OrderQty,              
	ProductID,             
	UnitPrice,             
	UnitPriceDiscount,     
	LineTotal,       
	sod.SpecialOfferID, 
	so.Description,
	so.DiscountPct,
	so.Type,
	so.Category
	ModifiedDate   
from salesorderdetail sod join specialoffer so 
on sod.SpecialOfferID = so.SpecialOfferID;


create view v_product as
select p.productId,
	p.name,
	productnumber,
	makeflag,
	finishedgoodsflag,
	color,
	safetystocklevel,
	reorderpoint,
	standardcost,
	listprice,
	size,
	sizeunitmeasurecode,
	weightunitmeasurecode,
	weight,
	daystomanufacture,
	productline,
	class,
	style,
	sellstartdate,
	sellenddate,
	discontinueddate,
	ps.name productsubcategory,
	pc.name productcategory,
	pm.name producemodel,
	pm.catalogdescription,
	pm.instructions,
	p.modifieddate
from product p left join productsubcategory ps on p.productsubcategoryid = ps.productsubcategoryid
left join productcategory pc on ps.productcategoryid = pc.productcategoryid
left join productmodel pm on p.productmodelid = pm.productmodelid;



create view v_customer as 
select cus.CustomerID,cus.AccountNumber, cus.CustomerType, ind.Demographics, con.NameStyle, con.Title, con.FirstName,             
con.MiddleName,con.LastName, con.Suffix, con.EmailAddress, con.EmailPromotion, con.Phone, con.AdditionalContactInfo,  
cus.TerritoryID,t.name territoryName, t.countryregioncode, t.group, con.ModifiedDate from customer cus
join individual ind on ind.CustomerID = cus.CustomerID join contact con on con.ContactID = ind.ContactID join salesterritory t on
cus.TerritoryID = t.TerritoryID
union	
select cus.CustomerID, cus.AccountNumber, cus.CustomerType, st.Demographics, con.NameStyle, con.Title, st.Name, con.MiddleName,            
con.LastName,con.Suffix,con.EmailAddress,con.EmailPromotion, con.Phone, con.AdditionalContactInfo, cus.TerritoryID, t.name territoryName, 
t.countryregioncode, t.group, cus.ModifiedDate from store st
join customer cus on  st.CustomerID = cus.CustomerID join storecontact sc on  st.CustomerID = sc.CustomerID  join contact con on
sc.ContactID = con.ContactID join salesterritory t on cus.TerritoryID = t.TerritoryID
