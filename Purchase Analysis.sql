---------We first restored the OLTP database and then the staging and data Warehouse
---Create Purchase Staging DataBase
CREATE database PurchaseStagging

 IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'PurchaseStaging')
	CREATE DATABASE PurchaseStaging
ELSE
	Print ('database already exist')


----Create Data Warehouse
 IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'PurchaseEDW')
	CREATE DATABASE PurchaseEDW
ELSE
	Print ('database already exist')


---Create the Schemas
USE PurchaseStaging
CREATE SCHEMA Staging


USE PurchaseEDW
CREATE SCHEMA EDW



----------------- Creating the Date Dimesion Table-------

CREATE TABLE EDW.DimDate
(
DateSK int,    
BusinessDate date,   
BusinessYear int,
BusinessMonth int,
BusinessQuarter nvarchar(2),
EnglishMonth nvarchar(50),
EnglishDayofWeek NVARCHAR(50),
SpanishMonth nvarchar(50),
SpanishDayofWeek nvarchar(50),
FrenchMonth nvarchar(50),
FrenchDayofWeek nvarchar(50),
LoadDate datetime default getdate(),
Constraint edw_dimdate_sk Primary key (DateSK)
)

----- insert Dta dynamically into the Date table using a stored Procedure

Create or alter procedure EDW.DateGenerator(@endDate date)
AS
BEGIN
SET NOCOUNT ON
--Declare @EndDate date = '2090-12-31'
	declare @StartDate date =  (SELECT MIN(CONVERT(DATE, minDate)) FROM
		(SELECT MIN(TRANSDATE) AS minDate FROM PurchaseOLTP.dbo.SalesTransaction
		union ALL
		SELECT MIN(TRANSDATE) AS minDate FROM PurchaseOLTP.dbo.PurchaseTransaction
		)A
	)
	
	Declare @NofDays int = DATEDIFF(Day, @startDate,  @Enddate)
	Declare @CurrentDay int = 0
	Declare @CurrentDate DATE

	IF (SELECT OBJECT_ID('EDW.DIMDATE')) IS NOT NULL
		TRUNCATE TABLE EDW.DimDate

	WHILE @CurrentDay <= @NofDays
	BEGIN
		SELECT @currentdate = (DATEADD(day, @CurrentDay, @StartDate))
		
	
		INSERT INTO EDW.DimDate(DateSK, BusinessDate,BusinessYear,BusinessMonth, BusinessQuarter, EnglishMonth,
		EnglishDayofWeek,SpanishMonth,SpanishDayofWeek,FrenchMonth, FrenchDayofWeek,LoadDate)
		SELECT CONVERT(INT, CONVERT(NVARCHAR(8), @CurrentDate, 112)), @CurrentDate, Year(@CurrentDate),
		MONTH(@CurrentDate), 'Q' + CAST(DATEPART(Q, @currentDate) as nvarchar), DATENAME(month, @currentDate), 
		datepart(dw, @CurrentDate),
		CASE DATEPART(MONTH, @CurrentDate)
			WHEN 1 THEN 'Enero' when 2 THEN 'Febrero' when 3 then 'Marzo' WHEN 4 THEN 'Abril' WHEN 5 THEN 'Mayo'
			WHEN 6 THEN 'Junio' WHEN 7 THEN 'Julio' WHEN 8 THEN 'Agosto' when 9 then 'Septiembre' when 10 then 'Octubre'
			WHEN 11 THEN 'Noviembre' when 12 then 'Diciembre' 
		END,
		CASE DATEPART(WEEKDAY, @CurrentDate)
			WHEN 1 THEN 'Domingo' when 2 then 'Lunes' when 3 then 'Martes' when 4 then 'Miercoles' when 5 then 'Jueves'
			when 6 then 'Viernes' when 7 then 'Sabado'
			END, 
		CASE DATEPART(MONTH, @CurrentDate)
			WHEN 1 THEN 'Janvier' when 2 THEN 'Février' when 3 then 'mars' WHEN 4 THEN 'Avril' WHEN 5 THEN 'Mai'
			WHEN 6 THEN 'JUIN' WHEN 7 THEN 'Juillet' WHEN 8 THEN 'Août' when 9 then 'Septembre' when 10 then 'Octobre'
			WHEN 11 THEN 'Novembre' when 12 then 'Décembre' 
		END,
		CASE DATEPART(WEEKDAY, @CurrentDate)
			WHEN 1 THEN 'Dimanche' when 2 then 'Lundi' when 3 then 'Mardi' when 4 then 'Mercredi' when 5 then 'Jeudi'
			when 6 then 'Vendredi' when 7 then 'Samedi'
			END,
			Getdate()
		SELECT @CurrentDay = @CurrentDay+1
	END
END


-----Extract Product Information from OLTP----
USE [PurchaseOLTP]
SELECT P.ProductID,P.Product, P.ProductNumber, p.UnitPrice, D.Department, getdate() as LoadDate
FROM PRODUCT P
INNER JOIN Department D ON P.DepartmentID = D.DepartmentID

SELECT Count(*) AS SourceCount
FROM PRODUCT P
INNER JOIN Department D ON P.DepartmentID = D.DepartmentID



------Transform and Load Product into Staging---
use [PurchaseStaging]
CREATE TABLE Staging.Product
	(
	ProductID INT,
	Product nvarchar(50),
	ProductNumber nvarchar(50),
	UnitPrice float,
	Department nvarchar(50),
	loadDate datetime default GETDATE(),
	constraint Staging_product_pk Primary key(productid)
	)


SELECT ProductID, Product, ProductNumber, UnitPrice, Department, getdate() as Loaddate

SELECT COUNT(*) AS DesCount FROM Staging.Product

Truncate Table Staging.Product


-------Transform and Load product into the data warehouse
USE PurchaseEDW
CREATE TABLE EDW.DimProduct
	(
	ProductSk int identity(1,1),
	ProductID INT,
	Product nvarchar(50),
	ProductNumber nvarchar(50),
	UnitPrice float,
	Department nvarchar(50),
	EffectiveStartDate datetime,
	EffectiveEndDate datetime,
	constraint Staging_product_sk Primary key(productsk)
	)

SELECT COUNT(*) As PreCount FROM EDW.DimProduct

SELECT COUNT(*) As PostCount FROM EDW.DimProduct



----Extract Store information from OLTP -----
USE PurchaseOLTP
SELECT  S.StoreID, s.StoreName, s.StreetAddress, C.CityName, st.State
FROM Store AS S
INNER JOIN City C ON S.CityID = C.CityID
INNER JOIN State AS st ON S.StateID = st.StateID


SELECT  COUNT(*) AS sourceCount
FROM Store AS S
INNER JOIN City C ON S.CityID = C.CityID
INNER JOIN State AS st ON S.StateID = st.StateID


----Transform and Load Store into Staging----
use PurchaseStaging
CREATE TABLE Staging.Store
(
StoreID int,
StoreName nvarchar(50),
StreetAddress nvarchar(50),
CityName nvarchar(50),
State nvarchar(50),
LoadDate datetime default getdate(),
Constraint staging_store_sk Primary key (storeID)
)

SELECT StoreID, StoreName, StreetAddress, CityName, State, getdate as LoadDate FROM staging.store
SELECT COUNT(*) AS DesCount FROM Staging.Store

Truncate Table Stagging.Store


-----Transform and Load Store into the data Warehouse----
use PurchaseEDW
CREATE TABLE EDW.DimStore
(
StoreSK int identity(1,1),
StoreID int,
StoreName nvarchar(50),
StreetAddress nvarchar(50),
CityName nvarchar(50),
State nvarchar(50),
EffectiveStartDate datetime,
Constraint stagging_store_sk Primary key (storeSK)
)

SELECT COUNT(*) AS PreCount FROM EDW.DimStore
SELECT COUNT(*) AS PostCount FROM EDW.DimStore


----Extract Employee Information Table From OLTP 
USE PurchaseOLTP
SELECT e.EmployeeID, E.EmployeeNo, CONCAT(UPPER(E.LastName), ',', E.FirstName) AS Employee,  E.DoB, m.MaritalStatus
FROM Employee E
INNER JOIN MaritalStatus M on E.MaritalStatus = M.MaritalStatusID

SELECT COUNT(*) AS SourceCount
FROM Employee E
INNER JOIN MaritalStatus M on E.MaritalStatus = M.MaritalStatusID


--- Transform and Load Employee into Staging-----
use PurchaseStaging
CREATE TABLE Staging.Employee
(
EmployeeID int,
EmployeeNO Nvarchar(50),
Employee nvarchar(255),
DOB Date,
MaritalStatus Nvarchar(50),
LoadDate datetime default getdate(),
Constraint staging_employee_sk primary key (EmployeeID)
)

SELECT EmployeeID, EmployeeNo, Employee, DOB, MaritalStatus, getdate() as LoadDate  FROM Staging.Employee


SELECT COUNT(*) AS DesCount  FROM Staging.Employee

TRUNCATE TABLE Staging.Employee


----Transform and Load Employee into the data warehouse-----
use PurchaseEDW
CREATE TABLE EDW.DimEmployee
(
EmployeeSK INT Identity(1,1),
EmployeeID int,
EmployeeNO Nvarchar(50),
Employee nvarchar(255),
DOB Date,
MaritalStatus Nvarchar(50),
EffectiveStartdate datetime,
EffectiveEndDate datetime,
Constraint EDW_Dimemployee_sk primary key (EmployeeSK)
)

SELECT COUNT(*) AS PreCount  FROM EDW.dimEmployee
SELECT COUNT(*) AS PostCount  FROM EDW.dimEmployee



----Extract Vendor information from OLTP-----
USE PurchaseOLTP
SELECT V.VendorID, v.vendorNo, CONCAT_WS( ',', UPPER(V.LastName), V.FirstName) AS Vendor,  V.RegistrationNo, 
v.VendorAddress, C.CityName AS City, S.State FROM Vendor V
INNER JOIN City C ON V.CityID = C.CityID
INNER JOIN STATE S ON C.StateID = S.StateID


SELECT COUNT(*) AS SourceCount FROM Vendor V
INNER JOIN City C ON V.CityID = C.CityID
INNER JOIN STATE S ON C.StateID = S.StateID


-----Transform and Load Vendor into Stagging -----
USE purchaseStagging
CREATE TABLE Staging.Vendor
(
VendorID int,
VendorNo nvarchar(50),
Vendor nvarchar(255),
RegistrationNo nvarchar(50),
VendorAddress nvarchar(50),
City nvarchar(50),
State nvarchar(50),
LoadDate datetime default getdate(),
Constraint staging_vendor_pk primary key(VendorID)
)

SELECT VendorID, VendorNo, Vendor,  RegistrationNo, VendorAddress,
City, State FROM Stagging.Vendor

SELECT COUNT(*) as Descount FROM Staging.Vendor

Truncate Table Vendor.staging


----Transform and Load Vendor into the data warehouse------
use	purchaseEDW
CREATE TABLE EDW.DimVendor
(
VendorSK int identity(1,1),
VendorID int,
VendorNo nvarchar(50),
Vendor nvarchar(255),
RegistrationNo nvarchar(50),
VendorAddress nvarchar(50),
City nvarchar(50),
State nvarchar(50),
EffectiveStartDate datetime,
EffectiveEndDate datetime,
Constraint stagging_Dimvendor_sk primary key(Vendorsk)
)


SELECT COUNT(*) as precount FROM EDW.DimVendor
SELECT COUNT(*) as postcount FROM EDW.DimVendor


----Extract Purchase Analysis information from OLTP------- 
USE purchaseOLTP
IF (SELECT COUNT(*) FROM purchaseEDW.EDW.Fact_PurchaseAnalysis) <=0
	SELECT P.TransactionID, P.TransactionNO, CONVERT(DATE, P.TransDate) AS TransDate, CONVERT(DATE,p.OrderDate)
	AS OrderDate, Convert(date, p.DeliveryDate) AS DeliveryDate, 
	p.VendorID, p.EmployeeID, p.ProductID, p.StoreID, DateDiff(Day, p.OrderDate, p.DeliveryDate) +1 AS DifferentialDays,
	p.Quantity, p.TaxAmount, p.LineAmount, GETDATE() AS LoadDate
	FROM PurchaseTransaction AS P
	WHERE CONVERT(DATE, p.TransDate) <= DATEADD(day,-1, convert(date, getdate()))
ELSE
	SELECT P.TransactionID, P.TransactionNO, CONVERT(DATE, P.TransDate) AS TransDate, CONVERT(DATE,p.OrderDate) AS OrderDate,
	Convert(date, p.DeliveryDate) AS DeliveryDate, 
	p.VendorID, p.EmployeeID, p.ProductID, p.StoreID, DateDiff(Day, p.OrderDate, p.DeliveryDate) +1 AS DifferentialDays,
	p.Quantity, p.TaxAmount, p.LineAmount, GETDATE() AS LoadDate
	FROM PurchaseTransaction AS P
	WHERE CONVERT(DATE, p.TransDate) = DATEADD(day,-1, convert(date, getdate()))

----SourceCount----
IF (SELECT COUNT(*) FROM purchaseEDW.EDW.Fact_PurchaseAnalysis) <=0
	SELECT COUNT(*) as SourceCount FROM PurchaseTransaction AS P
	WHERE CONVERT(DATE, p.TransDate) <= DATEADD(day,-1, convert(date, getdate()))
ELSE
	SELECT Count(*) AS SourceCount
	FROM PurchaseTransaction AS P
	WHERE CONVERT(DATE, p.TransDate) = DATEADD(day,-1, convert(date, getdate()))


----Transform and Load Purchase analysis into Stagging 
use purchaseStaging
CREATE TABLE Staging.PurchaseAnalysis
(
TransactionID int,
TransactionNO nvarchar(50),
TransDatesk Date,
OrderDate Date,
DeliveryDate Date,
VendorID int,
EmployeeID int,
ProductID int,
StoreID int,
DifferentialDays int,
Quantity float,
TaxAmount float,
LineAmount float,
LoadDate datetime default GETDATE(),
Constraint Staging_PurchaseAnalysis_pk primary key(TransactionID)
)
SELECT TransactionID, TransactionNO, TransDatesk, OrderDate, DeliveryDate, VendorID, EmployeeID, ProductID,
StoreID, DifferentialDays,Quantity,TaxAmount, LineAmount, getdate() AS LoadDate from Staging.PurchaseAnalysis

SELECT COUNT(*) AS DesCount FROM Staging.PurchaseAnalysis

Truncate Table Stagging.PurchaseAnalysis




---------Transform and Load Purchase analysis into the data warehouse-----
USE purchaseEDW
Create Table eDw.fact_PurchaseAnalysis
	(
	   purchaseAnalysisSK  bigint identity(1,1),
	   TransactionNo nvarchar(50),
	   TransDateSK int,
	   OrderDateSk int,
	   DeliveryDateSk int,
	   VendorSk int,
	   EmployeeSK int,
	   ProductSk int,
	   StoreSk int,
	   DifferentialDays int,
   	   Quantity float, 
	   TaxAmount  float,
	   LineAmount float,
	   LoadDate datetime default getdate(),
	   constraint edw_fact_PurchaseAnalysis_sk primary key(PurchaseAnalysisSk),
	   constraint EDW_Purchase_Transdatesk foreign key (TransDateSk) references EDW.DimDate(dateSk),		
		constraint EDW_Purchase_Orderdatesk foreign key (OrderDateSk) references EDW.DimDate(dateSk),		
		constraint EDW_Purchase_DeliveryDatesk foreign key (DeliveryDateSk) references EDW.DimDate(dateSk),
		Constraint EDW_Purchase_VendorSk foreign key (VendorSk) references EdW.DimVendor(VendorSk),
		constraint EDW_Purchase_EmployeeSK foreign key (EmployeeSk) references EDW.DimEmployee(EmployeeSK),
		constraint EDW_Purchase_ProductSk foreign key(ProductSK) references EDW.DimProduct(ProductSk),
		constraint EDW_Purchase_StoreSk foreign key(StoreSK) references EDW.dimStore(StoreSk)		
	)

 SELECT COUNT(*) AS PreCount from EDW.fact_PurchaseAnalysis
 SELECT COUNT(*) AS PostCount from EDW.fact_PurchaseAnalysis