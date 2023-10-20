----------The first thing I did was to Create the staging and data Warehouse for the Human Resources (Hr) data

----------Create the HR Staging Database-------
IF NOT EXISTs (SELECT Name FROM sys.databases WHERE Name = 'HrStaging')
	CREATE DATABASE HrStaging
ELSE
	print('Database already exist')


----------Create the data Warehouse Database----------
IF NOT EXISTs (SELECT Name FROM sys.databases WHERE Name = 'HrEDW')
	CREATE DATABASE HrEDW
ELSE
	print('Database already exist')


------------Create the Schemas-----------
USE HrStaging
CREATE SCHEMA Staging


USE HrEDW
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

----- insert Data dynamically into the Date table using a stored Procedure

Create or alter procedure EDW.DateGenerator(@StartDate date, @endDate date)
AS
BEGIN
SET NOCOUNT ON
	
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
			WHEN 1 THEN 'Janvier' when 2 THEN 'F�vrier' when 3 then 'mars' WHEN 4 THEN 'Avril' WHEN 5 THEN 'Mai'
			WHEN 6 THEN 'JUIN' WHEN 7 THEN 'Juillet' WHEN 8 THEN 'Ao�t' when 9 then 'Septembre' when 10 then 'Octobre'
			WHEN 11 THEN 'Novembre' when 12 then 'D�cembre' 
		END,
		CASE DATEPART(WEEKDAY, @CurrentDate)
			WHEN 1 THEN 'Dimanche' when 2 then 'Lundi' when 3 then 'Mardi' when 4 then 'Mercredi' when 5 then 'Jeudi'
			when 6 then 'Vendredi' when 7 then 'Samedi'
			END,
			Getdate()
		SELECT @CurrentDay = @CurrentDay+1
	END
END
Exec dateGenerator '20160101','20301231'



----------------- Creating the Time Dimesion Table-------
USE HrEDW
Create Table EDW.dimTime
 (
   TimeSK int identity(1,1),
   TimeHour int,   ---- 0  to 23
   TimeInterval nvarchar(20) not null, --- 00:00-00:59, 01:00-01:59, 
   BusinessHour nvarchar(20) not null,
   PeriodofDay  nvarchar(20) not null,
   LoadDate datetime default getdate(),
   constraint Edw_dimTime_sk primary key(TimeSk)
 )


 ----- insert Data dynamically into the Time dimension table using a stored Procedure
Create or alter procedure EDW.dimTimeGenerator()
AS
BEGIN
SET NOCOUNT ON
 declare  @currentHour int= 0
   IF OBJECT_ID('EDW.dimTime') is not null
    TRUNCATE TABLE EDW.dimTime

 WHILE @currentHour<=23
 BEGIN
	
	insert into  EDW.dimTime(TimeHour,TimeInterval,BusinessHour,PeriodofDay,LoadDate)
	select @currentHour, right(concat('0',@currentHour),2)+':00-'+right(concat('0',@currentHour),2)+':59',
		case 
			When (@currentHour>=0 and @currentHour<=7 )  or (@currentHour>=18 and @currentHour<=23 ) Then 'Closed'	
			Else 'Open'
		END, 
		case 
			When @currentHour =0 Then 'MidNight'
			When @currentHour>=1 and @currentHour<=4 Then 'Early Morning'
			When @currentHour>=5 and @currentHour<=11 Then 'Morning'
		    When @currentHour=12 Then 'Noon'
			When @currentHour>=13 and @currentHour<=17 Then 'Afternoon'
			When @currentHour>=18 and @currentHour<=21 Then 'Evening'
			ElSE  'Night'
		END, getdate()
			   
	select @currentHour=@currentHour+1
 END
END

exec EDW.dimTimeGenerator


----------Extract Employee Information Table From OLTP 
USE OLTP
SELECT E.EmployeeID, E.EmployeeNo, CONCAT(UPPER(E.LastName), ',', E.FirstName) AS Employee,  E.DoB, m.MaritalStatus
FROM Employee AS E
INNER JOIN MaritalStatus AS M on E.MaritalStatus = M.MaritalStatusID

SELECT COUNT(*) AS SourceCount
FROM Employee E
INNER JOIN MaritalStatus M on E.MaritalStatus = M.MaritalStatusID


----------Transform and Load Employee into Staging----------
use HrStaging
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


----------Transform and Load Employee from the Staging environment into the data warehouse----------
use HrEDW
CREATE TABLE EDW.DimEmployee
(
EmployeeSK int Identity(1,1),
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


----------Extract Store information from OLTP ---------
USE OLTP
SELECT  S.StoreID, s.StoreName, s.StreetAddress, C.CityName, st.State
FROM Store AS S
INNER JOIN City AS C ON S.CityID = C.CityID
INNER JOIN State AS st ON S.StateID = st.StateID


SELECT  COUNT(*) AS sourceCount
FROM Store AS S
INNER JOIN City AS C ON S.CityID = C.CityID
INNER JOIN State AS st ON S.StateID = st.StateID


----------Transform and Load Store into Staging---------
use HrStaging
CREATE TABLE Staging.Store
(
StoreID int,
StoreName nvarchar(50),
StreetAddress nvarchar(50),
CityName nvarchar(50),
State nvarchar(50),
LoadDate datetime default getdate(),
Constraint staging_store_Pk Primary key (storeID)
)

SELECT StoreID, StoreName, StreetAddress, CityName, State, getdate() as LoadDate FROM staging.store
SELECT COUNT(*) AS DesCount FROM Staging.Store

Truncate Table Stagging.Store


-----Transform and Load Store from the Staging environment into the data Warehouse----
use HrEDW
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

---------Load Absence data from flat file into the staging database---------
USE HrStaging
CREATE TABLE Staging.Absence
(
	CategoryID int,
	Category nvarchar(255),
	LoadDate datetime default getdate()
)

SELECT Count(*) AS DesCount FROM Staging.Absence
TRUNCATE TABLE Staging.Absence


----------Deduplication of Absence data----------------
With Absence_CTE
AS
(
SELECT CategoryID, Category  FROM Staging.Absence
Group by CategoryID, Category
)
SELECT CategoryID, Category, getdate() as EffectiveStartDate from Absence_CTE


With Absence_CTE AS
(
SELECT CategoryID, Category  FROM Staging.Absence
Group by CategoryID, Category
)
SELECT Count(*) as CurrentCount from Absence_CTE


----------Transform and Load Absence from the Staging environment into the data warehouse----------- 
USE HrEDW
CREATE TABLE EDW.DimAbsence
(
	CategorySK int identity(1,1),
	CategoryID int,
	Category nvarchar(255),
	EffectiveStartDate datetime,
	Constraint EDW_DimAbsence_sk primary key(Categorysk)
)

SELECT COUNT(*) AS PreCount FROM EDW.DimAbsence
SELECT COUNT(*) AS PostCount FROM EDW.DimAbsence


---------Load Absence Analysis data (Fact table) from flat file into the staging database---------

USE HrStagging
CREATE TABLE Staging.Absent_Analysis
(
AbsentSK Bigint identity(1,1),
empid int,
store int,
absent_date date,
absent_hour int,
absent_category int,
Constraint stagging_Absent_pk primary key(AbsentSK)
)

SELECT CoUNT(*) AS DesCount from Stagging.Absent_Analysis
SELECT CoUNT(*) AS EDWCount from EDW.Fact_Absent_Analysis


------Absent table contain duplicate data. The following code was used to deduplicate before
---Loading into the staging database---------

SELECT AbsentSK, empid,store,absent_date,absent_hour,absent_category, getdate() as loaddate 
FROM Staging.Absent_Analysis
WHERE  AbsentSK  IN
(
SELECT min(AbsentSK) as AbsentSK from Staging.Absent_Analysis
GROUP BY empid,store,absent_date,absent_hour,absent_category
)


----------Current Count--------
SELECT Count(*) as CurrentCount FROM Stagging.Absent_Analysis
WHERE AbsentSK IN
(
SELECT min(AbsentSK) as AbsentSK, empid,store,absent_date,absent_hour,absent_category from Stagging.Absent_Analysis
GROUP BY empid,store,absent_date,absent_hour,absent_category
)

Truncate Table Staging.Absent_Analysis


----------Transform and Load Absence Analysis (Fact table) from the staging environment into the data warehouse----------- 
USE HrEDW
CREATE TABLE EDW.Fact_Absent_Analysis
(
AbsentSK Bigint identity(1,1),
employeeSK int,
storeSK int,
absent_dateSK int,
absent_hour int,
Absent_CategorySK int,
LoadDate datetime default GETDATE(),
Constraint EDW_AbsentAnalysis_SK primary key(AbsentSK),
Constraint EDW_Absent_EmployeeSK Foreign key(EmployeeSK) references EDW.DimEmployee(EmployeeSK),
Constraint EDW_Absent_DateSK Foreign key(Absent_Datesk) references EDW.DimDate(datesk),
Constraint EDW_Absent_StoreSK Foreign key(StoreSK) references EDW.DimStore(StoreSK),
Constraint EDW_Absent_CategorySK Foreign key(Absent_CategorySK) references EDW.DimAbsence(CategorySK) 
)

SELECT COUNT(*) AS PreCount FROM EDW.Fact_Absent_Analysis

SELECT COUNT(*) AS PostCount FROM EDW.Fact_Absent_Analysis


---------Load Misconduct data from flat file into the staging database---------
use HrStaging
CREATE TABLE Staging.Misconduct

(
MisconductID int,
Misconductdesc nvarchar(255),
LoadDate datetime default getdate()
)

SELECT count(*) DesCount FROM Staging.Misconduct


WITH Misconduct_CTE
AS
(
SELECT MisconductID, Misconductdesc FROM Staging.Misconduct
GROUP BY MisconductID, Misconductdesc
)
SELECT MisconductID, Misconductdesc, getdate() as EffectiveStartDate from Misconduct_CTE


select count(*) CurrentCount FROM Staging.Misconduct
GROUP BY MisconductID, Misconductdesc
Truncate Table staging.Misconduct

----------Transform and Load Misconduct data from the Staging environment into the data warehouse----------- 
USE HrEDW
CREATE TABLE EDW.DimMisconduct
(
MisconductSK int identity(1,1),
MisconductID int,
Misconductdesc nvarchar(255),
EffectiveStartDate datetime,
Constraint edw_DimMisconduct_sk primary key (MisconductSK)
)

SELECT COUNT(*) AS preCount FROM EDW.DimMisconduct
SELECT COUNT(*) AS PostCount FROM EDW.DimMisconduct


---------Load Misconduct Decision data from flat file into the staging database---------
USE HrStaging
CREATE TABLE Staging.Decision
(
Decision_ID int,
Decision nvarchar(255),
LoadDate datetime default getdate()
)
SELECT Decision_ID, Decision FROM Staging.Decision
select count(*) DesCount FROM Staging.Decision



with Decision_CTE AS
(SELECT Decision_ID, Decision FROM Stagging.Decision
GROUP BY Decision_ID, Decision
)
SELECT Decision_ID, Decision, getdate() as EffectiveStartDate  FROM Decision_CTE


with Decision_CTE AS
(SELECT Decision_ID, Decision FROM Staging.Decision
GROUP BY Decision_ID, Decision
)
SELECT COUNT(*) AS CurrentCount FROM Decision_CTE

Truncate Table staging.Decision

----------Transform and Load Misconduct Decision data from the staging environment into the data warehouse----------- 
Use HrEDW
CREATE TABLE EDW.DimDecision
(
DecisionSK int identity(1,1),
Decision_ID int,
Decision nvarchar(255),
EffectiveStartDate datetime,
Constraint EDW_dimDecision_sk primary key (DecisionSK)
)

Select COUNT(*) AS PreCount FROM EDW.DimDecision
Select COUNT(*) AS PostCount FROM EDW.DimDecision



---------Load Misconduct Fact table from flat file into the staging database--------
USE HrStagging
CREATE TABLE Staging.Misconduct_Analysis
(
Misconsk bigint identity(1,1),
empid int,
storeID int,
misconduct_date date,
misconduct_id int,
decision_id int,
LoadDate datetime default GETDATE(),
Constraint Stagging_Misconduct_pk primary key(MisconSK)
)

SELECT COUNT(*) AS DESCount from Stagging.Misconduct_analysis
SELECT COUNT(*) AS EDWCount from EDW.Fact_Misconduct_Analysis
 



------The misconduct fact table contain duplicate data. The following code was used to deduplicate before
------ Loading into the database---------

SELECT Misconsk, empid, storeID, misconduct_date, misconduct_id, decision_id, getdate() as LoadDate 
FROM Staging.Misconduct_Analysis
WHERE MisconSK in
	(SELECT max(Misconsk)
	FROM Staging.Misconduct_Analysis
	GROUP BY empid, storeID, misconduct_date, misconduct_id, decision_id)


SELECT COUNT(*) CurrentCount FROM Staging.Misconduct
WHERE MisconSK IN
	(SELECT max(Misconsk), empid, storeID, misconduct_date, misconduct_id, decision_id FROM Staging.Misconduct
	GROUP BY empid, storeID, misconduct_date, misconduct_id, decision_id)


--------------Transform and Load the Misconduct fact table from the staging environment into the data Warehouse
USE HrEDW
CREATE TABLE EDW.Fact_Misconduct_Analysis
(
Misconsk bigint identity(1,1),
EmployeeSK int,
storeSK int,
misconduct_dateSK int,
misconduct_idSK int,
decisionid_sk int,
LoadDate datetime default GETDATE(),
Constraint EDW_Misconduct_Sk primary key(MisconSK),
Constraint EDW_Misconduct_employee_sk Foreign key(EmployeeSK) references EDW.DimEmployee(EmployeeSK),
Constraint EDW_Misconduct_Store_sk Foreign key(StoreSK) references EDW.DimStore(StoreSK),
Constraint EDW_misconduct_dateSK foreign key(misconduct_dateSK) references  EDW.DimDate(datesk),
Constraint EDW_Misconduct_Misconductid_sk foreign key(Misconduct_idsk) references EDW.DimMisconduct(Misconductsk),
Constraint EDW_Misconduct_decisionid_sk foreign key(decisionid_sk) references EDW.DimDecision(decisionsk)
)

SELECT COUNT(*) AS PreCount from edw.Fact_Misconduct_Analysis

SELECT COUNT(*) AS PostCount from edw.Fact_Misconduct_Analysis


---------Load Overtime data from flat file into the staging database---------

 USE HrStaging
 CREATE TABLE Staging.Overtime
 (
 OvertimeID int,
 EmployeeNo nvarchar(50),
 FirstName nvarchar(50),
 LastName nvarchar(50),
 StartOvertime datetime,
 EndOvertime datetime,
 LoadDate datetime default GETDATE()
 )
 

 SELECT COUNT(*) AS DesCount from Stagging.Overtime


------------Deduplicate the Overtime data-------

  Select  OvertimeID, EmployeeNo, CONVERT(date,startOvertime) startOvertimeDate, DATEPART(hour, StartOvertime) StartOvertimeHour,
	CONVERT(date,EndOvertime) EndOvertimeDate, DATEPART(hour, EndOvertime) EndOvertimeHour, 
	convert(float,DATEDIFF(MINUTE,StartOvertime, EndOvertime))*1.0/60.0 as OvertimeHour, getdate() as LoadDate  from  	
	(
	  select Max(OvertimeId) OvertimeID, employeeNo,FirstName, LastName, StartOvertime, EndOvertime  from Staging.Overtime
	  group by employeeNo,FirstName, LastName, StartOvertime, EndOvertime
	) 
 

 
 Select  COUNT(*) AS CurrentCount from  	
	(
	  select Max(OvertimeId) OvertimeID, employeeNo,FirstName, LastName, StartOvertime, EndOvertime  from Staging.Overtime
	  group by employeeNo,FirstName, LastName, StartOvertime, EndOvertime
	) 

  TRUNCATE TABLE Stagging.overtime

-------------Transform and Load the Overtime fact table from the staging environment into the data Warehouse------
USE HrEDW
 Create Table EDW.fact_OvertimeAnalysis
	( 
	     OvertimeSK bigint identity(1,1),
	     OvertimeID int,		 
		 EmployeeSK int,
		 StartDatesk int,
		 StartHourSk int, 
		 EndDateSk int,
		 EndHourSK int,
		 OvertimeHour float,
		 LoadDate datetime default getdate(),
		 constraint edw_overtimeanalysis_sk primary key(OvertimeSk),
		 constraint edw_overtime_employeeSk  foreign key(Employeesk) references EDW.dimEmployee(employeeSk), 
		 constraint edw_overtime_startdateSk  foreign key(StartDatesk) references EDW.dimdate(datesk),
		 constraint edw_overtime_starthoursk foreign key(StartHourSk) references  EDW.dimTime(Timesk), 
		 constraint edw_overtime_EnddateSk  foreign key(EndDatesk) references EDW.dimdate(datesk),
		 constraint edw_overtime_Endhoursk foreign key(EndHourSk) references  EDW.dimTime(Timesk)
		)

SELECT Count(*) AS PreCount FROM EDW.fact_OvertimeAnalysis
SELECT Count(*) AS PostCount FROM EDW.fact_OvertimeAnalysis

