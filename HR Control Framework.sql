-------ETL Control Framework-------
-----------Create Control database-----

IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'HrControl')
	CREATE DATABASE HrControl
ELSE
	Print ('database already exist')

use HrControl
CREATE SCHEMA Control


----------Staging and data Warehouse Environment----------
CREATE TABLE Control.Environment
(
EnvironmentID int,
Environment nvarchar(255),
CreatedDate datetime,
Constraint Control_environment_pk primary key(EnvironmentID)
)

	Insert into Control.Environment(EnvironmentID, Environment, CreatedDate)
	values
	(1, 'Stagging', getdate()),
	(2, 'EDW', getdate())



----------Migration Frequency----------
CREATE TABLE Control.Frequency
(FrequencyID int,
Frequency nvarchar(255),
CreatedDate datetime,
Constraint Control_Frequency_pk primary key(FrequencyID)
)

	Insert into control.Frequency(FrequencyID, Frequency, CreatedDate)
	values
	(1, 'Daily', getdate()),
	(2, 'Weekly', getdate()),
	(3, 'Monthly', getdate()),
	(4, 'Yearly', getdate())



----------Control PackageType----------
CREATE TABLE Control.PackageType
(
PackageTypeID int,
PackageType nvarchar(255),
CreatedDate datetime,
Constraint control_packageType_pk primary key(PackageTypeID)
)

Insert into Control.PackageType(PackageTypeID, PackageType, CreatedDate)
Values
(1, 'Dimension', GETDATE()),
(2, 'Fact', Getdate())



----------Control Package---------
CREATE TABLE control.Package
(
PackageID int,
PackageName nvarchar(255), 
PackageTypeid int, 
SequenceNo int, 
EnvironmentID int, 
FrequencyID int, 
RunStartDate date,
RunEndDate date,
Active bit,
LastRunDate datetime,
Constraint control_Package_PackageID_pk Primary key(PackageID),
Constraint control_Package_PackageType_fk Foreign key(PackageTypeID) references Control.PackageType(PackageTypeID),
Constraint control_Package_Environment_fk Foreign key(EnvironmentID) references control.Environment(EnvironmentID),
CONSTRAINT Control_Package_Frequency_fk Foreign key(FrequencyID) references control.Frequency(FrequencyID)
)

Insert into Control.Package(packageID, PackageName, PackageTypeID, SequenceNo, EnvironmentID, FrequencyID, RunStartDate, 
Active)
values
		(1, 'StgEmployee.dtsx', 1, 100,1,1, convert(date, getdate()),1),
		(2, 'StgStore.dtsx', 1, 200,1,1, convert(date, getdate()),1),
		(3, 'StgAbsence.dtsx', 1, 300,1,1, convert(date, getdate()),1),
		(4, 'stgAbsentAalysis.dtsx', 2, 400,1,1, convert(date, getdate()),1),
		(5, 'StgMisconduct.dtsx', 1, 500,1,1, convert(date, getdate()),1),
		(6, 'StgDecision.dtsx', 1, 600,1,1, convert(date, getdate()),1),
		(7, 'stgMisconductAalysis.dtsx', 2, 700,1,1, convert(date, getdate()),1),
		(8, 'stgOvertimeAnalysis.dtsx', 2, 800,1,1, convert(date, getdate()),1),
		(9, 'dimEmployee.dtsx', 1, 100,2,1, convert(date, getdate()),1),
		(10, 'dimStore.dtsx', 1, 200,2,1, convert(date, getdate()),1),
		(11, 'dimAbsence.dtsx', 1, 300,2,1, convert(date, getdate()),1),
		(12, 'AbsentAnalysis.dtsx', 2, 400,2,1, convert(date, getdate()),1),
		(13, 'dimMisconduct.dtsx', 1, 500,2,1, convert(date, getdate()),1),
		(14, 'dimDecision.dtsx', 1, 600,2,1, convert(date, getdate()),1),
		(15, 'MisConductAnalysis.dtsx', 2, 700,2,1, convert(date, getdate()),1),
		(16, 'OvertimeAnalysis.dtsx', 2, 800,2,1, convert(date, getdate()),1)
		

--------- Control Metrics----------
Create table Control.Metrics
(
MetricsID bigint identity(1,1),
PackageID int,
StgSourceCount int,
StgDesCount int,
PreCount int,
CurrentCount int,
Type1Count int,
Type2Count int,
PostCount int,
RunDate datetime,
Constraint control_Metrics_MetricsID_pk Primary key(MetricsID),
Constraint Control_Metrics_PackageID_fk Foreign key(PackageID) references control.Package(PackageID)
)

--------Metrics for Dimension Table----------
Declare @PackageID int =?
Declare @StgSourceCount int =?
Declare @StgDesCount int =?
INSERT INTO Control.metrics(PackageID, StgSourceCount, StgDesCount, RunDate)
VALUES(@PackageID, @StgSourceCount, @StgDesCount, getdate())

Update control.Package set LastRunDate = getdate() where packageID = @PackageID


----------Metrics For Fact Table--------
Declare @PackageID int =?
Declare @PreCount int =?
Declare @CurrentCount int =?
Declare @Type1Count int =?
Declare @Type2Count int =?
Declare @PostCount int =?

INSERT INTO Control.metrics(PackageID, PreCount, CurrentCount, Type1Count, Type2Count, PostCount, RunDate)
VALUES(@PackageID, @PreCount, @CurrentCount, @Type1Count, @Type2Count, @PostCount, getdate())

Update control.Package set LastRunDate = getdate() where packageID = @PackageID


----------Control Anomalies Table----------
Create Table control.Anomalies
(
AnomaliesSK Bigint identity(1,1),
PackageID int,
TableName nvarchar(255),
ColumnName nvarchar(255),
RecordID int,
CreatedDate datetime default getdate(),
Constraint Control_Anomalies_sk primary key(AnomaliesSK),
Constraint anomalies_package_fk foreign key(PackageID) references control.Package(packageID)
)


-----Control Framework for stagging-----------
SELECT PackageID, PackageName, SequenceNo FROM(
	SELECT PackageID, PackageName, SequenceNo, FrequencyID FROM Control.Package
	WHERE (Active = 1 AND RunStartDate <= convert(date,getdate()))
	AND (RunEndDate is NULL OR RunEndDate>=convert(date, getdate()))
	AND EnvironmentID = 1 and FrequencyID =1
	UNION all
	SELECT PackageID, PackageName, SequenceNo, FrequencyID FROM Control.Package
	WHERE (Active = 1 AND RunStartDate <= convert(date,getdate()))
	AND (RunEndDate is NULL OR RunEndDate>=convert(date, getdate()))
	AND EnvironmentID = 1 and FrequencyID =2 AND DATEPART(WEEKDAY, dateadd(day, -1, convert(date, getdate()))) = 7
	UNION all
	SELECT PackageID, PackageName, SequenceNo, FrequencyID FROM Control.Package
	WHERE (Active = 1 AND RunStartDate <= convert(date,getdate()))
	AND (RunEndDate is NULL OR RunEndDate>=convert(date, getdate()))
	AND EnvironmentID = 1 and FrequencyID =3 AND EOMONTH(GETDATE())= dateadd(day, -1, convert(date, getdate()))
	UNION all
	SELECT PackageID, PackageName, SequenceNo, FrequencyID FROM Control.Package
	WHERE (Active = 1 AND RunStartDate <= convert(date,getdate()))
	AND (RunEndDate is NULL OR RunEndDate>=convert(date, getdate()))
	AND EnvironmentID = 1 and FrequencyID =4 AND EOMONTH(GETDATE())= dateadd(day, -1, convert(date, getdate()))
	AND DATEPART(MONTH, DATEADD(DAY, -1, CONVERT(DATE,GETDATE()))) = 12
) RunP ORDER BY FrequencyID, SequenceNo


-----Control Framework for the Data Warehouse---------
SELECT PackageID, PackageName, SequenceNo FROM(
	SELECT PackageID, PackageName, SequenceNo, FrequencyID FROM Control.Package
	WHERE (Active = 1 AND RunStartDate <= convert(date,getdate()))
	AND (RunEndDate is NULL OR RunEndDate>=convert(date, getdate()))
	AND EnvironmentID = 2 and FrequencyID =1
	UNION all
	SELECT PackageID, PackageName, SequenceNo, FrequencyID FROM Control.Package
	WHERE (Active = 1 AND RunStartDate <= convert(date,getdate()))
	AND (RunEndDate is NULL OR RunEndDate>=convert(date, getdate()))
	AND EnvironmentID = 2 and FrequencyID =2 AND DATEPART(WEEKDAY, dateadd(day, -1, convert(date, getdate()))) = 7
	UNION all
	SELECT PackageID, PackageName, SequenceNo, FrequencyID FROM Control.Package
	WHERE (Active = 1 AND RunStartDate <= convert(date,getdate()))
	AND (RunEndDate is NULL OR RunEndDate>=convert(date, getdate()))
	AND EnvironmentID = 2 and FrequencyID =3 AND EOMONTH(GETDATE())= dateadd(day, -1, convert(date, getdate()))
	UNION all
	SELECT PackageID, PackageName, SequenceNo, FrequencyID FROM Control.Package
	WHERE (Active = 1 AND RunStartDate <= convert(date,getdate()))
	AND (RunEndDate is NULL OR RunEndDate>=convert(date, getdate()))
	AND EnvironmentID = 2 and FrequencyID =4 AND EOMONTH(GETDATE())= dateadd(day, -1, convert(date, getdate()))
	AND DATEPART(MONTH, DATEADD(DAY, -1, CONVERT(DATE,GETDATE()))) = 12
) RunP ORDER BY FrequencyID, SequenceNo

