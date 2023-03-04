# AdventureWorks DWH
Build the AdventureWorks Datawarehourse model from the OLTP model. Target DWH model is presented in ``AdventureWorksDW.png``.

# Fact Tables

- ``FactCallCenter``: Call center calls aggregated daily
- ``FactFinance``
- ``NewFactCurrencyRate``
- ``FactAdditionalInternationalProductDescription``
- ``FactSurveyResponse``
- ``FactInternetSalesReason``
- ``FactInternetSales``
- ``FactSalesQuota``
- ``FactProductInventory``
- ``FactResellerSales``
- ``FactCurrencyRate``

# Dimension Tables

- ``DimAccount``
- ``DimCurrency``
- ``DimCustomer``
- ``DimDate``
- ``DimDepartmentGroup``
- ``DimEmployee``
- ``DimGeography``
- ``DimOrganization``
- ``DimProduct``
- ``DimProductCategory``
- ``DimProductSubcategory``
- ``DimPromotion``
- ``DimReseller``
- ``DimSalesReason``
- ``DimSalesTerritory``
- ``DimScenario``

# Restore OLTP Back up

```sql
USE [master]
RESTORE DATABASE [AdventureWorks2014]
FROM DISK = '/data/AdventureWorks2014.bak'
WITH MOVE 'AdventureWorks2014_Data' TO '/var/opt/mssql/data/AdventureWorks2014_Data.mdf',
MOVE 'AdventureWorks2014_Log' TO '/var/opt/mssql/data/AdventureWorks2014_Log.ldf',
FILE = 1,  NOUNLOAD,  STATS = 5
GO
```

# Restore DW Back up

```sql
USE [master]
RESTORE DATABASE [AdventureWorksDW2014]
FROM DISK = '/data/AdventureWorksDW2014.bak'
WITH MOVE 'AdventureWorksDW2014_Data' TO '/var/opt/mssql/data/AdventureWorksDW2014_Data.mdf',
MOVE 'AdventureWorksDW2014_Log' TO '/var/opt/mssql/data/AdventureWorksDW2014_Log.ldf',
FILE = 1,  NOUNLOAD,  STATS = 5
GO
```
