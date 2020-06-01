-- tag=name: TempPersons
create table TempPersons as
select PersonID, BirthDate, Gender from Persons where GroupID = :IdGroup;

-- tag=name: ExportPersons
-- tag=FileName: persons.csv
-- tag=hash: PersonID
select PersonID, BirthDate, Gender from Persons where BirthDate <= :refDate;

-- tag=name: ExportCities
-- tag=fileName: cities.csv
select CityID, Name, ProvID from cities;

-- tag=name: TempPersonsIndex:
create index tempperX1 on TempPersons(BirthDate);

-- tag=name: UpdateStocks
update Stocks set qty = 0 where qty = -1;

-- tag=name: SalesVW
create view SalesVW as
select ClientID, Qty, Amount from Sales where CompanyID = 1;

-- tag=name: Sales
-- tag=filename: sales.csv
-- tag=hash: ClientID
select ClientID, Qty, Amount from SalesVW;
