use role accountadmin;
use warehouse compute_wh;

create or replace database SuperStore;
use database SuperStore;

create or replace schema SuperStore_Schema;
use schema SuperStore_Schema;

create or replace file format SuperStore_ff
    type = 'CSV'
    field_delimiter = ','
    field_optionally_enclosed_by = '"'
    skip_header = 1
    replace_invalid_characters = TRUE
    trim_space = TRUE
    empty_field_as_null = TRUE
    null_if = ('NULL','null','');

create or replace stage SuperStore_Stage
    file_format = SuperStore_ff;

list @SuperStore_Stage;

create or replace table SuperStore_Bronze_Table (
    Row_ID STRING,
    Order_ID STRING,
    Order_Date STRING,
    Ship_Date STRING,
    Ship_Mode STRING,
    Customer_ID STRING,
    Customer_Name STRING,
    Segment STRING,
    Country STRING,
    City STRING,
    State STRING,
    Postal_Code STRING,
    Region STRING,
    Product_ID STRING,
    Category STRING,
    Sub_Category STRING,
    Product_Name STRING,
    Sales STRING,
    Quantity STRING,
    Discount STRING,
    Profit STRING
);

copy into SuperStore_Bronze_Table from @SuperStore_Stage
    file_format = (format_name = SuperStore_ff)
    on_error = 'CONTINUE';

select * from SuperStore_Bronze_Table;
select count(*) from SuperStore_Bronze_Table;

truncate table SuperStore_Bronze_Table;

copy into SuperStore_Bronze_Table from @SuperStore_Stage
    file_format = (format_name = SuperStore_ff)
    on_error = 'CONTINUE';

select * from SuperStore_Bronze_Table;
select count(*) from SuperStore_Bronze_Table;

create or replace table DIM_Customer AS
    select distinct Customer_ID,
                    Customer_Name,
                    Segment from SuperStore_Bronze_Table;
select * from DIM_Customer;

create or replace table DIM_Product AS
    select distinct Product_ID,
                    Product_Name,
                    Category,
                    Sub_Category from SuperStore_Bronze_Table;
select * from DIM_Product;

create or replace table DIM_Location AS
    select distinct Postal_Code,
                    City,
                    State,
                    Country,
                    Region from SuperStore_Bronze_Table;
select * from DIM_Location;

create or replace table FACT_Orders AS
    select  CAST(Row_ID AS INT) AS Row_ID,
            Order_ID,
            TRY_TO_DATE(Order_Date,'MM/DD/YYYY') AS Order_Date,
            TRY_TO_DATE(Ship_Date,'MM/DD/YYYY') AS Ship_Date,
            Ship_Mode,
            Customer_ID,
            Product_ID,
            Postal_Code,
            CAST(Sales AS FLOAT) AS Sales,
            CAST(Quantity AS INT) AS Quantity,
            CAST(Discount AS FLOAT) AS Discount,
            CAST(Profit AS FLOAT) AS Profit from SuperStore_Bronze_Table;
select * from FACT_Orders;

SELECT 
    COUNT(*) AS Total_Rows,
    COUNT(Order_Date) AS Valid_Dates,
    COUNT(*) - COUNT(Order_Date) AS Failed_Dates
FROM FACT_Orders;

truncate table DIM_LOCATION;
create or replace table DIM_Location AS
    select distinct Postal_Code,
                    MAX(City) AS City,
                    MAX(State) AS State,
                    MAX(Country) AS Country,
                    MAX(Region) AS Region from SuperStore_Bronze_Table
                    group by Postal_Code;
select * from DIM_Location;

truncate table DIM_Product;
create or replace table DIM_Product AS
    select distinct Product_ID,
                    MAX(Product_Name) AS Product_Name,
                    MAX(Category) AS Category,
                    MAX(Sub_Category) AS Sub_Category from SuperStore_Bronze_Table
                    group by Product_ID;
select * from DIM_Product;