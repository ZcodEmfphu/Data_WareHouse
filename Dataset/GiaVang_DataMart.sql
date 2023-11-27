CREATE DATABASE GiaVang_DataMart;
use GiaVang_DataMart;

CREATE TABLE Product_dim (
Id int  PRIMARY KEY  AUTO_INCREMENT,
Name text);


CREATE TABLE Product_fact(
Id int  PRIMARY KEY  AUTO_INCREMENT, 
Date_Id VARCHAR(255), 
Time VARCHAR(255),
Date_ex date,
Product_Id int, 
BuyingPrice VARCHAR(10), 
SellingPrice VARCHAR(10),  
status int,  
Config_Id int,
FOREIGN KEY (Config_Id) REFERENCES control.Config(Id),
FOREIGN KEY (Product_Id) REFERENCES Product_dim(Id)
);

drop TABLE Product_fact;

Insert into product_fact(Product_id,BuyingPrice, SellingPrice,time, Config_id)
Select giavang_datamart.product_dim.Id,giavang_staging.temp.BuyingPrice, giavang_staging.temp.SellingPrice, giavang_staging.temp.Time, control.config.Id
from giavang_datamart.product_dim, control.config, giavang_staging.temp;

Insert into product_fact(Product_id, Date_id, time)
select giavang_staging.temp.id,  giavang_staging.temp.Date,  giavang_staging.temp.Time
from giavang_staging.temp;

SELECT * from product_fact;
SELECT * from product_dim;






