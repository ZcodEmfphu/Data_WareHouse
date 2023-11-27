create database Control;
use Control;
go

CREATE TABLE Status (
Id INT PRIMARY KEY AUTO_INCREMENT,	
status varchar(10),
Description varchar(255)
);

CREATE TABLE Config (
    Id INT PRIMARY KEY AUTO_INCREMENT,
    Process VARCHAR(255),
    Source VARCHAR(255),
    Username VARCHAR(40),
    Password VARCHAR(50),
    Port INT,
    Title VARCHAR(255),
    FileName VARCHAR(255),
    Status INT,
    Flag CHAR(5),
    CONSTRAINT config_fk FOREIGN KEY (Status) REFERENCES STATUS(Id)
);

CREATE TABLE Log (
    Id INT PRIMARY KEY AUTO_INCREMENT,
    Time DATETIME,
    Title VARCHAR(255),
    Description VARCHAR(255),
    Config_Id INT,
    Status INT,
    FOREIGN KEY (Config_Id) REFERENCES Config(Id),
    FOREIGN KEY (Status) REFERENCES Status(Id)
);

Insert into Status(status, description) values('Prepared','Sẵn sàng lấy dữ liệu');
Insert into Status(status, description) values('Crawling','Dữ liệu đang được lấy');
Insert into Status(status, description) values('Crawled','Dữ liệu đã được lấy');
Insert into Status(status, description) values('Extracting','Dữ liệu đang được truyền');
Insert into Status(status, description) values('Extracted','Dữ liệu đã được truyền');
Insert into Status(status, description) values('Transform','Hoàn tác thay thế');
Insert into Status(status, description) values('Loading','Đang Load dữ liệu');
Insert into Status(status, description) values('Loaded','Đã Load dữ liệu');
Insert into Status(status, description) values('Repalce','Dữ liệu lỗi đang được thay thế');
Insert into Status(status, description) values('Error','Lỗi');

