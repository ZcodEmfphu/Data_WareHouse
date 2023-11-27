CREATE DATABASE GiaVang_STAGING;
use giavang_staging;
go

CREATE TABLE TEMP (
Id int  PRIMARY KEY  AUTO_INCREMENT, 
Date text, 
Time text,
Product text, 
BuyingPrice text, 
SellingPrice Text
);

INSERT INTO `temp` (`Id`, `Date`, `Time`, `Product`, `BuyingPrice`, `SellingPrice`) VALUES (1, '2023-11-26', '21:19:39', 'Vàng miếng SJC 999.9', '7080', '7170');
INSERT INTO `temp` (`Id`, `Date`, `Time`, `Product`, `BuyingPrice`, `SellingPrice`) VALUES (2, '2023-11-26', '21:19:39', 'Nhẫn Trơn PNJ 999.9', '5975', '6090');
INSERT INTO `temp` (`Id`, `Date`, `Time`, `Product`, `BuyingPrice`, `SellingPrice`) VALUES (3, '2023-11-26', '21:19:39', 'Vàng Kim Bảo 999.9', '5975', '6090');
INSERT INTO `temp` (`Id`, `Date`, `Time`, `Product`, `BuyingPrice`, `SellingPrice`) VALUES (4, '2023-11-26', '21:19:39', 'Vàng Phúc Lộc Tài 999.9', '5975', '6095');
INSERT INTO `temp` (`Id`, `Date`, `Time`, `Product`, `BuyingPrice`, `SellingPrice`) VALUES (5, '2023-11-26', '21:19:39', 'Vàng nữ trang 999.9', '5970', '6050');
INSERT INTO `temp` (`Id`, `Date`, `Time`, `Product`, `BuyingPrice`, `SellingPrice`) VALUES (6, '2023-11-26', '21:19:39', 'Vàng nữ trang 999', '5964', '6044');
INSERT INTO `temp` (`Id`, `Date`, `Time`, `Product`, `BuyingPrice`, `SellingPrice`) VALUES (7, '2023-11-26', '21:19:39', 'Vàng nữ trang 99', '5900', '6000');
INSERT INTO `temp` (`Id`, `Date`, `Time`, `Product`, `BuyingPrice`, `SellingPrice`) VALUES (8, '2023-11-26', '21:19:39', 'Vàng 750 (18K)', '4413', '4553');
INSERT INTO `temp` (`Id`, `Date`, `Time`, `Product`, `BuyingPrice`, `SellingPrice`) VALUES (9, '2023-11-26', '21:19:39', 'Vàng 585 (14K)', '3414', '3554');
INSERT INTO `temp` (`Id`, `Date`, `Time`, `Product`, `BuyingPrice`, `SellingPrice`) VALUES (10, '2023-11-26', '21:19:39', 'Vàng 416 (10K)', '2392', '2532');
INSERT INTO `temp` (`Id`, `Date`, `Time`, `Product`, `BuyingPrice`, `SellingPrice`) VALUES (11, '2023-11-26', '21:19:39', 'Vàng miếng PNJ (999.9)', '5975', '6095');
INSERT INTO `temp` (`Id`, `Date`, `Time`, `Product`, `BuyingPrice`, `SellingPrice`) VALUES (12, '2023-11-26', '21:19:39', 'Vàng 916 (22K)', '5502', '5552');
INSERT INTO `temp` (`Id`, `Date`, `Time`, `Product`, `BuyingPrice`, `SellingPrice`) VALUES (13, '2023-11-26', '21:19:39', 'Vàng 650 (15.6K)', '3808', '3948');
INSERT INTO `temp` (`Id`, `Date`, `Time`, `Product`, `BuyingPrice`, `SellingPrice`) VALUES (14, '2023-11-26', '21:19:39', 'Vàng 680 (16.3K)', '3989', '4129');
INSERT INTO `temp` (`Id`, `Date`, `Time`, `Product`, `BuyingPrice`, `SellingPrice`) VALUES (15, '2023-11-26', '21:19:39', 'Vàng 610 (14.6K)', '3566', '3706');
INSERT INTO `temp` (`Id`, `Date`, `Time`, `Product`, `BuyingPrice`, `SellingPrice`) VALUES (16, '2023-11-26', '21:19:39', 'Vàng 375 (9K)', '2144', '2284');
INSERT INTO `temp` (`Id`, `Date`, `Time`, `Product`, `BuyingPrice`, `SellingPrice`) VALUES (17, '2023-11-26', '21:19:39', 'Vàng 333 (8K)', '1872', '2012');