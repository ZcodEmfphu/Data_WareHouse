package model;

import controller.LoadFromTempToDataWareHouse;
import controller.LoadFromDataWareHousetoDataMart;

public class Main {

	private static LoadFromTempToDataWareHouse l1 = new LoadFromTempToDataWareHouse();
	private static LoadFromDataWareHousetoDataMart l2 = new LoadFromDataWareHousetoDataMart();
	private static Config cof = new Config();

	public static void main(String[] args) {
		l1.loadDataFromTempToDataWarehouse();
//		l2.LoadDatawarehouseToDataMart();
//		cof.loadConfigToDatabase("control");
	}
}
