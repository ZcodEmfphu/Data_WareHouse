package model;

import controller.LoadFromTempToDataWareHouse;

public class Main {

	private static LoadFromTempToDataWareHouse l1 = new LoadFromTempToDataWareHouse();

	public static void main(String[] args) {
		l1.loadDataFromTempToDataWarehouse();
	}
}
