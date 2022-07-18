package com.jobreadyprogrammer.spark;

/*
* Customize scheme, Added for Simple JSON, Multiline JSON
* */
public class Application {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\WinUtil");

/*		InferCSVSchema parser = new InferCSVSchema();
		parser.printSchema();*/
		
/*		DefineCSVSchema parser2 = new DefineCSVSchema();
		parser2.printDefinedSchema();*/

		JSONLinesParser parser3 = new JSONLinesParser();
		parser3.parseJsonLines();

	}

}
