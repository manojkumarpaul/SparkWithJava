package com.jobreadyprogrammer.spark;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class SaveDataInPostGresDB {
	public static void main(String args[]) throws InterruptedException {
		System.setProperty("hadoop.home.dir", "C:\\WinUtil");
		// Create a session
		SparkSession sparkSession = new SparkSession.Builder()
				.appName("CSV to DB") //any name for a session
				//.config("spark.sql.broadcastTimeout", "1800") //for timeout
				.master("local") //where the master node is
				.getOrCreate();

		//get the data
		Dataset<Row> df = sparkSession.read().format("csv")
				.option("header",true)
				.load("src/main/resources/name_and_comments.csv");
//				.load("src/main/resources/machine-readable-business-employment-data-mar-2022-quarter.csv");

		//save into DB
		String dbConnectionUrl = "jdbc:postgresql://localhost/course_data";
		Properties prop = new Properties();
		prop.setProperty("driver","org.postgresql.Driver");
		prop.setProperty("user","postgres");
		prop.setProperty("password","sysadmin");

		df.write()
				.mode(SaveMode.Overwrite)
				.jdbc(dbConnectionUrl,"project1",prop);
	}
}