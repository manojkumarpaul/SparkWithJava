package com.jobreadyprogrammer.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

public class DisplayDataOnConsole {
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
        //df.show();
//		  df.show(3);
        //transformation
		  df = df.withColumn("Full Name",
				  concat(df.col("first_name"), lit(", "), df.col("last_name")))
				  .filter(df.col("comment").rlike("\\d+"))
		  .orderBy(df.col("first_name").desc());

        df.show();

    }
}
