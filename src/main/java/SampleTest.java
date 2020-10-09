import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

import java.util.TimerTask;
import java.util.Date;
import java.util.Timer;


public class SampleTest  {

    public static void main(String[] args) {

        try {
            //assuming it takes 20 secs to complete the task
            String providerFile = "/usr/local/providers/*.xml";
            String metaFile = "/usr/local/providers/providersmeta.xml";

            SparkSession spark = SparkSession.builder().
                    config("dfs.client.read.shortcircuit.skip.checksum", "true").getOrCreate();
            Dataset<Row> df = spark.read()
                    .format("com.databricks.spark.xml")
                    .option("rowTag", "details")
                    .load(providerFile);

            Dataset<Row> df1 = spark.read()
                    .format("com.databricks.spark.xml")
                    .option("rowTag", "address")
                    .load(metaFile);
            //System.out.println("Aradhya "+df.);
            df.show();
            df1.show();

            Dataset<Row> newdf = df.join(df1, "id");
            newdf.show();

            System.out.println("ajay");

            System.out.println("Numbes of Rows : " + df.count());

            newdf.where("city = 'Jabalpur'").repartition(1).write()
                    .mode("overwrite")
                    .format("com.databricks.spark.xml")
                    .option("rootTag", "providers")
                    .option("rowTag", "provider")
                    .save("pro.xml");
            spark.stop();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
