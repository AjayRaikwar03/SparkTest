import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

public class SampleTest {
    public static void main(String[] args) {
 //       final String sparkHome = "/usr/local/spark-3.0.1-bin-hadoop2.7";
//        SparkConf conf = new SparkConf()
//                .setMaster("local[*]")
//                .setAppName("spark tests")
//                .setSparkHome(sparkHome + "/libexec");
//        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        String logFile = "/usr/local/spark-3.0.1-bin-hadoop2.7/README.md";// Should be some file on your system
        SparkSession spark = SparkSession.builder().appName("SparkTests").getOrCreate();
        Dataset<String> logData = spark.read().textFile(logFile).cache();


        long numAs = logData.filter((FilterFunction<String>) s -> s.contains("a")).count();


        //logData.f
        long numBs = logData.filter((FilterFunction<String>)s -> s.contains("b")).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

        spark.stop();
    }
}