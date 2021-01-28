import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class DistributedDataset {

    void executeQ12RDD(){
        SparkConf conf12 = new SparkConf().setAppName("ex4RDDq12").setMaster("local[*]");
        JavaSparkContext sc12 = new JavaSparkContext(conf12);
        //SQLContext sqlCont = new SQLContext(sc12);
        SparkSession spark12 = SparkSession
                .builder().appName("appName12")
                .getOrCreate();

        JavaRDD<String> ardd1 = sc12.textFile("src\\main\\resources\\AuthPapers.csv");
        JavaRDD<String> ardd2 = sc12.textFile("src\\main\\resources\\Papers.csv");
        JavaRDD<String> ardd3 = sc12.textFile("src\\main\\resources\\Persons.csv");
        //JavaRDD<String> querry12 = sc12.
        //System.out.println(query12);
        spark12.stop();
    }

    void executeQ13RDD(){
        SparkConf conf13 = new SparkConf().setAppName("ex4RDDq13").setMaster("local[*]");
        JavaSparkContext sc13 = new JavaSparkContext(conf13);
        //SQLContext sqlCont = new SQLContext(sc1);
        SparkSession spark13 = SparkSession
                .builder().appName("appName1")
                .getOrCreate();

        JavaRDD<String> brdd1 = sc13.textFile("src\\main\\resources\\AuthPapers.csv");
        JavaRDD<String> brdd2 = sc13.textFile("src\\main\\resources\\Papers.csv");
        JavaRDD<String> brdd3 = sc13.textFile("src\\main\\resources\\Persons.csv");
        JavaRDD<String> brdd4 = sc13.textFile("src\\main\\resources\\Journals.csv");
        JavaRDD<String> brdd5 = sc13.textFile("src\\main\\resources\\Conferences.csv");
        //JavaRDD<String> querry13 = sc13.
        //System.out.println(query13);
        spark13.stop();
    }
}
