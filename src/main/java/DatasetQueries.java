import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

class DatasetQueries {



    void executeQ12Dataset() {

        SparkConf conf1 = new SparkConf().setAppName("ex4q12").setMaster("local[*]");
        JavaSparkContext sc1 = new JavaSparkContext(conf1);
        //SQLContext sqlCont = new SQLContext(sc1);
        SparkSession spark1 = SparkSession
                .builder().appName("appName1")
                .getOrCreate();

        Dataset<Row> df = spark1.read().option("header", true).csv("src\\main\\resources\\AuthPapers.csv");
        //df.show();
        df.createOrReplaceTempView("authpapers");
        Dataset<Row> dg = spark1.read().option("header", true).csv("src\\main\\resources\\Persons.csv");
        //dg.show();
        dg.createOrReplaceTempView("persons");
        Dataset<Row> dh = spark1.read().option("header", true).csv("src\\main\\resources\\Papers.csv");
        //dh.show();
        dh.createOrReplaceTempView("papers");

        Dataset<Row> q12 = spark1.sql("Select p2.name as a1, p1.name as a2, count(a2.akey) as cnt --a1.pkey, a2. pkey, a1.akey, a2.akey,\n" +
                "from\n" +
                "persons p1 inner join\n" +
                "authpapers a1 on p1.akey=a1.akey \n" +
                "\tinner join authpapers a2 on a1.pkey = a2.pkey inner join persons p2 on a2.akey = p2.akey\n" +
                "where a1.akey < a2.akey \n" +
                "group by p1.name, p2.name\n" +
                "order by cnt desc\n" +
                "limit 5");
        q12.show();
        q12.write().parquet("C:\\out12.parquet");
        spark1.stop();
    }

    void executeQ13Dataset() {

        SparkConf conf2 = new SparkConf().setAppName("ex4q13").setMaster("local[*]");
        JavaSparkContext sc2 = new JavaSparkContext(conf2);
        //SQLContext sqlCont = new SQLContext(sc2);
        SparkSession spark2 = SparkSession
                .builder().appName("appName2")
                .getOrCreate();

        Dataset<Row> ef = spark2.read().option("header", true).csv("src\\main\\resources\\AuthPapers.csv");
        //ef.show();
        ef.createOrReplaceTempView("authpapers");
        Dataset<Row> eg = spark2.read().option("header", true).csv("src\\main\\resources\\Persons.csv");
        //eg.show();
        eg.createOrReplaceTempView("persons");
        Dataset<Row> eh = spark2.read().option("header", true).csv("src\\main\\resources\\Papers.csv");
        //eh.show();
        eh.createOrReplaceTempView("papers");
        Dataset<Row> ei = spark2.read().option("header", true).csv("src\\main\\resources\\Conferences.csv");
        //ei.show();
        ei.createOrReplaceTempView("conferences");
        Dataset<Row> ej = spark2.read().option("header", true).csv("src\\main\\resources\\Journals.csv");
        //ej.show();
        ej.createOrReplaceTempView("journals");

        Dataset<Row> q13 = spark2.sql("select persons.name, count(papers.pkey) as countp\n" +
                "from persons \n" +
                "\tinner join authpapers on persons.akey = authpapers.akey \n" +
                "\tinner join papers on authpapers.pkey = papers.pkey\n" +
                "left join conferences on papers.ckey = conferences.ckey\n" +
                "left join journals on journals.jkey = papers.jkey\n" +
                "where conferences.sname = 'SIGMOD'  and conferences.year between 2014 and 2020\n" +
                "\tor journals.sname = 'PVLDB' and journals.year between 2014 and 2020 \n" +
                "group by persons.name\n" +
                "having count(authpapers.pkey) > 20\n" +
                "order by countp desc");

        q13.show();
        q13.write().parquet("C:\\out13.parquet");
        spark2.stop();
    }
}
