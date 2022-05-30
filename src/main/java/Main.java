import algebra.lattice.Bool;
import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames.DataFrame;
import org.apache.hadoop.thirdparty.protobuf.Int32Value;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.Encode;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import scala.Array;
import scala.Function1;
import spire.random.FlatMap;
import org.apache.spark.sql.functions.*;

import java.nio.file.Paths;
import java.sql.Date;
import java.util.*;
import java.time.Instant;

import static org.apache.spark.sql.functions.*;

public class Main {
    private static String
            APPLICATION_NAME =      "App",
            SPARK_MASTER_TYPE =     "spark://26.55.32.120:7077",
            HADOOP_WIN_PATH =       "Z:/hadoop",
            HADOOP_UNIX_PATH =      "/",
            RESOURCES_PATH =        "src/main/resources/",
            ANALYZING_FILENAME =    "owid-covid-data.csv";

    public static int
            MAX_CONTINENT_TOP =     3;

    public static long
            ONE_MONTH_UNIX =        262_974_3,
            ONE_YEAR_UNIX =         31_535_999;

    public static SparkConf sparkConf;
    public static SparkSession spark;

    public static void main(String[] args) {
        /*String os = System.getProperty("os.name").toLowerCase();

        System.setProperty(
                "hadoop.home.dir",
                os.contains("win") ? Paths.get(HADOOP_WIN_PATH).toAbsolutePath().toString() : HADOOP_UNIX_PATH
        );*/

        Main.sparkConf = new SparkConf()
                .setAppName(APPLICATION_NAME)
                .setMaster(SPARK_MASTER_TYPE);

        Main.spark = SparkSession.builder()
                .config(Main.sparkConf)
                .getOrCreate();

        Dataset<Row> data = Main.spark
                .read()
                .option("header", "true")
                .csv(RESOURCES_PATH + ANALYZING_FILENAME);
        data.createOrReplaceTempView("countries");

        Dataset<Row> continents = Main.spark.sql(
        "SELECT DISTINCT" +
                " `continent`" +
                " FROM `countries`"
        );

        Dataset<String> continentString = continents.map(
                (MapFunction<Row, String>) row -> row.getAs("continent"),
                Encoders.STRING()
        );

        ArrayList<String> continentsNames = new ArrayList<>();
        continentsNames.addAll(continentString.collectAsList());

        World world = new World(data);

        for (String continentName : continentsNames) {
            world.createContinentByName(continentName);
        }

        ArrayList<Dataset<Row>> topCountriesEachContinent = new ArrayList<>();
        ArrayList<Continent> continentsList = world.getContinents();

        for (Continent continent : continentsList) {
            //continent.printInDataset();
            continent.printTopGrowingDataset();
            topCountriesEachContinent.add(continent.getTopGrowingDataset());
        }

        Dataset<Row> topCountriesWorld = topCountriesEachContinent.get(0);

        for (Dataset<Row> dataset : topCountriesEachContinent) {
            if (dataset.equals(topCountriesWorld)) {
                continue;
            }

            topCountriesWorld = topCountriesWorld.union(dataset);
            //topCountriesWorld.show((int)topCountriesWorld.count());
            //dataset.show((int)dataset.count());
        }

        Dataset<Row> maxTopCountriesWorld = topCountriesWorld
                .groupBy("location")
                .agg(
                        max("growin_percent").as("growin_percent")
                ).filter(
                        (FilterFunction<Row>) r -> !r.anyNull()
                )
                .orderBy(desc("growin_percent"))
                .limit(Main.MAX_CONTINENT_TOP);

        maxTopCountriesWorld.show((int)maxTopCountriesWorld.count());

        //Dataset<Row> selectAsia2 = selectAsia.filter((FilterFunction<Row>) s -> !s.isNull);
        //date_format(col("date"), "YYYY-MM-DD").as("format");

        /*Dataset<Row> flatMap = df.flatMap(
                (FlatMapFunction<Row, String>) (r, s) -> {
                    if (r.equals(null)) {
                        return null;
                    }

                    return Arrays.asList(r, s).iterator();
                },
                Encoders.()
        );*/

        /*double flatMap = df.flatMap(
                (FlatMapFunction<double, double>) (a, b) -> {
                    return (double)a;
                },
                Encoders.DOUBLE()
        );*/

        //newDF.show((int)newDF.count());

        /*Dataset<Long> vacPercent = newDF.map(
                (MapFunction<Row, Long>) row -> {
                    if (row.)
                    return row.getAs("ratio");
                },
                Encoders.LONG()
        );*/
        //vac.show((int)vac.count());

        /*Dataset<String> flatMap = df.flatMap(
                (FlatMapFunction<Row, String>) row -> {
                    return Arrays.asList(
                            (String)row.getAs("location"),
                            (String)row.getAs("date")
                    ).iterator();
                },
                Encoders.STRING()
        );*/

        //flatMap.select(collect_list("new_cases")).show(false);

        //flatMap.show((int)flatMap.count());

        /*Dataset<Row> ds = selectAsia
                .groupBy(col("location"))
                .agg(sum("new_cases").as("sum"))
                .sort(col("sum"));

        ds.show((int)ds.count());*/

        /*Dataset<String> maximum = populations.flatMap(
                (FlatMapFunction<Row, String>) row -> (Iterator<String>) row.getAs("population"), Encoders.STRING()
        );*/

        //selectAsia.show();
        //maximum.show();
        //populations.show();
        //System.out.println(a);

        /*long numBs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) { return s.contains("b"); }
        }).count();*/
                //selectAsia.show(1000);
                //System.out.println(a);

                // new_cases - +зараженных сегодня
                // population - население
                // new_cases / population - +процент зараженных сегодня
                // total_cases / population - процент зараженных всего
                // MAX(new_cases(...) / population(...)) - максимальный процент роста заражаемости в стране

                //selectAsia.filter()

                //selectAsia.
                //Dataset<Row> selectAll = spark.sql("SELECT DISTINCT `continent`, `location` FROM countries");
                //selectAll.show((int)selectAll.count());
                //data.toDF().show();

                //Dataset<Row> dataDF = data.toDF();

        /*JavaRDD<Country> rdd_records = data.map(
            new Function<String, Country>() {
                public Country call(String line) throws Exception {
                    String[] fields = line.split(",");
                    Country country = new Country(fields[0], fields[1], fields[2]);
                    return country;
                }
            });

        System.out.println(data.rdd().toDebugString());*/

                //JavaRDD<String> logData = sc.textFile(logFile).cache();

        /*long numAs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) { return s.contains("a"); }
        }).count();

        long numBs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) { return s.contains("b"); }
        }).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);*/

                //sc.stop();
    }
}
