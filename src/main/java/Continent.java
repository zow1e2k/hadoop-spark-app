import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;

import static org.apache.spark.sql.functions.*;

public class Continent {
    private Dataset<Row> inDataset;
    private Dataset<Row> topGrowingDataset;
    private Dataset<Row> data;

    public Continent(Dataset<Row> data, String continentName) {
        long unix = Instant.now().getEpochSecond();

        this.data = data;

        this.inDataset = Main.spark.sql(
                "SELECT" +
                        " `continent`," +
                        " `location`," +
                        " `date`," +
                        " `total_cases`," +
                        " `new_cases`," +
                        " `population`" +
                        " FROM `countries`" +
                        " WHERE `continent` = '" + continentName + "'"
        );

        //long month_ago_unix = unix - Main.ONE_MONTH_UNIX;
        long one_year_ago_unix = unix - Main.ONE_YEAR_UNIX;
        Date date = new Date(one_year_ago_unix * 1000L);
        SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd");
        String formattedDate = sdf.format(date);
        //System.out.println(formattedDate);

        this.topGrowingDataset = this.inDataset
                .select("`date`", "`new_cases`", "`population`", "`location`")
                .withColumn(
                        "growin_percent",
                        round(col("new_cases")
                                .divide(col("population")
                                        .multiply(100)),
                        12
                        )
                )
                .where("`date` > TO_DATE('" + formattedDate + "', 'yyyy-mm-dd')")
                .groupBy("location")
                .agg(
                        avg("growin_percent").as("growin_percent")
                ).filter(
                        (FilterFunction<Row>) r -> !r.anyNull()
                )
                .orderBy(desc("growin_percent"))
                .limit(Main.MAX_CONTINENT_TOP);

        return;
    }

    public Dataset<Row> getTopGrowingDataset() {
        return this.topGrowingDataset;
    }

    public void printInDataset() {
        this.inDataset.show((int)this.inDataset.count());
        return;
    }

    public void printTopGrowingDataset() {
        this.topGrowingDataset.show((int)this.topGrowingDataset.count());
        return;
    }
}
