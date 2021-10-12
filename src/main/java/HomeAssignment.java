import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class HomeAssignment {
    private static String PATH = "./kinaxis_data_eng_v1.0/";

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Build a DataFrame from Scratch")
                .master("local[*]")
                .getOrCreate();
        HomeAssignment homeAssignment = new HomeAssignment();

        homeAssignment.sparkProcessing(sparkSession);
    }

    public void sparkProcessing(SparkSession sparkSession) {
        Dataset<Row> df = sparkSession.read().format("csv")
                .option("header", "true").load(PATH + "expeditions.csv");

        df.createOrReplaceTempView("ExpeditionTable");

        Dataset<Row> sqlDF = sparkSession.sql(
                "SELECT " +
                        "Mineral, SUM(Quantity) AS Quantity " +
                        "FROM ExpeditionTable  " +
                        "GROUP BY Mineral " +
                        "ORDER BY Mineral");

        sqlDF.coalesce(1).write().option("header", true)
                .csv(PATH + "datacsv/");
        sparkSession.close();

        renameFile(sparkSession);
    }

    private static void renameFile(SparkSession sparkSession) {
        try {
            FileSystem fs = FileSystem.get(sparkSession.sparkContext().hadoopConfiguration());

            String file = fs.globStatus(new Path(PATH + "datacsv/part*"))[0].getPath().getName();
            fs.rename(new Path(PATH + "datacsv/" + file),
                    new Path(PATH + "datacsv/minerals.csv"));

            fs.delete(new Path(PATH + "datacsv/" + file), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
