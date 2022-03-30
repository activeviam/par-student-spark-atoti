package io.atoti.spark;

import java.util.ArrayList;
import java.util.List;
import java.sql.Date;
import java.util.Map;
import java.util.stream.Collectors;

import io.github.cdimascio.dotenv.Dotenv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.Dataset;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestDatabricksCluster {

    static Dotenv dotenv = Dotenv.load();
    static SparkSession spark =
            SparkSession.builder()
                    .appName("Spark Atoti")
                    .config("spark.master", "local")
                    .config("spark.databricks.service.clusterId", dotenv.get("clusterId"))
                    .getOrCreate();

    @Test
    void testDatabricksConnection() {
        StructType schema = new StructType(new StructField[] {
                new StructField("AirportCode", DataTypes.StringType, false, Metadata.empty()),
                new StructField("Date", DataTypes.DateType, false, Metadata.empty()),
                new StructField("TempHighF", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("TempLowF", DataTypes.IntegerType, false, Metadata.empty()),
        });

        List<Row> dataList = new ArrayList<Row>();
        dataList.add(RowFactory.create("BLI", Date.valueOf("2021-04-03"), 52, 43));
        dataList.add(RowFactory.create("BLI", Date.valueOf("2021-04-02"), 50, 38));
        dataList.add(RowFactory.create("BLI", Date.valueOf("2021-04-01"), 52, 41));
        dataList.add(RowFactory.create("PDX", Date.valueOf("2021-04-03"), 64, 45));
        dataList.add(RowFactory.create("PDX", Date.valueOf("2021-04-02"), 61, 41));
        dataList.add(RowFactory.create("PDX", Date.valueOf("2021-04-01"), 66, 39));
        dataList.add(RowFactory.create("SEA", Date.valueOf("2021-04-03"), 57, 43));
        dataList.add(RowFactory.create("SEA", Date.valueOf("2021-04-02"), 54, 39));
        dataList.add(RowFactory.create("SEA", Date.valueOf("2021-04-01"), 56, 41));

        Dataset<Row> temps = spark.createDataFrame(dataList, schema);

        // Create a table on the Databricks cluster and then fill
        // the table with the DataFrame's contents.
        // If the table already exists from a previous run,
        // delete it first.
        spark.sql("USE default");
        spark.sql("DROP TABLE IF EXISTS demo_temps_table");
        temps.write().saveAsTable("demo_temps_table");

        // Query the table on the Databricks cluster, returning rows
        // where the airport code is not BLI and the date is later
        // than 2021-04-01. Group the results and order by high
        // temperature in descending order.
        Dataset<Row> df_temps = spark.sql("SELECT * FROM demo_temps_table " +
                "WHERE AirportCode != 'BLI' AND Date > '2021-04-01' " +
                "GROUP BY AirportCode, Date, TempHighF, TempLowF " +
                "ORDER BY TempHighF DESC");

        // Results:
        //
        // +-----------+----------+---------+--------+
        // |AirportCode|      Date|TempHighF|TempLowF|
        // +-----------+----------+---------+--------+
        // |        PDX|2021-04-03|       64|      45|
        // |        PDX|2021-04-02|       61|      41|
        // |        SEA|2021-04-03|       57|      43|
        // |        SEA|2021-04-02|       54|      39|
        // +-----------+----------+---------+--------+

        assertThat(df_temps).isNotNull();
        List<Row> rows = df_temps.collectAsList();
        assertThat(rows).hasSize(4);
        final var valuesById =
                rows.stream()
                        .collect(
                                Collectors.toUnmodifiableMap(
                                        row -> ((Number) row.getAs("TempLowF")).intValue(),
                                        row -> ((String) row.getAs("AirportCode"))));
        assertThat(valuesById).containsExactlyEntriesOf(Map.of(45, "PDX", 41, "PDX", 43, "SEA", 39, "SEA"));

        // Clean up by deleting the table from the Databricks cluster.
        spark.sql("DROP TABLE demo_temps_table");
    }
}