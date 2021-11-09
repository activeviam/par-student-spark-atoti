package io.atoti.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URISyntaxException;
import java.net.URL;
import java.util.Map;
import java.util.Objects;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Test;
import scala.Tuple2;


public class TestDiscovery {
  SparkSession spark =
      SparkSession.builder().appName("Spark Atoti").config("spark.master", "local").getOrCreate();

  @Test
  void testDiscovery() throws URISyntaxException {
    final ClassLoader cl = Thread.currentThread().getContextClassLoader();
    final URL url = Objects.requireNonNull(cl.getResource("csv/basic.csv"), "Cannot find file");
    final URL urlTwoTypesInSameColumn = Objects.requireNonNull(cl.getResource("csv/twoTypesInSameColumn.csv"), "Cannot find file");
    final URL urlToJoin = Objects.requireNonNull(cl.getResource("csv/toJoin.csv"), "Cannot find file");
    final URL urlCalculate = Objects.requireNonNull(cl.getResource("csv/calculate.csv"), "Cannot find file");

    System.out.println(url.toURI().getPath());
	final Dataset<Row> dataframe = spark.read()
			.format("csv")
			.option("sep", ";")
			.option("header", true)
			.option("dateFormat", "dd/MM/yyyy")
            .option("inferSchema", true)
            .load(url.toURI().getPath());
    final Map<String, String> dTypes = Discovery.discoverDataframe(dataframe);

    assertThat(dataframe).isNotNull();
    assertThat(dTypes).isNotNull();

    assertTrue(dTypes.containsKey("id"));
    assertThat(dTypes.get("id")).isEqualTo(DataTypes.IntegerType.toString());
    assertTrue(dTypes.containsKey("label"));
    assertThat(dTypes.get("label")).isEqualTo(DataTypes.StringType.toString());
    assertTrue(dTypes.containsKey("value"));
    assertThat(dTypes.get("value")).isEqualTo(DataTypes.DoubleType.toString());

    final Dataset<Row> dataframeTwoTypesInSameColumn = spark.read()
          .format("csv")
          .option("sep", ";")
          .option("header", true)
          .option("timestampFormat", "dd/MM/yyyy")
          .option("inferSchema", true)
          .load(urlTwoTypesInSameColumn.toURI().getPath());

    Discovery.discoverDataframe(dataframeTwoTypesInSameColumn);

    assertThat(dataframeTwoTypesInSameColumn).isNotNull();
    assertThat(dataframeTwoTypesInSameColumn.dtypes()).contains(new Tuple2<>("id", DataTypes.IntegerType.toString()));
    assertThat(dataframeTwoTypesInSameColumn.dtypes()).contains(new Tuple2<>("label", DataTypes.StringType.toString()));
    assertThat(dataframeTwoTypesInSameColumn.dtypes()).contains(new Tuple2<>("value", DataTypes.StringType.toString()));
    assertThat(dataframeTwoTypesInSameColumn.dtypes()).contains(new Tuple2<>("date", DataTypes.StringType.toString()));

    final Dataset<Row> dataframeToJoin = spark.read()
          .format("csv")
          .option("sep", ";")
          .option("header", true)
          .option("timestampFormat", "dd/MM/yyyy")
          .option("inferSchema", true)
          .load(urlToJoin.toURI().getPath());
    final Dataset<Row> dataframeJoin = dataframe.join(
          dataframeToJoin,
          dataframeToJoin.col("basic_id").equalTo(dataframe.col("id")),
          "inner"
    );

    Discovery.discoverDataframe(dataframeJoin);

    assertThat(dataframeJoin).isNotNull();
    assertThat(dataframeJoin.dtypes()).contains(new Tuple2<>("id", DataTypes.IntegerType.toString()));
    assertThat(dataframeJoin.dtypes()).contains(new Tuple2<>("label", DataTypes.StringType.toString()));
    assertThat(dataframeJoin.dtypes()).contains(new Tuple2<>("value", DataTypes.DoubleType.toString()));
    assertThat(dataframeJoin.dtypes()).contains(new Tuple2<>("date", DataTypes.TimestampType.toString()));
    assertThat(dataframeJoin.dtypes()).contains(new Tuple2<>("join_id", DataTypes.IntegerType.toString()));
    assertThat(dataframeJoin.dtypes()).contains(new Tuple2<>("join_value", DataTypes.DoubleType.toString()));
    assertThat(dataframeJoin.dtypes()).contains(new Tuple2<>("join_date", DataTypes.TimestampType.toString()));

    System.out.println(urlCalculate);
    System.out.println(urlCalculate.toURI().getPath());

    Dataset<Row> dataframeCalculate = spark.read()
          .format("csv")
          .option("sep", ";")
          .option("header", true)
          .option("timestampFormat", "dd/MM/yyyy")
          .option("inferSchema", true)
          .load(urlCalculate.toURI().getPath());
    dataframeCalculate = dataframeCalculate
          .withColumn("val1_minus_val2",dataframeCalculate.col("val1").minus(dataframeCalculate.col("val2")))
          .withColumn("val1_equal_val2",dataframeCalculate.col("val1").equalTo(dataframeCalculate.col("val2")));

    Discovery.discoverDataframe(dataframeCalculate);

    assertThat(dataframeCalculate).isNotNull();
    assertThat(dataframeCalculate.dtypes()).contains(new Tuple2<>("id", DataTypes.IntegerType.toString()));
    assertThat(dataframeCalculate.dtypes()).contains(new Tuple2<>("val1", DataTypes.IntegerType.toString()));
    assertThat(dataframeCalculate.dtypes()).contains(new Tuple2<>("val2", DataTypes.DoubleType.toString()));
    assertThat(dataframeCalculate.dtypes()).contains(new Tuple2<>("val1_minus_val2", DataTypes.DoubleType.toString()));
    assertThat(dataframeCalculate.dtypes()).contains(new Tuple2<>("val1_equal_val2", DataTypes.BooleanType.toString()));
  }
}
