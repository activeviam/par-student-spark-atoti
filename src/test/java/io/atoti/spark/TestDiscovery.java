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


public class TestDiscovery {
  SparkSession spark =
      SparkSession.builder().appName("Spark Atoti").config("spark.master", "local").getOrCreate();

  @Test
  void testDiscovery() throws URISyntaxException {
    final ClassLoader cl = Thread.currentThread().getContextClassLoader();
    final URL url = Objects.requireNonNull(cl.getResource("csv/basic.csv"), "Cannot find file");

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
  }
}
