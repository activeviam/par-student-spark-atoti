package io.atoti.spark;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URISyntaxException;
import java.net.URL;
import java.util.Objects;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Test;

import scala.Tuple2;

public class TestDiscovery {
	
	SparkSession spark = SparkSession.builder().appName("Spark Atoti").config("spark.master", "local").getOrCreate();

  @Test
  void testDiscovery() throws URISyntaxException {
    final ClassLoader cl = Thread.currentThread().getContextClassLoader();
    final URL url = Objects.requireNonNull(cl.getResource("csv/basic.csv"), "Cannot find file");
    
    System.out.println(url);

    System.out.println(url.toURI().getPath());
	final Dataset<Row> dataframe = spark.read()
			.format("csv")
			.option("sep", ";")
			.option("header", true)
			.option("dateFormat", "dd/MM/yyyy")
            .option("inferSchema", true)
            .load(url.toURI().getPath());
    Discovery.discoverDataframe(dataframe);

    assertThat(dataframe).isNotNull();
    assertThat(dataframe.dtypes()).contains(new Tuple2<String, String>("id", DataTypes.IntegerType.toString()));
    assertThat(dataframe.dtypes()).contains(new Tuple2<String, String>("label", DataTypes.StringType.toString()));
    assertThat(dataframe.dtypes()).contains(new Tuple2<String, String>("value", DataTypes.DoubleType.toString()));
  }
}
