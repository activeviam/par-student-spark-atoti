package io.atoti.spark;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Objects;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CsvReader {
    public static Dataset<Row> read(String path, SparkSession session) {
        final ClassLoader cl = Thread.currentThread().getContextClassLoader();
        final URL url = Objects.requireNonNull(cl.getResource(path), "Cannot find file");

        Dataset<Row> dataframe;
		try {
			dataframe = session
                .read()
                .format("csv")
                .option("sep", ";")
                .option("header", true)
                .option("timestampFormat", "dd/MM/yyyy")
                .option("inferSchema", true)
                .load(url.toURI().getPath());
		} catch (URISyntaxException e) {
			throw new IllegalArgumentException("Failed to read csv " + path, e);
		}

        return dataframe;
    }
}
