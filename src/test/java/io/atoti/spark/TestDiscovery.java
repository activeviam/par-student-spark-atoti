package io.atoti.spark;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Objects;
import org.junit.jupiter.api.Test;

public class TestDiscovery {

  @Test
  void testDiscovery() throws URISyntaxException {
    final ClassLoader cl = Thread.currentThread().getContextClassLoader();
    final var url = Objects.requireNonNull(cl.getResource("csv/basic.csv"), "Cannot find file");
    final var path = Path.of(url.toURI());

    if (true) {
      throw new UnsupportedOperationException("Load the path as as dataframe and ");
    }
    final Object dataframe = null; // convert path to dataframe
    Discovery.discoverDataframe(dataframe);

    assertThat(dataframe).isNotNull();
    // TODO: add more assertions about the discovered values in the dataframe
    // we should see 3 columns: id, label and value
    // we should see that id is a number, label is something string-y and value is a number, ideally
    // floating.
  }
}
