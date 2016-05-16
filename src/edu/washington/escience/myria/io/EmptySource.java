package edu.washington.escience.myria.io;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * A data source with no tuples.
 *
 * @author whitaker
 */
public class EmptySource implements DataSource {

  @Override
  public InputStream getInputStream() throws IOException {
    return new ByteArrayInputStream(new byte[0]);
  }
}
