package edu.washington.escience.myria.io;

import java.io.IOException;
import java.io.InputStream;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * An interface for any source of bits. This interface should be the principal parameter to any operator that produces
 * tuples from input data, e.g., files, the web, or other.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "dataType")
@JsonSubTypes({
  @Type(name = "Bytes", value = ByteArraySource.class),
  @Type(name = "File", value = FileSource.class),
  @Type(name = "URI", value = UriSource.class),
  @Type(name = "Empty", value = EmptySource.class)
})
public interface DataSource {
  /**
   * Returns an {@link InputStream} providing read access to the bits in the specified data source.
   *
   * @return an {@link InputStream} providing read access to the bits in the specified data source.
   * @throws IOException if there is an error producing the input stream.
   */
  InputStream getInputStream() throws IOException;
}
