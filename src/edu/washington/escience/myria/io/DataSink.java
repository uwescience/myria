package edu.washington.escience.myria.io;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * An interface for any sink. This interface should be the principal parameter to any operator that produces tuples for
 * a destination (HDFS, S3, files, etc)
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "dataType")
@JsonSubTypes({
  @Type(name = "URI", value = UriSink.class),
  @Type(name = "Pipe", value = PipeSink.class),
  @Type(name = "Bytes", value = ByteSink.class)
})
public interface DataSink extends Serializable {
  /**
   * Returns an {@link OutputStream} providing write access to the specified data destination.
   *
   * @return an {@link OutputStream} providing write access to the specified data destination.
   * @throws IOException if there is an error producing the input stream.
   */
  OutputStream getOutputStream() throws IOException;
}
