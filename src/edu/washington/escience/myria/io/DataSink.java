/**
 *
 */
package edu.washington.escience.myria.io;

import java.io.IOException;
import java.io.OutputStream;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * An interface for any sink of bits. This interface should be the principal parameter to any operator that produces
 * tuples for a destination (HDFS, S3, files, etc)
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "dataType")
@JsonSubTypes({
    @Type(name = "Bytes", value = ByteArraySource.class), @Type(name = "File", value = FileSource.class),
    @Type(name = "URI", value = UriSource.class), @Type(name = "Empty", value = EmptySource.class) })
public interface DataSink {
  /**
   * Returns an {@link OutputStream} providing write access to the bits in the specified data destination.
   * 
   * @return an {@link OutputStream} providing read access to the bits in the specified data destination.
   * @throws IOException if there is an error producing the input stream.
   */
  OutputStream getOutputStream() throws IOException;

}
