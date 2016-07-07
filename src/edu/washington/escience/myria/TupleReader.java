/**
 *
 */
package edu.washington.escience.myria;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import edu.washington.escience.myria.operator.TupleSource;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 *
 */
@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "readerType"
)
@JsonSubTypes({
  @Type(name = "Csv", value = CsvTupleReader.class),
  @Type(name = "Binary", value = BinaryTupleReader.class)
})
public interface TupleReader extends Serializable {
  /**
   * This will initialize the {@link TupleReader} InputStream from the DataSource
   */
  void open(InputStream stream) throws IOException, DbException;

  /**
   * Parses tuples from the DataSource InputStream
   */
  TupleBatch readTuples() throws IOException, DbException;

  /**
   * Clears the buffer making sure all tuples are read from the source
   */
  void done() throws IOException;

  /**
   * Provides the schema to the {@link TupleSource} operator
   */
  Schema getSchema();
}
