package edu.washington.escience.myria;

import java.io.IOException;
import java.util.List;

/**
 * A {@link TupleWriter} is an object that can serialize a {@link TupleBatch}. The assumption is that most classes that
 * implement {@link TupleWriter} will write the data to an {@link java.io.OutputStream}, e.g.
 * 
 * There are three functions:
 * 
 * <ul>
 * <li>{@link writeColumnHeaders} - called once at the start of a batch of tuples to enable the writer to label columns
 * if desired.</li>
 * <li>{@link writeTuples} - called when there are new tuples to serialize.</li>
 * <li>{@link done} - called when there will be no more tuples.</li>
 * </ul>
 * 
 * @author dhalperi
 * 
 */
public interface TupleWriter {

  /**
   * Inform the TupleWriter of the column headers. In the standard case (CSV output, see {@link CsvTupleWriter}), they
   * are written out directly. In some cases, they may be cached and output with the data (see {@link JsonTupleWriter}).
   * 
   * @param columnNames the names of the columns.
   * @throws IOException if there is an error writing the tuples.
   */
  void writeColumnHeaders(List<String> columnNames) throws IOException;

  /**
   * Provide a TupleWriter with tuples to serialize.
   * 
   * @param tuples the batch of tuples to be serialized.
   * @throws IOException if there is an error writing the tuples.
   */
  void writeTuples(TupleBatch tuples) throws IOException;

  /**
   * Inform the TupleWriter that no more tuples will come. It may close and flush an {@link java.io.OutputStream}, for
   * example.
   * 
   * @throws IOException if there is an error writing the tuples.
   */
  void done() throws IOException;

}
