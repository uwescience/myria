package edu.washington.escience.myria.operator;

import java.io.DataInput;
import java.io.IOException;

import edu.washington.escience.myria.TupleBatchBuffer;

/**
 * Interface for evaluating janino expressions.
 */
public interface TupleReader {
  /**
   * 
   * @param buffer the tuple buffer the data should be read into
   * @param dataInput the data input
   * @throws IOException on io errors
   */
  void read(TupleBatchBuffer buffer, DataInput dataInput) throws IOException;
}
