package edu.washington.escience.myria.operator;

import java.util.InputMismatchException;
import java.util.Scanner;

import edu.washington.escience.myria.TupleBatchBuffer;

/**
 * Interface for evaluating janino expressions.
 */
public interface TextTupleReader {
  /**
   * Method to scan a tuple in {@link FileScan}.
   * 
   * @param buffer the tuple buffer the data should be read into
   * @param scanner the scanner to read from
   * @throws InputMismatchException if the data does not confirm to the expected schema
   */
  void read(TupleBatchBuffer buffer, Scanner scanner) throws InputMismatchException;
}
