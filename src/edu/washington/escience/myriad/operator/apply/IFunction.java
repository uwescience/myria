package edu.washington.escience.myriad.operator.apply;

import edu.washington.escience.myriad.Type;

/**
 * An interface for Math operations to be used with Apply
 */

public interface IFunction {

  /**
   * Determines what should the Type of the resulting field be
   * 
   * @param srcField
   *          the type of the source field
   * @return the desired type of the resulting field
   */
  Type getResultType(Type srcField);

  /**
   * Executes the function on the value, int src
   * 
   * @param src
   *          the buffer to store the result
   * @return the value after the function is applied on src
   * 
   */
  Number execute(Number src);

}
