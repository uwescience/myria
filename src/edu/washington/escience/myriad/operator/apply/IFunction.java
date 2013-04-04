package edu.washington.escience.myriad.operator.apply;

import java.util.List;

import edu.washington.escience.myriad.Type;

/**
 * An interface for Math operations to be used with Apply.
 */

public abstract class IFunction {

  /**
   * Determines what should the Type of the resulting field be.
   * 
   * @param srcField the type of the source field
   * @return the desired type of the resulting field
   */
  public Type getResultType(final List<Type> srcField) {
    Type retval = Type.INT_TYPE;
    for (Type eachType : srcField) {
      if (eachType == Type.DOUBLE_TYPE) {
        return Type.DOUBLE_TYPE;
      }
      if (eachType != Type.INT_TYPE) {
        // it can either be a long/float type
        if (retval == Type.LONG_TYPE && eachType == Type.FLOAT_TYPE) {
          retval = eachType;
        } else if (retval == Type.INT_TYPE) {
          retval = eachType;
        }
      }
    }
    return retval;
  }

  /**
   * Executes the function on the value, int src.
   * 
   * @throws IllegalArgumentException , if the source or arguments list size doesn't match the desired size
   * 
   * @param source the list of values to start the computation with
   * @param arguments the list of arguments required for that function
   * @return the value after the function is applied on src
   * 
   */
  public abstract Number execute(List<Number> source, List<Number> arguments);

  /**
   * For example, pow(src, x) will return 1 because only src is needed.
   * 
   * @return the number of fields required to do the apply
   */
  public abstract int numApplyFields();

  /**
   * This number will represent the arguments to execute the function for example, pow(src, x) will return 1, because x
   * is the extra argument.
   * 
   * @return number of arguments to get in order to do the apply
   */
  public abstract int numExtraArgument();

  /**
   * @param names the name of the argument such as x to complete the string representation
   * @param arguments the arguments for toString to generate the string representation correctly
   * @return a string representation of this function
   */
  public abstract String toString(List<String> names, List<Number> arguments);

  /**
   * Checks if the list has at least the expectedSize or not.
   * 
   * @throws IllegalArgumentException , when the condition is not met
   * 
   * @param list the list to be checked
   * @param expectedSize the expected size of the list
   */
  public final void checkPreconditions(final List<?> list, final int expectedSize) {
    if (list.size() < expectedSize) {
      throw new IllegalArgumentException("Expected at least " + expectedSize + ", but got " + list.size() + " numbers.");
    }
  }

}
