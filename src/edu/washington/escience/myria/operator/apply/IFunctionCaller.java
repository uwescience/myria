package edu.washington.escience.myria.operator.apply;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Type;

/**
 * A wrapper class for calling IFunctions.
 * 
 * @author vaspol
 * 
 */
public final class IFunctionCaller implements Serializable {

  /** Required by java. */
  private static final long serialVersionUID = 1L;
  /** The underlying function for the caller. */
  private final IFunction function;
  /** apply fields. */
  private final List<Integer> applyFields;
  /** extra arguments for the function. */
  private final List<Number> otherArguments;

  /**
   * Constructs an IFunctionCaller.
   * 
   * @param function the underlying function to be used for this caller
   * @param arguments extra arguments besides from the raw values that needs to be manipulated
   */
  public IFunctionCaller(final IFunction function, final List<? extends Number> arguments) {
    this.function = function;
    applyFields = new ArrayList<Integer>();
    otherArguments = new ArrayList<Number>();
    for (int i = 0; i < function.numApplyFields(); i++) {
      applyFields.add((Integer) arguments.get(i));
    }
    for (int i = 0; i < function.numExtraArgument(); i++) {
      otherArguments.add(arguments.get(function.numApplyFields() + i));
    }
  }

  /**
   * Determines what should the Type of the resulting field be.
   * 
   * @param srcField the type of the source field
   * @return the desired type of the resulting field
   */
  public Type getResultType(final List<Type> srcField) {
    return function.getResultType(srcField);
  }

  /**
   * @return a list containing the apply fields
   */
  public List<Integer> getApplyField() {
    return ImmutableList.copyOf(applyFields);
  }

  /**
   * Executes the function on the src value(s), and return the result out.
   * 
   * @param src the arguments to be processed by the function
   * @throws IllegalArgumentException , if the src.size() != the number of required arguments
   * @return the result after the function has been applied
   */
  public Number execute(final List<Number> src) {
    if (src.size() != applyFields.size()) {
      throw new IllegalArgumentException("The numbers put in doesn't match with the required in the field");
    }
    return function.execute(src, ImmutableList.copyOf(otherArguments));
  }

  /**
   * Generates a string representation of the function.
   * 
   * @param names the names used, the column names
   * @return a string representation of the function
   */
  public String toString(final List<String> names) {
    return function.toString(names, otherArguments);
  }
}
