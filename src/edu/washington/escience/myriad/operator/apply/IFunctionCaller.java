package edu.washington.escience.myriad.operator.apply;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.Type;

public class IFunctionCaller {

  private final IFunction function;
  private final List<Integer> applyFields;
  private final List<Number> otherArguments;

  public IFunctionCaller(IFunction function, List<? extends Number> arguments) {
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
   * Determines what should the Type of the resulting field be
   * 
   * @param srcField
   *          the type of the source field
   * @return the desired type of the resulting field
   */
  public Type getResultType(List<Type> srcField) {
    return function.getResultType(srcField);
  }

  /**
   * @return a list containing the apply fields
   */
  public List<Integer> getApplyField() {
    return ImmutableList.copyOf(applyFields);
  }

  /**
   * Executes the function on the src value(s), and return the result out
   * 
   * @param src
   *          the arguments to be processed by the function
   * @throws IllegalArgumentException
   *           , if the src.size() != the number of required arguments
   * @return the result after the function has been applied
   */
  public Number execute(List<Number> src) {
    if (src.size() != applyFields.size()) {
      throw new IllegalArgumentException(
          "The numbers put in doesn't match with the required in the field");
    }
    return function.execute(src, ImmutableList.copyOf(otherArguments));
  }

  public String toString(List<String> names) {
    return function.toString(names, otherArguments);
  }
}
