package edu.washington.escience.myriad.operator.apply;

import java.util.List;

/**
 * Constant multiplication Function for use in Apply
 * 
 * @param <Tin>
 *          Type of the data being fed and output
 */

public class ConstantMultiplicationIFunction extends IFunction {

  /**
   * creates the Constant Multiplication function with the desired power value
   * 
   * @param constant
   *          the constant to multiply through
   */
  public ConstantMultiplicationIFunction() {
  }

  /**
   * {@inheritDoc}
   * 
   * @throws IllegalArgumentException
   *           , the wrong argument was passed
   */
  @Override
  public Number execute(List<Number> source, List<Number> arguments) {
    checkPreconditions(source, numApplyFields());
    checkPreconditions(arguments, numExtraArgument());
    Number src = source.get(0);
    Number constant = arguments.get(0);
    if (src instanceof Long) {
      return constant.longValue() * src.longValue();
    } else if (src instanceof Integer) {
      return constant.intValue() * src.intValue();
    } else {
      return constant.doubleValue() * src.doubleValue();
    }
  }

  @Override
  public String toString(List<String> names, List<Number> arguments) {
    checkPreconditions(arguments, numExtraArgument());
    checkPreconditions(names, numApplyFields());
    return arguments.get(0) + " * " + names.get(0);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int numApplyFields() {
    return 1;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int numExtraArgument() {
    return 1;
  }
}
