package edu.washington.escience.myriad.operator.apply;

import java.util.List;

/**
 * Constant multiplication Function for use in Apply.
 */

public final class ConstantMultiplicationIFunction extends IFunction {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * creates the Constant Multiplication function with the desired power value.
   */
  public ConstantMultiplicationIFunction() {
  }

  /**
   * {@inheritDoc}
   * 
   * @throws IllegalArgumentException , the wrong argument was passed
   */
  @Override
  public Number execute(final List<Number> source, final List<Number> arguments) {
    checkPreconditions(source, numApplyFields());
    checkPreconditions(arguments, numExtraArgument());
    Number src = source.get(0);
    Number constant = arguments.get(0);
    if (src instanceof Long) {
      return constant.longValue() * src.longValue();
    } else if (src instanceof Integer) {
      return constant.intValue() * src.intValue();
    } else if (src instanceof Float) {
      return constant.floatValue() * src.floatValue();
    } else if (src instanceof Double) {
      return constant.doubleValue() * src.doubleValue();
    } else {
      throw new IllegalArgumentException("Unexpected src of type " + src.getClass());
    }
  }

  @Override
  public String toString(final List<String> names, final List<Number> arguments) {
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
