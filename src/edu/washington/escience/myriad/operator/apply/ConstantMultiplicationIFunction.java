package edu.washington.escience.myriad.operator.apply;

import edu.washington.escience.myriad.Type;

/**
 * Constant multiplication Function for use in Apply
 * 
 * @param <Tin>
 *          Type of the data being fed and output
 */

public class ConstantMultiplicationIFunction implements IFunction {

  private final int constant;

  /**
   * creates the Constant Multiplication function with the desired power value
   * 
   * @param constant
   *          the constant to multiply through
   */
  public ConstantMultiplicationIFunction(int constant) {
    this.constant = constant;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Type getResultType(Type srcField) {
    return srcField;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Number execute(Number src) {
    if (src instanceof Long) {
      return constant * src.longValue();
    } else if (src instanceof Integer) {
      return constant * src.intValue();
    } else {
      return constant * src.doubleValue();
    }
  }

  @Override
  public String toString() {
    return constant + " * ";
  }
}
