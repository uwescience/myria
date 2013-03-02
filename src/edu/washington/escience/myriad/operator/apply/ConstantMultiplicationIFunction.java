package edu.washington.escience.myriad.operator.apply;

import edu.washington.escience.myriad.Type;

/**
 * Constant multiplication Function for use in Apply
 * 
 * @param <Tin>
 *          Type of the data being fed and output
 */

public class ConstantMultiplicationIFunction<Tin extends Number> implements
    IFunction<Tin, Tin> {

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
  @SuppressWarnings("unchecked")
  @Override
  public Tin execute(Tin src) {
    if (src instanceof Long) {
      return (Tin) (Long) (constant * src.longValue());
    } else if (src instanceof Integer) {
      return (Tin) (Integer) (constant * src.intValue());
    } else {
      return (Tin) (Double) (constant * src.doubleValue());
    }
  }

  @Override
  public String toString() {
    return constant + " * ";
  }
}
