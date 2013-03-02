package edu.washington.escience.myriad.operator.apply;

import edu.washington.escience.myriad.Type;

/**
 * Power Function for use in Apply
 * 
 * @param <Tin>
 *          Type of the data being fed and output
 */
public class PowIFunction implements IFunction {

  private final int power;

  /**
   * creates the Power function with the desired power value
   * 
   * @param power
   *          The factor for the src to be powered by
   */
  public PowIFunction(int power) {
    this.power = power;
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
    Number retval = Math.pow(src.doubleValue(), power);
    if (src instanceof Long) {
      return retval.longValue();
    } else if (src instanceof Integer) {
      return retval.intValue();
    }
    return retval;
  }

  @Override
  public String toString() {
    return "POW";
  }

}
