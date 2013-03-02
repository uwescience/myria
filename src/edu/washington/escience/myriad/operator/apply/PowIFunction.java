package edu.washington.escience.myriad.operator.apply;

import edu.washington.escience.myriad.Type;

/**
 * Power Function for use in Apply
 * 
 * @param <Tin>
 *          Type of the data being fed and output
 */
public class PowIFunction<Tin extends Number> implements IFunction<Tin, Tin> {

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
  @SuppressWarnings("unchecked")
  @Override
  public Tin execute(Tin src) {
    Double result = Math.pow(src.doubleValue(), power);
    Tin retval = (Tin) result;
    if (src instanceof Long) {
      retval = (Tin) ((Long) result.longValue());
    } else if (src instanceof Integer) {
      retval = (Tin) ((Integer) result.intValue());
    }
    return retval;
  }

  @Override
  public String toString() {
    return "POW";
  }

}
