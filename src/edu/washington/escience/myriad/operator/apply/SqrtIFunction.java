package edu.washington.escience.myriad.operator.apply;

import edu.washington.escience.myriad.Type;

/**
 * SQRT Function sqrt(x)
 */
public class SqrtIFunction implements IFunction<Double, Number> {

  /**
   * Executes the function on src
   * 
   * @return Math.sqrt(src), return type will always be Double
   */
  @Override
  public Double execute(Number src) {
    double tmp = src.doubleValue();
    return Math.sqrt(tmp);
  }

  @Override
  public String toString() {
    return "SQRT";
  }

  /**
   * @see IFunction
   */
  @Override
  public Type getResultType(Type srcField) {
    return Type.DOUBLE_TYPE;
  }

}
