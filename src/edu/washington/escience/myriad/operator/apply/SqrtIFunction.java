package edu.washington.escience.myriad.operator.apply;

import edu.washington.escience.myriad.Type;

/**
 * SQRT Function sqrt(x)
 */
public class SqrtIFunction implements IFunction {

  /**
   * Executes the function on src
   * 
   * @return Math.sqrt(src), return type will always be Double
   */
  @Override
  public Number execute(Number src) {
    return Math.sqrt(src.doubleValue());
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
