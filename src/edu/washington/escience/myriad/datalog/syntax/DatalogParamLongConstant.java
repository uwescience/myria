package edu.washington.escience.myriad.datalog.syntax;

public class DatalogParamLongConstant extends DatalogParamConstant {
  long constantValue;

  public DatalogParamLongConstant(final long s) {
    constantValue = s;
  }

  public long getConstantValue() {
    return constantValue;
  }

  @Override
  public String toString() {
    final String res = "Constant(" + constantValue + ")";
    return res;
  }
}