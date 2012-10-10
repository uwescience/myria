package edu.washington.escience.myriad.datalog.syntax;

public class DatalogParamIntegerConstant extends DatalogParamConstant {
  int constantValue;

  public DatalogParamIntegerConstant(final int s) {
    constantValue = s;
  }

  public int getConstantValue() {
    return constantValue;
  }

  @Override
  public String toString() {
    final String res = "Constant(" + constantValue + ")";
    return res;
  }
}