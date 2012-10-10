package edu.washington.escience.myriad.datalog.syntax;

public class DatalogParamStringConstant extends DatalogParamConstant {
  String constantValue;

  public DatalogParamStringConstant(final String s) {
    constantValue = s;
  }

  public String getConstantValue() {
    return constantValue;
  }

  @Override
  public String toString() {
    final String res = "Constant(" + constantValue + ")";
    return res;
  }
}