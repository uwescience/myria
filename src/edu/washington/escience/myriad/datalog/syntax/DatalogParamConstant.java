package edu.washington.escience.myriad.datalog.syntax;

public class DatalogParamConstant extends DatalogParamValue {

  @Override
  public final boolean isVariable() {
    return false;
  }

  @Override
  public final boolean isConstant() {
    return true;
  }

  @Override
  public String toString() {
    return "Constant()";
  }
}