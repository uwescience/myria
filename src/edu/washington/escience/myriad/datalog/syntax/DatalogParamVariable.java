package edu.washington.escience.myriad.datalog.syntax;

import org.apache.commons.lang3.builder.HashCodeBuilder;

public class DatalogParamVariable extends DatalogParamValue implements Comparable<DatalogParamVariable> {
  /** Class-specific magic number used to generate the hash code. */
  private static final int MAGIC_HASHCODE1 = 255;
  /** Class-specific magic number used to generate the hash code. */
  private static final int MAGIC_HASHCODE2 = 69;
  final String variableName;

  public DatalogParamVariable(final String s) {
    variableName = s;
  }

  public final String getVariableName() {
    return variableName;
  }

  @Override
  public final boolean isVariable() {
    return true;
  }

  @Override
  public final boolean isConstant() {
    return false;
  }

  @Override
  public final boolean equals(final Object obj) {
    if (!(obj instanceof DatalogParamVariable)) {
      return false;
    }
    return (this.compareTo((DatalogParamVariable) obj) == 0);
  }

  @Override
  public final int hashCode() {
    final HashCodeBuilder hb = new HashCodeBuilder(MAGIC_HASHCODE1, MAGIC_HASHCODE2);
    hb.append(variableName);
    return hb.toHashCode();
  }

  @Override
  public final int compareTo(final DatalogParamVariable other) {
    return this.variableName.compareTo(other.variableName);
  }

  @Override
  public final String toString() {
    final String res = "Var(" + variableName + ")";
    return res;
  }
}