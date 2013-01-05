package edu.washington.escience.myriad.util;

public class MathUtils {

  private MathUtils() {
  }

  public static int numBinaryOnesInInteger(int v) {
    int result = 0;
    while (v != 0) {
      result += (v & 0x01);
      v = v >>> 1;
    }
    return result;
  }

}
