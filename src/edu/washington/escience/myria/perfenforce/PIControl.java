/**
 *
 */
package edu.washington.escience.myria.perfenforce;

/**
 * 
 */
public class PIControl implements ScalingAlgorithm {

  int kp;
  int ki;
  int w;
  int currentClusterSize;

  public PIControl(final int currentClusterSize, final int kp, final int ki, final int w) {
    this.kp = kp;
    this.ki = ki;
    this.w = w;
    this.currentClusterSize = currentClusterSize;
  }

  @Override
  public void step() {

  }

  @Override
  public int getCurrentClusterSize() {
    return currentClusterSize;
  }

}
