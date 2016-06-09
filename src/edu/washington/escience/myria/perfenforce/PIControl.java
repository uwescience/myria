/**
 *
 */
package edu.washington.escience.myria.perfenforce;

import edu.washington.escience.myria.perfenforce.encoding.ScalingStatusEncoding;

/**
 * 
 */
public class PIControl implements ScalingAlgorithm {

  int kp;
  int ki;
  int w;
  int currentClusterSize;
  double ut;

  public PIControl(final int currentClusterSize, final int kp, final int ki, final int w) {
    this.kp = kp;
    this.ki = ki;
    this.w = w;
    this.currentClusterSize = currentClusterSize;
    ut = currentClusterSize;
  }

  public void setKP(final int kp) {
    this.kp = kp;
  }

  public void setKI(final int ki) {
    this.ki = ki;
  }

  public void setW(final int w) {
    this.w = w;
  }

  @Override
  public void step(final QueryMetaData q) {

  }

  @Override
  public int getCurrentClusterSize() {
    return currentClusterSize;
  }

  @Override
  public ScalingStatusEncoding getScalingStatus() {
    ScalingStatusEncoding statusEncoding = new ScalingStatusEncoding();
    statusEncoding.PIControlUT = ut;
    return statusEncoding;
  }

}
