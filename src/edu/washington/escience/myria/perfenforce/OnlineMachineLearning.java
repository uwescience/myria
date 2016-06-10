/**
 *
 */
package edu.washington.escience.myria.perfenforce;

import edu.washington.escience.myria.perfenforce.encoding.ScalingStatusEncoding;

/**
 * 
 */
public class OnlineMachineLearning implements ScalingAlgorithm {

  double lr;
  double queryPrediction;
  int currentClusterSize;

  public OnlineMachineLearning(final int currentClusterSize, final double lr) {
    this.lr = lr;
    this.currentClusterSize = currentClusterSize;
    queryPrediction = 0;
  }

  public void setLR(final double lr) {
    this.lr = lr;
  }

  @Override
  public void step(final QueryMetaData q) {
    // queryPrediction modified here
  }

  @Override
  public int getCurrentClusterSize() {
    return currentClusterSize;
  }

  @Override
  public ScalingStatusEncoding getScalingStatus() {
    ScalingStatusEncoding statusEncoding = new ScalingStatusEncoding();
    statusEncoding.OMLPrediction = queryPrediction;
    return statusEncoding;
  }

}