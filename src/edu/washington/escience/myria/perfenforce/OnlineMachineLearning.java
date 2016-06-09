/**
 *
 */
package edu.washington.escience.myria.perfenforce;

import edu.washington.escience.myria.perfenforce.encoding.ScalingStatusEncoding;

/**
 * 
 */
public class OnlineMachineLearning implements ScalingAlgorithm {

  int lr;
  double queryPrediction;
  int currentClusterSize;

  public OnlineMachineLearning(final int currentClusterSize, final int lr) {
    this.lr = lr;
    this.currentClusterSize = currentClusterSize;
    queryPrediction = 0;
  }

  public void setLR(final int lr) {
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