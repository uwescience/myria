/**
 *
 */
package edu.washington.escience.myria.perfenforce;

import java.util.List;

import edu.washington.escience.myria.perfenforce.encoding.ScalingStatusEncoding;

/**
 * 
 */
public class OnlineMachineLearning implements ScalingAlgorithm {

  double lr;
  Double[] queryPredictions;
  int currentClusterSize;
  List<Integer> configs;

  public OnlineMachineLearning(final List<Integer> configs, final int currentClusterSize, final double lr) {
    this.lr = lr;
    this.currentClusterSize = currentClusterSize;
    this.configs = configs;
    queryPredictions = new Double[configs.size()];
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
    statusEncoding.OMLPredictions = queryPredictions;
    return statusEncoding;
  }

}