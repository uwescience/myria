/**
 *
 */
package edu.washington.escience.myria.perfenforce;

/**
 * 
 */
public class OnlineMachineLearning implements ScalingAlgorithm {

  int lr;
  int currentClusterSize;

  public OnlineMachineLearning(final int currentClusterSize, final int lr) {
    this.lr = lr;
    this.currentClusterSize = currentClusterSize;
  }

  /*
   */
  @Override
  public void step() {
  }

  /* 
   */
  @Override
  public int getCurrentClusterSize() {
    return currentClusterSize;
  }

}