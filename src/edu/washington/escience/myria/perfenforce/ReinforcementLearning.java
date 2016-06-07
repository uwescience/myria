/**
 *
 */
package edu.washington.escience.myria.perfenforce;

import org.slf4j.LoggerFactory;

import scala.util.Random;

/**
 * 
 */
public class ReinforcementLearning implements ScalingAlgorithm {

  int alpha;
  int beta;
  int currentClusterSize;

  protected static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ReinforcementLearning.class);

  public ReinforcementLearning(final int currentClusterSize, final int alpha, final int beta) {
    this.alpha = alpha;
    this.beta = beta;
    this.currentClusterSize = currentClusterSize;
  }

  /*
   * For now, just tweak the cluster size to something random
   */
  @Override
  public void step() {
    LOGGER.warn("Stepping for RL");
    Random r = new Random();
    currentClusterSize = r.nextInt(8) + 4;
  }

  /*
   */
  @Override
  public int getCurrentClusterSize() {
    return currentClusterSize;
  }

  public void setAlpha(final int alpha) {
    this.alpha = alpha;
  }

  public void setBeta(final int beta) {
    this.beta = beta;
  }

}
