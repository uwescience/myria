/**
 *
 */
package edu.washington.escience.myria.perfenforce;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.LoggerFactory;

import scala.util.Random;
import edu.washington.escience.myria.perfenforce.encoding.ScalingStatusEncoding;

/**
 * 
 */
public class ReinforcementLearning implements ScalingAlgorithm {

  int alpha;
  int beta;
  List<Integer> activeStates;
  int currentClusterSize;

  protected static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ReinforcementLearning.class);

  public ReinforcementLearning(final int currentClusterSize, final int alpha, final int beta) {
    this.alpha = alpha;
    this.beta = beta;
    this.currentClusterSize = currentClusterSize;
    activeStates = new ArrayList<Integer>();
  }

  public void setAlpha(final int alpha) {
    this.alpha = alpha;
  }

  public void setBeta(final int beta) {
    this.beta = beta;
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

  @Override
  public int getCurrentClusterSize() {
    return currentClusterSize;
  }

  @Override
  public ScalingStatusEncoding getScalingStatus() {
    ScalingStatusEncoding statusEncoding = new ScalingStatusEncoding();
    statusEncoding.RLActiveStates = activeStates;
    return statusEncoding;
  }
}
