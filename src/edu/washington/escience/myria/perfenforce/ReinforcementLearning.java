/**
 *
 */
package edu.washington.escience.myria.perfenforce;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.LoggerFactory;

import edu.washington.escience.myria.perfenforce.encoding.ScalingStatusEncoding;

/**
 * 
 */
public class ReinforcementLearning implements ScalingAlgorithm {

  int alpha;
  int beta;
  List<Double> activeStateRatios;
  int currentClusterSize;
  List<Integer> configs;

  protected static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ReinforcementLearning.class);

  public ReinforcementLearning(final List<Integer> configs, final int currentClusterSize, final int alpha,
      final int beta) {
    this.alpha = alpha;
    this.beta = beta;
    this.configs = configs;
    this.currentClusterSize = currentClusterSize;
    activeStateRatios = new ArrayList<Double>();
  }

  public void setAlpha(final int alpha) {
    this.alpha = alpha;
  }

  public void setBeta(final int beta) {
    this.beta = beta;
  }

  /*
   * Receives the query that previously ran
   */
  @Override
  public void step(final QueryMetaData currentQuery) {
    LOGGER.warn("Stepping for RL");
    double ratio = currentQuery.runtimes.get(currentClusterSize) / currentQuery.slaRuntime;

    // Make the correction based on alpha
    double oldRatio = activeStateRatios.get(currentClusterSize);
    double newRatio = ratio;
    activeStateRatios.set(currentClusterSize, alpha * (newRatio - oldRatio) + oldRatio);

    // For all other states, make a beta change
    for (int a = 0; a < activeStateRatios.size(); a++) {
      if (a != currentClusterSize) {
        activeStateRatios
            .set(
                a,
                beta
                    * (newRatio * ((1.0 * configs.get(currentClusterSize) / configs.get(a)) - activeStateRatios.get(a)) + activeStateRatios
                        .get(a)));
      }
    }
    // Find the best state
    int bestState = 0;
    double bestRatioScore = -1000;
    for (int a = 0; a < activeStateRatios.size(); a++) {
      double stateCalculateReward = closeToOneScore(activeStateRatios.get(a));

      if (stateCalculateReward > bestRatioScore) {
        bestRatioScore = stateCalculateReward;
        bestState = a;
      }
    }
    // Introduce another cluster state
    if (activeStateRatios.get(bestState) > 1 && configs.get(bestState) < Collections.max(configs)) {
      currentClusterSize = bestState + 1;
      activeStateRatios.add(currentClusterSize, .999);
    } else if (activeStateRatios.get(bestState) < 1 && configs.get(bestState) > Collections.min(configs)) {
      currentClusterSize = bestState - 1;
      activeStateRatios.add(currentClusterSize, 1.0);
    } else {
      currentClusterSize = bestState;
    }

  }

  public double closeToOneScore(final double ratio) {
    if (ratio == PerfEnforceScalingAlgorithms.SET_POINT) {
      return Double.MAX_VALUE;
    } else {
      return Math.abs(1 / (ratio - PerfEnforceScalingAlgorithms.SET_POINT));
    }
  }

  @Override
  public int getCurrentClusterSize() {
    return currentClusterSize;
  }

  @Override
  public ScalingStatusEncoding getScalingStatus() {
    ScalingStatusEncoding statusEncoding = new ScalingStatusEncoding();
    statusEncoding.RLActiveStates = activeStateRatios;
    return statusEncoding;
  }
}
