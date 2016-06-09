/**
 *
 */
package edu.washington.escience.myria.perfenforce;

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
  Double[] activeStateRatios;
  int currentClusterSize;
  int clusterSizeIndex;
  List<Integer> configs;

  protected static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ReinforcementLearning.class);

  public ReinforcementLearning(final List<Integer> configs, final int currentClusterSize, final int alpha,
      final int beta) {
    this.alpha = alpha;
    this.beta = beta;
    this.configs = configs;
    this.currentClusterSize = currentClusterSize;
    activeStateRatios = new Double[configs.size()];

    clusterSizeIndex = configs.indexOf(currentClusterSize);
    for (int i = 0; i < configs.size(); i++) {
      if (i == clusterSizeIndex) {
        activeStateRatios[i] = 1.0;
      } else {
        activeStateRatios[i] = 0.0;
      }
    }

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

    double ratio = currentQuery.runtimes.get(clusterSizeIndex) / currentQuery.slaRuntime;

    // Make the correction based on alpha
    double oldRatio = activeStateRatios[clusterSizeIndex];
    double newRatio = ratio;
    activeStateRatios[clusterSizeIndex] = alpha * (newRatio - oldRatio) + oldRatio;

    // For all other states, make a beta change
    for (int a = 0; a < activeStateRatios.length; a++) {
      if (a != clusterSizeIndex) {
        activeStateRatios[a] =
            beta
                * (newRatio * ((1.0 * configs.get(clusterSizeIndex) / configs.get(a)) - activeStateRatios[a]) + activeStateRatios[a]);
      }
    }
    // Find the best state
    int bestState = 0;
    double bestRatioScore = -1000;
    for (int a = 0; a < activeStateRatios.length; a++) {
      double stateCalculateReward = closeToOneScore(activeStateRatios[a]);

      if (stateCalculateReward > bestRatioScore) {
        bestRatioScore = stateCalculateReward;
        bestState = a;
      }
    }
    // Introduce another cluster state
    if (activeStateRatios[bestState] > 1 && configs.get(bestState) < Collections.max(configs)) {
      currentClusterSize = configs.get(bestState + 1);
      clusterSizeIndex = configs.indexOf(currentClusterSize);
      activeStateRatios[clusterSizeIndex] = .999;
    } else if (activeStateRatios[bestState] < 1 && configs.get(bestState) > Collections.min(configs)) {
      currentClusterSize = bestState - 1;
      clusterSizeIndex = configs.indexOf(currentClusterSize);
      activeStateRatios[clusterSizeIndex] = .999;;
    } else {
      currentClusterSize = configs.get(bestState);
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
