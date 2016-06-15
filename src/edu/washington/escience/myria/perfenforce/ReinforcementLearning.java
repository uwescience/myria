/**
 *
 */
package edu.washington.escience.myria.perfenforce;

import java.util.List;

import org.slf4j.LoggerFactory;

import edu.washington.escience.myria.perfenforce.encoding.ScalingStatusEncoding;

/**
 * 
 */
public class ReinforcementLearning implements ScalingAlgorithm {

  double alpha;
  double beta;

  int currentClusterSize;
  int currentStateIndex;
  List<Integer> configs;

  Double[] activeStateRatios;

  protected static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ReinforcementLearning.class);

  public ReinforcementLearning(final List<Integer> configs, final int currentClusterSize, final double alpha,
      final double beta) {
    this.alpha = alpha;
    this.beta = beta;
    this.configs = configs;
    this.currentClusterSize = currentClusterSize;
    currentStateIndex = configs.indexOf(currentClusterSize);

    initializeActiveStates();
  }

  public void initializeActiveStates() {
    activeStateRatios = new Double[configs.size()];

    for (int i = 0; i < configs.size(); i++) {
      if (i == currentStateIndex) {
        activeStateRatios[i] = 1.0;
      } else {
        activeStateRatios[i] = -1.0;
      }
    }
  }

  public void setAlpha(final double alpha) {
    this.alpha = alpha;
  }

  public void setBeta(final double beta) {
    this.beta = beta;
  }

  /*
   * Receives the query that previously ran
   */
  @Override
  public void step(final QueryMetaData currentQuery) {
    LOGGER.warn("Stepping for RL");

    LOGGER.warn("Current State Index " + currentStateIndex);

    // Find the best state
    int bestState = currentStateIndex;

    // Introduce another cluster state if we have the opportunity
    if (activeStateRatios[bestState] > 1 && bestState < configs.size() - 1) {
      if (activeStateRatios[bestState + 1] == -1) {
        LOGGER.warn("CONDITION CHANGE " + (bestState + 1));
        currentClusterSize = configs.get(bestState + 1);
        currentStateIndex = configs.indexOf(currentClusterSize);
        activeStateRatios[currentStateIndex] = 1.0;
      }
    } else if (activeStateRatios[bestState] < 1 && bestState > 0) {
      if (activeStateRatios[bestState - 1] == -1) {
        LOGGER.warn("CONDITION CHANGE " + (bestState - 1));
        currentClusterSize = configs.get(bestState - 1);
        currentStateIndex = configs.indexOf(currentClusterSize);
        activeStateRatios[currentStateIndex] = 1.0;
      }
    } else {
      LOGGER.warn("CONDITION CHANGE " + bestState);
      currentClusterSize = configs.get(bestState);
      currentStateIndex = configs.indexOf(currentClusterSize);
    }

    // Resulting runtime
    double ratio = currentQuery.runtimes.get(currentStateIndex) / currentQuery.slaRuntime;

    // Make the correction based on alpha
    double oldRatio = activeStateRatios[currentStateIndex];
    double newRatio = ratio;
    activeStateRatios[currentStateIndex] = alpha * (newRatio - oldRatio) + oldRatio;

    // For all other states, make a beta change
    for (int a = 0; a < activeStateRatios.length; a++) {
      if (a != currentStateIndex && activeStateRatios[a] != -1) {
        activeStateRatios[a] =
            beta
                * (newRatio * ((1.0 * configs.get(currentStateIndex) / configs.get(a)) - activeStateRatios[a]) + activeStateRatios[a]);
      }
    }

    double bestRatioScore = Double.MIN_VALUE;
    for (int a = 0; a < activeStateRatios.length; a++) {
      double stateCalculateReward = closeToOneScore(activeStateRatios[a]);
      if (stateCalculateReward > bestRatioScore) {
        bestRatioScore = stateCalculateReward;
        bestState = a;
      }
    }

    LOGGER.warn("BEST STATE FOUND " + bestState);

  }

  public double closeToOneScore(final double ratio) {
    if (ratio == 1.0) {
      return Double.MAX_VALUE;
    } else {
      return Math.abs(1 / (ratio - 1.0));
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
