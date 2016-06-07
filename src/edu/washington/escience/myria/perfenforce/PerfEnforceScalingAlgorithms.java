/**
 *
 */
package edu.washington.escience.myria.perfenforce;

import java.util.Arrays;
import java.util.List;

import edu.washington.escience.myria.perfenforce.encoding.InitializeScalingEncoding;

/**
 * 
 */
public class PerfEnforceScalingAlgorithms {

  int tier;
  int ithQuerySequence;
  int currentClusterSize;
  List<Integer> configs;
  QueryMetaData currentQuery;

  ScalingAlgorithm scalingAlgorithm;

  public PerfEnforceScalingAlgorithms(final InitializeScalingEncoding scalingEncoding) {
    tier = scalingEncoding.tierSelected;

    // Only because they all share the same encoding
    switch (scalingEncoding.scalingAlgorithm.name) {
      case "RL":
        scalingAlgorithm =
            new ReinforcementLearning(currentClusterSize, scalingEncoding.scalingAlgorithm.alpha,
                scalingEncoding.scalingAlgorithm.beta);
        break;
      case "PI":
        break;
      case "OML":
        break;
    }
    // Other params
    configs = Arrays.asList(4, 6, 8, 10, 12);
    ithQuerySequence = 0; // ensures a sequence restart
    currentClusterSize = configs.get(tier);
  }

  public int getCurrentClusterSize() {
    return currentClusterSize;
  }

  public int getCurrentQueryIdealSize() {
    return currentQuery.idealClusterSize;
  }

  public int getQueryCounter() {
    return ithQuerySequence;
  }

  public void setCurrentQuery(final QueryMetaData currentQuery) {
    this.currentQuery = currentQuery;
  }

  public void incrementQueryCounter() {
    ithQuerySequence++;
  }

  public void step() {
    scalingAlgorithm.step();
    currentClusterSize = scalingAlgorithm.getCurrentClusterSize();
  }

}
