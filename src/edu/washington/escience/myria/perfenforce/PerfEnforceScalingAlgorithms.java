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

  // Params should go to their respective classes
  double alpha;
  double beta;

  double kp;
  double ki;

  double lr;

  public PerfEnforceScalingAlgorithms(final InitializeScalingEncoding scalingEncoding) {
    tier = scalingEncoding.tierSelected;
    /*
     * scalingAlgorithm = scalingEncoding.scalingAlgorithm.name;
     * 
     * switch (scalingAlgorithm) { case "RL": alpha = scalingEncoding.scalingAlgorithm.alpha; beta =
     * scalingEncoding.scalingAlgorithm.beta; break; case "PI": kp = scalingEncoding.scalingAlgorithm.kp; ki =
     * scalingEncoding.scalingAlgorithm.ki; case "OML": lr = scalingEncoding.scalingAlgorithm.lr; break; }
     */

    // Other params
    configs = Arrays.asList(4, 6, 8, 10, 12);
    ithQuerySequence = 0; // ensures a sequence restart
    currentClusterSize = configs.get(tier);
  }

  public int getCurrentClusterSize() {
    return currentClusterSize;
  }

  public int getQueryID() {
    return ithQuerySequence;
  }

}
