/**
 *
 */
package edu.washington.escience.myria.perfenforce;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

import org.slf4j.LoggerFactory;

import edu.washington.escience.myria.perfenforce.encoding.InitializeScalingEncoding;
import edu.washington.escience.myria.perfenforce.encoding.ScalingAlgorithmEncoding;
import edu.washington.escience.myria.perfenforce.encoding.ScalingStatusEncoding;

/**
 * 
 */
public class PerfEnforceScalingAlgorithms {

  int tier;
  int ithQuerySequence;
  int currentClusterSize;
  List<Integer> configs;
  QueryMetaData currentQuery;
  QueryMetaData previousQuery;

  String path;

  ScalingAlgorithm scalingAlgorithm;

  protected static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(PerfEnforceScalingAlgorithms.class);

  public PerfEnforceScalingAlgorithms(final InitializeScalingEncoding scalingEncoding) {
    tier = scalingEncoding.tier;
    path = scalingEncoding.path;

    configs = Arrays.asList(4, 6, 8, 10, 12);
    ithQuerySequence = 0;
    currentClusterSize = configs.get(tier);
    currentQuery = new QueryMetaData();
    previousQuery = new QueryMetaData();

    initializeScalingAlgorithm(scalingEncoding);
  }

  public void initializeScalingAlgorithm(final InitializeScalingEncoding scalingEncoding) {
    switch (scalingEncoding.scalingAlgorithm.name) {
      case "RL":
        scalingAlgorithm =
            new ReinforcementLearning(configs, currentClusterSize, scalingEncoding.scalingAlgorithm.alpha,
                scalingEncoding.scalingAlgorithm.beta);
        break;
      case "PI":
        scalingAlgorithm =
            new PIControl(configs, currentClusterSize, scalingEncoding.scalingAlgorithm.kp,
                scalingEncoding.scalingAlgorithm.ki, scalingEncoding.scalingAlgorithm.w);
        break;
      case "OML":
        scalingAlgorithm =
            new OnlineMachineLearning(configs, currentClusterSize, scalingEncoding.scalingAlgorithm.lr, path);
        break;
    }
  }

  public int getCurrentClusterSize() {
    return currentClusterSize;
  }

  public int getCurrentQueryIdealSize() {
    return currentQuery.getIdealClusterSize();
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
    scalingAlgorithm.step(currentQuery);
    currentClusterSize = scalingAlgorithm.getCurrentClusterSize();
  }

  public void setupNextFakeQuery() {
    // Files to read
    String queryRuntimeFile = path + "query_runtimes";
    LOGGER.warn(queryRuntimeFile);
    String slaFile = path + "SLAs/tier" + tier;
    LOGGER.warn(slaFile);
    String idealFile = path + "Ideal/ideal" + tier;
    LOGGER.warn(idealFile);
    String descFile = path + "descriptions";
    LOGGER.warn(descFile);

    try {
      BufferedReader runtimeReader = new BufferedReader(new InputStreamReader(new FileInputStream(queryRuntimeFile)));
      BufferedReader slaReader = new BufferedReader(new InputStreamReader(new FileInputStream(slaFile)));
      BufferedReader idealReader = new BufferedReader(new InputStreamReader(new FileInputStream(idealFile)));
      BufferedReader descReader = new BufferedReader(new InputStreamReader(new FileInputStream(descFile)));

      String runtimeLine = runtimeReader.readLine();
      String slaLine = slaReader.readLine();
      String idealLine = idealReader.readLine();
      String desc = descReader.readLine();
      int counter = 0;
      while (runtimeLine != null) {
        if (counter == ithQuerySequence) {
          String[] runtimeParts = runtimeLine.split(",");
          List<Double> queryRuntimesList =
              Arrays.asList(Double.parseDouble(runtimeParts[0]), Double.parseDouble(runtimeParts[1]), Double
                  .parseDouble(runtimeParts[2]), Double.parseDouble(runtimeParts[3]), Double
                  .parseDouble(runtimeParts[4]));
          int queryIdeal = Integer.parseInt(idealLine);
          double querySLA = Double.parseDouble(slaLine);
          QueryMetaData q = new QueryMetaData(counter, desc, querySLA, queryIdeal, queryRuntimesList);
          previousQuery = currentQuery;
          setCurrentQuery(q);
        }

        runtimeLine = runtimeReader.readLine();
        slaLine = slaReader.readLine();
        idealLine = idealReader.readLine();
        desc = descReader.readLine();
        counter++;
      }
      runtimeReader.close();
      slaReader.close();
      idealReader.close();
      descReader.close();

      LOGGER.warn("PREVIOUS QUERY: " + previousQuery.toString());
      LOGGER.warn("CURRENT QUERY: " + currentQuery.toString());

      // case if we finish the sequence
      if (counter == ithQuerySequence + 1) {
        currentQuery = null;
      } else if (counter < ithQuerySequence) {
        currentQuery = null;
        previousQuery = null;
      }

    } catch (NumberFormatException | IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * @param scalingAlgorithmEncoding
   */
  public void updateParameters(final ScalingAlgorithmEncoding scalingAlgorithmEncoding) {
    if (scalingAlgorithm instanceof ReinforcementLearning) {
      ReinforcementLearning r = (ReinforcementLearning) scalingAlgorithm;
      r.setAlpha(scalingAlgorithmEncoding.alpha);
      r.setBeta(scalingAlgorithmEncoding.beta);
    } else if (scalingAlgorithm instanceof PIControl) {
      PIControl p = (PIControl) scalingAlgorithm;
      p.setKP(scalingAlgorithmEncoding.kp);
      p.setKI(scalingAlgorithmEncoding.ki);
    } else if (scalingAlgorithm instanceof OnlineMachineLearning) {
      OnlineMachineLearning o = (OnlineMachineLearning) scalingAlgorithm;
      o.setLR(scalingAlgorithmEncoding.lr);
    }
  }

  /**
   */
  public ScalingStatusEncoding getScalingStatus() {
    return scalingAlgorithm.getScalingStatus();
  }

  public QueryMetaData getPreviousQuery() {
    return previousQuery;
  }

  public QueryMetaData getCurrentQuery() {
    return currentQuery;
  }

}
