/**
 *
 */
package edu.washington.escience.myria.perfenforce;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.LoggerFactory;

import edu.washington.escience.myria.api.encoding.PerfEnforceQueryMetadataEncoding;
import edu.washington.escience.myria.parallel.Server;

/**
 * This class focuses on running the learning algorithm for PerfEnforce
 */
public class PerfEnforceOnlineLearning {

  private final Server server;

  List<String> previousDataPoints;
  private PerfEnforceQueryMetadataEncoding currentQuery;
  private final String onlineLearningPath;
  private final Double[] queryPredictions;
  private int currentConfiguration;
  private int selectedTier;
  private int queryCounter;

  protected static final org.slf4j.Logger LOGGER =
      LoggerFactory.getLogger(PerfEnforceOnlineLearning.class);

  /**
   * The constructor for the PerfEnforceOnlineLearning class
   * 
   * @param server an instance of the server class
   * @param tier the tier selected by the user
   */
  public PerfEnforceOnlineLearning(final Server server, final int tier) {
    selectedTier = tier;
    currentConfiguration = PerfEnforceDriver.configurations.get(tier);
    currentQuery = new PerfEnforceQueryMetadataEncoding();
    queryPredictions = new Double[PerfEnforceDriver.configurations.size()];
    onlineLearningPath =
        PerfEnforceDriver.configurationPath.resolve("PerfEnforceScaling").toString();
    previousDataPoints = new ArrayList<String>();
    this.server = server;
  }

  /**
   * Replaces the query's reference to the fact table based on the current cluster configuration
   * 
   * @param queryText the query that the user will run
   * @param configuration the current configuration
   */
  public String convertQueryForConfiguration(String queryText, int configuration) {
    String convertedQuery = "";

    String factTableName = PerfEnforceDriver.factTableDesc.relationKey.getRelationName();
    if (queryText.contains(factTableName)) {
      convertedQuery = queryText.replace(factTableName, factTableName + configuration);
    }
    return convertedQuery;
  }

  /**
   * Finds the SLA for a given query
   * 
   * @param querySQL the query from the user
   */
  public void findSLA(final String querySQL) throws PerfEnforceException {
    String pslaPath = PerfEnforceDriver.configurationPath.resolve("PSLAGeneration").toString();

    int currentConfigurationSize = PerfEnforceDriver.configurations.get(selectedTier);
    String currentQueryForConfiguration =
        convertQueryForConfiguration(querySQL, currentConfigurationSize);
    String currentQueryFeatures =
        PerfEnforceUtils.getMaxFeature(server, currentQueryForConfiguration, currentConfigurationSize);

    try {
      PrintWriter featureWriter =
          new PrintWriter(Paths.get(pslaPath, "current-q-features.arff").toString(), "UTF-8");

      featureWriter.write("@relation testing \n");

      featureWriter.write("@attribute numberTables numeric \n");
      featureWriter.write("@attribute postgesEstCostMin numeric \n");
      featureWriter.write("@attribute postgesEstCostMax numeric \n");
      featureWriter.write("@attribute postgesEstNumRows numeric \n");
      featureWriter.write("@attribute postgesEstWidth numeric \n");
      featureWriter.write("@attribute numberOfWorkers numeric \n");
      featureWriter.write("@attribute realTime numeric \n");

      featureWriter.write("\n");
      featureWriter.write("@data \n");
      featureWriter.write(currentQueryFeatures + "\n");
      featureWriter.close();

      // predict the runtime
      String[] cmd = {
        "java",
        "-classpath",
        Paths.get(pslaPath, "weka.jar").toString(),
        "weka.classifiers.rules.M5Rules",
        "-M",
        "4.0",
        "-t",
        Paths.get(pslaPath, "training.arff").toString(),
        "-T",
        Paths.get(pslaPath, "current-q-features.arff").toString(),
        "-p",
        "0",
        "-classifications",
        "weka.classifiers.evaluation.output.prediction.CSV -file \""
            + Paths.get(pslaPath, "current-q-results.txt").toString()
            + "\""
      };

      Process p = Runtime.getRuntime().exec(cmd);
      BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
      while ((reader.readLine()) != null) {}

      String querySLA = "";
      BufferedReader predictionReader =
          new BufferedReader(
              new FileReader(Paths.get(pslaPath, "current-q-results.txt").toString()));
      predictionReader.readLine();
      querySLA = predictionReader.readLine().split(",")[2];
      predictionReader.close();

      currentQuery =
          new PerfEnforceQueryMetadataEncoding(
              queryCounter, Double.parseDouble(querySLA), querySQL);
    } catch (Exception e) {
      throw new PerfEnforceException("Error finding SLA");
    }
  }

  /**
   * Determines the best configuration size for currentQuery
   */
  public void findBestConfigurationSize() throws PerfEnforceException {
    try {
      for (int c : PerfEnforceDriver.configurations) {

        int currentConfigurationSize = c;
        String currentQueryForConfiguration =
            convertQueryForConfiguration(currentQuery.getQueryText(), currentConfigurationSize);
        String currentQueryFeatures =
            PerfEnforceUtils.getMaxFeature(
                server, currentQueryForConfiguration, currentConfigurationSize);

        FileWriter featureWriterForConfiguration;
        featureWriterForConfiguration =
            new FileWriter(Paths.get(onlineLearningPath, "features", String.valueOf(c)).toString());
        featureWriterForConfiguration.write(currentQueryFeatures + '\n');
        featureWriterForConfiguration.close();
      }
    } catch (Exception e) {
      throw new PerfEnforceException("Error selecting best configuration size");
    }

    List<Thread> threadList = new ArrayList<Thread>();
    for (int i = 0; i < PerfEnforceDriver.configurations.size(); i++) {
      final int configurationIndex = i;
      Thread thread =
          new Thread(
              new Runnable() {
                @Override
                public void run() {
                  try {
                    trainOnlineQueries(configurationIndex);
                  } catch (Exception e) {
                    e.printStackTrace();
                  }
                }
              });
      threadList.add(thread);
    }

    for (Thread t : threadList) {
      t.start();
    }

    for (Thread t : threadList) {
      try {
        t.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    double maxScore = 0;
    int winnerIndex = 0;
    for (int currentState = 0;
        currentState < PerfEnforceDriver.configurations.size();
        currentState++) {
      double onlinePrediction = queryPredictions[currentState];

      onlinePrediction = (onlinePrediction < 0) ? 0 : onlinePrediction;
      double currentRatio = 0;
      if (currentQuery.slaRuntime == 0) {
        currentRatio = onlinePrediction / 1;
      } else {
        currentRatio = onlinePrediction / currentQuery.slaRuntime;
      }
      double currentScore = closeToOneScore(currentRatio);

      if (currentScore > maxScore) {
        winnerIndex = currentState;
        maxScore = currentScore;
      }
    }

    currentConfiguration = PerfEnforceDriver.configurations.get(winnerIndex);
  }

  /**
   * Given a configuration size, this method predicts the runtime of the currentQuery
   * 
   * @param configurationIndex the configuration size in consideration 
   */
  public void trainOnlineQueries(final int configurationIndex)
      throws PerfEnforceException {
    String MOAFileName = Paths.get(onlineLearningPath, "moa.jar").toString();
    String trainingFileName = Paths.get(onlineLearningPath, "training.arff").toString();
    String modifiedTrainingFileName =
        Paths.get(onlineLearningPath, "training-modified-" + configurationIndex + ".arff").toString();
    String predictionsFileName =
        Paths.get(onlineLearningPath, "predictions" + configurationIndex + ".txt").toString();

    try {
      PrintWriter outputWriter = new PrintWriter(modifiedTrainingFileName);
      outputWriter.close();

      // copy training file to new file
      FileChannel src = new FileInputStream(trainingFileName).getChannel();
      FileChannel dest = new FileOutputStream(modifiedTrainingFileName).getChannel();
      dest.transferFrom(src, 0, src.size());
      src.close();
      dest.close();

      // Append all previous data points
      FileWriter appendDataWriter = new FileWriter(modifiedTrainingFileName, true);
      for (String s : previousDataPoints) {
        appendDataWriter.write(s + "\n");
      }

      // Append the current point
      String newPoint = getQueryFeature(configurationIndex, 0);
      appendDataWriter.write(newPoint + "\n");
      appendDataWriter.close();

      String moaCommand =
          String.format(
              "EvaluatePrequentialRegression -l (rules.functions.Perceptron  -d -l %s) -s (ArffFileStream -f %s) -e (WindowRegressionPerformanceEvaluator -w 1) -f 1 -o %s",
              .04,
              modifiedTrainingFileName,
              predictionsFileName);

      String[] arrayCommand =
          new String[] {"java", "-classpath", MOAFileName, "moa.DoTask", moaCommand};

      Process p = Runtime.getRuntime().exec(arrayCommand);

      BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
      while ((reader.readLine()) != null) {}

      parsingOnlineFile(configurationIndex, predictionsFileName);
    } catch (Exception e) {
      throw new PerfEnforceException("Error during training");
    }
  }

  /**
   * Helps parse the output file from MOA to read the runtime prediction
   * 
   * @param configurationIndex the configuration size in consideration
   * @param predictionFileName the file that contains the prediction
   */
  public void parsingOnlineFile(final int configurationIndex, final String predictionFileName)
      throws PerfEnforceException {
    try {
      BufferedReader streamReader = new BufferedReader(new FileReader(predictionFileName));
      String currentLine = "";
      double nextQueryPrediction = 0;
      while ((currentLine = streamReader.readLine()) != null) {
        nextQueryPrediction = Double.parseDouble((currentLine.split(",")[0]).split(":")[1]);
      }
      streamReader.close();
      queryPredictions[configurationIndex] = nextQueryPrediction;
    } catch (Exception e) {
      throw new PerfEnforceException("Error parsing online predictions file");
    }
  }

  
  /**
   * Returns features for a particular query
   * 
   * @param configurationIndex the configuration size in consideration
   * @param queryRuntime the runtime of the query
   */
  public String getQueryFeature(
      final int configurationIndex, final double queryRuntime)
      throws PerfEnforceException {
    String featureFilePath =
        Paths.get(
                onlineLearningPath,
                "features",
                String.valueOf(PerfEnforceDriver.configurations.get(configurationIndex)))
            .toString();

    try {
      BufferedReader featureReader = new BufferedReader(new FileReader(featureFilePath));
      String result = featureReader.readLine();
      if (queryRuntime != 0) {
        String[] parts = result.split(",");
        result =
            parts[0]
                + ","
                + parts[1]
                + ","
                + parts[2]
                + ","
                + parts[3]
                + ","
                + parts[4]
                + ","
                + parts[5]
                + ","
                + queryRuntime;
      }

      featureReader.close();
      return result;
    } catch (Exception e) {
      throw new PerfEnforceException("Error collecting query feature");
    }
  }

  /**
   * Returns a score to determine the distance between the real runtime and the SLA runtime
   * 
   * @param ratio the ratio between the real runtime and the SLA runtime
   */
  public double closeToOneScore(final double ratio) {
    if (ratio == 1.0) {
      return Double.MAX_VALUE;
    } else {
      return Math.abs(1 / (ratio - 1.0));
    }
  }

  /**
   * Records the real runtime of the query. Used for learning.
   * 
   * @param queryRuntime the runtime of the query
   */
  public void recordRealRuntime(final double queryRuntime) throws PerfEnforceException {
    previousDataPoints.add(
        getQueryFeature(
            PerfEnforceDriver.configurations.indexOf(currentConfiguration),  queryRuntime));
  }

  /**
   * Returns metadata about the current query
   * 
   */
  public PerfEnforceQueryMetadataEncoding getCurrentQuery() {
    return currentQuery;
  }

  /**
   * Gets the current cluster size
   * 
   */
  public int getClusterSize() {
    return currentConfiguration;
  }

  /**
   * Returns the tier selected by the user
   * 
   */
  public int getSelectedTier() {
    return selectedTier;
  }
}
