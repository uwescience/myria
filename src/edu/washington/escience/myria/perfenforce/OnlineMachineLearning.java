/**
 *
 */
package edu.washington.escience.myria.perfenforce;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.time.StopWatch;
import org.slf4j.LoggerFactory;

import edu.washington.escience.myria.perfenforce.encoding.ScalingStatusEncoding;

/**
 * 
 */
public class OnlineMachineLearning implements ScalingAlgorithm {

  double lr;
  Double[] queryPredictions;
  int currentClusterSize;
  int currentPositionIndex;
  String path;
  List<Integer> configs;
  List<String> additionalDataPoints;

  protected static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(OnlineMachineLearning.class);

  public OnlineMachineLearning(final List<Integer> configs, final int currentClusterSize, final double lr,
      final String path) {
    this.lr = lr;
    this.currentClusterSize = currentClusterSize;
    currentPositionIndex = configs.indexOf(currentClusterSize);
    this.configs = configs;
    this.path = path;
    queryPredictions = new Double[configs.size()];
    additionalDataPoints = new ArrayList<String>();
  }

  public void setLR(final double lr) {
    this.lr = lr;
  }

  @Override
  public void step(final QueryMetaData nextQuery) {

    // Predict in parallel
    StopWatch stopWatch = new StopWatch();
    stopWatch.start();
    List<Thread> threadList = new ArrayList<Thread>();
    for (int i = 0; i < configs.size(); i++) {
      final int clusterIndex = i;
      Thread thread = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            trainOnlineQueries(clusterIndex, nextQuery.id);
          } catch (IOException e) {
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
    stopWatch.stop();
    LOGGER.warn("TIME : " + stopWatch.getTime() / 1000 + " seconds");

    LOGGER.warn("FINISHED PARALLEL PREDICTIONS ");
    LOGGER.warn("ALL PREDICTIONS " + queryPredictions[0] + "," + queryPredictions[1] + "," + queryPredictions[2] + ","
        + queryPredictions[3] + "," + queryPredictions[4]);

    int maxScore = 0;
    int winnerIndex = 0;
    for (int currentState = 0; currentState < configs.size(); currentState++) {
      double onlinePrediction = queryPredictions[currentState];

      onlinePrediction = (onlinePrediction < 0) ? 0 : onlinePrediction;

      double currentRatio = onlinePrediction / nextQuery.slaRuntime;

      double currentScore = closeToOneScore(currentRatio);
      if (currentScore > maxScore) {
        winnerIndex = currentState;
        maxScore = (int) currentScore;
      }
    }

    // Officially record the winner
    currentPositionIndex = winnerIndex;
    currentClusterSize = configs.get(currentPositionIndex);
    LOGGER.warn("WINNER SIZE " + currentClusterSize);

  }

  // fill up queryPredictions array
  public void trainOnlineQueries(final int clusterIndex, final int queryID) throws IOException {
    String MOAFileName = path + "OMLFiles/moa.jar";
    String trainingFileName = path + "OMLFiles/training.arff"; // Figure out which File to use as training
    String modifiedTrainingFileName = path + "OMLFiles/training-modified-" + clusterIndex + ".arff";
    String predictionsFileName = path + "OMLFiles/predictions" + clusterIndex + ".txt";

    // clear results -- necessary to make the file?
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
    for (String s : additionalDataPoints) {
      appendDataWriter.write(s + "\n");
      LOGGER.warn("ADDED POINT " + s);
    }
    // Append the current point
    String newPoint = getQueryFeature(clusterIndex, queryID);
    LOGGER.warn("ADDED NEW POINT " + newPoint);
    appendDataWriter.write(newPoint + "\n");

    appendDataWriter.close();

    String moaCommand =
        String
            .format(
                "EvaluatePrequentialRegression -l (rules.functions.Perceptron  -d -l %s) -s (ArffFileStream -f %s) -e (WindowRegressionPerformanceEvaluator -w 1) -f 1 -o %s",
                lr, modifiedTrainingFileName, predictionsFileName);
    LOGGER.warn("MOA COMMAND " + moaCommand);
    String[] arrayCommand = new String[] { "java", "-classpath", MOAFileName, "moa.DoTask", moaCommand };

    Process p = Runtime.getRuntime().exec(arrayCommand);

    BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
    while ((reader.readLine()) != null) {
    }
    try {
      p.waitFor();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    // read the file and return the final prediction parsingOnlineFile(clusterSize);
    parsingOnlineFile(clusterIndex, predictionsFileName);
  }

  public void parsingOnlineFile(final int clusterSize, final String predictionFileName) throws IOException {
    BufferedReader streamReader = new BufferedReader(new FileReader(predictionFileName));
    String currentLine = "";
    double nextQueryPrediction = 0;
    try {
      while ((currentLine = streamReader.readLine()) != null) {
        nextQueryPrediction = Double.parseDouble((currentLine.split(",")[0]).split(":")[1]);
      }
      streamReader.close();
      queryPredictions[clusterSize] = nextQueryPrediction;
    } catch (NumberFormatException | IOException e) {
      e.printStackTrace();
    }

  }

  public String getQueryFeature(final int clusterIndex, final int queryID) throws IOException {
    String featureFilePath = path + "OMLFiles/features/" + configs.get(clusterIndex);
    LOGGER.warn("Feature path " + featureFilePath);
    BufferedReader featureReader = new BufferedReader(new FileReader(featureFilePath));

    // Lines to skip
    for (int i = 0; i < queryID; i++) {
      featureReader.readLine();
    }
    String result = featureReader.readLine();
    featureReader.close();
    return result;
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
    statusEncoding.OMLPredictions = queryPredictions;
    return statusEncoding;
  }

}