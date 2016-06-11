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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

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

  static final int totalNumberOfTrainingQueries = 2000;

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
    List<Thread> threadList = new ArrayList<Thread>();
    for (int i = 0; i < 5; i++) {
      final int clusterIndex = i;
      Thread thread = new Thread() {
        @Override
        public void run() {
          try {
            trainOnlineQueries(clusterIndex, nextQuery.id);
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      };
      threadList.add(thread);
    }

    for (Thread t : threadList) {
      t.start();
    }
    LOGGER.warn("FINISHED PARALLEL PREDICTIONS ");

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
    LOGGER.warn("ALL PREDICTIONS " + queryPredictions[0] + "," + queryPredictions[1] + "," + queryPredictions[2] + ","
        + queryPredictions[3] + "," + queryPredictions[4]);
  }

  // fill up queryPredictions array
  public void trainOnlineQueries(final int clusterSize, final int queryID) throws IOException {
    String MOAFileName = path + "moa.jar";
    String trainingFileName = path + "training.arff"; // Figure out which File to use as training
    String modifiedTrainingFileName = path + "training-modified.arff";

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
    String newPoint = getQueryFeature(clusterSize, queryID);
    LOGGER.warn("ADDED NEW POINT " + newPoint);
    appendDataWriter.write(newPoint + "\n");

    appendDataWriter.close();

    // Run a java app in a separate system process
    String moaCommand =
        String
            .format(
                "java -cp %s moa.DoTask \"EvaluatePrequentialRegression -l (rules.functions.Perceptron  -d -l %s) -s (ArffFileStream -f %s) -e (WindowRegressionPerformanceEvaluator -w 1) -f 1 -o %s",
                MOAFileName, lr, modifiedTrainingFileName);
    LOGGER.warn("OML command " + moaCommand);
    Process p = Runtime.getRuntime().exec(moaCommand);
    try {
      p.waitFor();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    // Then retrieve the output and feed it to the parser
    InputStream in = p.getInputStream();

    // read the file and return the final prediction parsingOnlineFile(clusterSize);
    parsingOnlineFile(clusterSize, in);
  }

  public String getQueryFeature(final int clusterSize, final int queryID) throws IOException {
    String featureFilePath = path + "features/" + clusterSize;
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

  public void parsingOnlineFile(final int clusterSize, final InputStream in) {
    BufferedReader streamReader = new BufferedReader(new InputStreamReader(in));

    String currentLine = "";
    int numberHeaderLines = 0;
    int numberDataPoints = 0;
    int numberTraining = 0;

    numberDataPoints = totalNumberOfTrainingQueries;
    numberTraining = numberDataPoints + additionalDataPoints.size();
    int totalLinesToSkip = numberHeaderLines + numberTraining;

    int countLine = 0;
    double nextQueryPrediction = 0;
    try {
      while ((currentLine = streamReader.readLine()) != null) {
        countLine++;
        if (countLine > totalLinesToSkip) {
          LOGGER.warn("FROM STREAM " + currentLine);
          nextQueryPrediction = Double.parseDouble((currentLine.split(",")[0]).split(":")[1]);
        }
      }
      streamReader.close();
      queryPredictions[clusterSize] = nextQueryPrediction;
    } catch (NumberFormatException | IOException e) {
      e.printStackTrace();
    }

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