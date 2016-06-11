/**
 *
 */
package edu.washington.escience.myria.perfenforce;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import edu.washington.escience.myria.perfenforce.encoding.ScalingStatusEncoding;

/**
 * 
 */
public class OnlineMachineLearning implements ScalingAlgorithm {

  double lr;
  Double[] queryPredictions;
  int currentClusterSize;
  int currentPositionIndex;
  List<Integer> configs;
  List<String> additionalDataPoints;

  static final int totalNumberOfTrainingQueries = 2000;

  public OnlineMachineLearning(final List<Integer> configs, final int currentClusterSize, final double lr) {
    this.lr = lr;
    this.currentClusterSize = currentClusterSize;
    currentPositionIndex = configs.indexOf(currentClusterSize);
    this.configs = configs;
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
  }

  // fill up queryPredictions array
  public void trainOnlineQueries(final int clusterSize, final int queryID) throws IOException {
    String MOAPath = "";
    String trainingFile = ""; // Figure out which File to use as training
    String modifyTrainingFile = "";

    String fileToWriteDataPoint = String.format("training_testing{0}.arff", clusterSize);

    // clear results
    PrintWriter outputWriter = new PrintWriter(MOAPath + "output_pred" + clusterSize);
    outputWriter.close();

    // copy training file to new file
    FileChannel src = new FileInputStream(trainingFile).getChannel();
    FileChannel dest = new FileOutputStream(modifyTrainingFile).getChannel();
    dest.transferFrom(src, 0, src.size());
    src.close();
    dest.close();

    // Append all previous data points
    FileWriter appendDataWriter = new FileWriter(MOAPath + fileToWriteDataPoint, true);
    for (String s : additionalDataPoints) {
      appendDataWriter.write(s + "\n");
    }
    appendDataWriter.write(getQueryFeature(clusterSize, queryID) + "\n");
    appendDataWriter.close();

    // Run a java app in a separate system process (FIX the MOA call)
    Process proc = Runtime.getRuntime().exec("java -jar A.jar");

    // Then retrieve the output and feed it to the parser
    InputStream in = proc.getInputStream();

    // read the file and return the final prediction parsingOnlineFile(clusterSize);
    parsingOnlineFile(clusterSize, in);
  }

  public void parsingOnlineFile(final int clusterSize, final InputStream in) {
    BufferedReader streamReader = new BufferedReader(new InputStreamReader(System.in));

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
          nextQueryPrediction = Double.parseDouble((currentLine.split(",")[0]).split(":")[1]);
        }
      }
      streamReader.close();
      queryPredictions[clusterSize] = nextQueryPrediction;
    } catch (NumberFormatException | IOException e) {
      e.printStackTrace();
    }

  }

  public String getQueryFeature(final int clusterSize, final int queryID) {
    return "";
    // The clusterSize helps with the folder finding
    // Even if not fake (real query) should read from the file for ith query
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