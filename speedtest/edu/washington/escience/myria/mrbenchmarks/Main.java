package edu.washington.escience.myria.mrbenchmarks;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.ProcessBuilder.Redirect;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.io.FilenameUtils;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.parallel.Query;
import edu.washington.escience.myria.parallel.Server;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.tool.MyriaConfiguration;
import edu.washington.escience.myria.util.DateTimeUtils;

public class Main {

  static String masterHome = ".";

  public static void startWorkers(final String startingBashScript) {
    final ProcessBuilder pb = new ProcessBuilder("bash", startingBashScript);

    pb.directory(new File("."));
    pb.redirectErrorStream(true);
    pb.redirectOutput(Redirect.PIPE);

    Thread stdoutReader = new Thread("Script stdout reader") {

      @Override
      public void run() {
        try {
          Process ps = pb.start();
          writeProcessOutput(ps);
        } catch (final Exception e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }

      void writeProcessOutput(final Process process) throws Exception {

        final InputStreamReader tempReader = new InputStreamReader(new BufferedInputStream(process.getInputStream()));
        final BufferedReader reader = new BufferedReader(tempReader);
        try {
          while (true) {
            final String line = reader.readLine();
            if (line == null) {
              break;
            }
            System.out.println("script$ " + line);
          }
        } catch (final IOException e) {
          // remote has shutdown. Not an exception.
        }
      }
    };

    stdoutReader.start();
  }

  /**
   * @param args
   * @throws ClassNotFoundException
   * @throws IllegalAccessException
   * @throws InstantiationException
   */
  public static void main(final String[] args) throws Exception {
    String queryClassname = args[0];
    String startWorkersScript = args[1];
    masterHome = args[2];
    String queryWorkerNameListFile = args[3];

    QueryPlanGenerator qpg = (QueryPlanGenerator) (Class.forName(queryClassname).newInstance());
    System.out.println("Query is: " + qpg);

    System.out.println("Begin start master");
    Server server = startMaster();
    System.out.println("End start master");

    System.out.println("Begin start workers");
    startWorkers(startWorkersScript);
    System.out.println("End start workers");

    Set<Integer> computingWorkers = new HashSet<Integer>();
    HashMap<String, Integer> workerName2ID = new HashMap<String, Integer>();
    MyriaConfiguration config =
        MyriaConfiguration.loadWithDefaultValues(FilenameUtils.concat(masterHome, MyriaConstants.DEPLOYMENT_CONF_FILE));
    for (String id : config.getWorkerIds()) {
      workerName2ID.put(config.getHostname(id), Integer.parseInt(id));
    }

    BufferedReader br =
        new BufferedReader(new InputStreamReader(new FileInputStream(new File(queryWorkerNameListFile))));
    String line = null;
    while ((line = br.readLine()) != null) {
      computingWorkers.add(workerName2ID.get(line));
    }
    br.close();

    int[] allQueryWorkers = com.google.common.primitives.Ints.toArray(computingWorkers);

    System.out.println("Num computing workers is" + allQueryWorkers.length);
    System.out.println("All computing workers are: " + computingWorkers);

    final Map<Integer, RootOperator[]> workerPlans = qpg.getWorkerPlan(allQueryWorkers);
    final LinkedBlockingQueue<TupleBatch> resultStore = new LinkedBlockingQueue<TupleBatch>();
    final SinkRoot masterPlan = qpg.getMasterPlan(allQueryWorkers, resultStore);

    long start = System.nanoTime();
    System.out.println("start at : " + start);

    Query queryState = server.submitQueryPlan(masterPlan, workerPlans).get();
    // System.out.println("Query delay:"
    // + DateTimeUtils.nanoElapseToHumanReadable(qf.getQuery().getExecutionStatistics().getQueryDelay()));
    // System.out.println("Query init elapse:"
    // + DateTimeUtils.nanoElapseToHumanReadable(qf.getQuery().getExecutionStatistics().getQueryInitElapse()));
    System.out
        .println("Query execution elapse:" + DateTimeUtils.nanoElapseToHumanReadable(queryState.getElapsedTime()));

    System.out.println("Time spent: " + DateTimeUtils.nanoElapseToHumanReadable(System.nanoTime() - start));
    System.out.println("Total " + masterPlan.getCount() + " tuples.");
    server.shutdown();

  }

  static Server startMaster() throws Exception {
    Server server = new Server(masterHome);
    server.start();
    return server;
  }

}
