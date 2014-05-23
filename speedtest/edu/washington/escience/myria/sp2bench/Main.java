package edu.washington.escience.myria.sp2bench;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.ProcessBuilder.Redirect;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FilenameUtils;

import edu.washington.escience.myria.coordinator.catalog.MasterCatalog;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.parallel.QueryFuture;
import edu.washington.escience.myria.parallel.Server;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.DateTimeUtils;

public class Main {

  final static String catalogFileName = FilenameUtils.concat("/tmp/slxu_experiment", "master.catalog");

  public static void startWorkers(String startingBashScript) {
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
  public static void main(String[] args) throws Exception {
    String queryClassname = args[0];
    String startWorkersScript = args[1];

    QueryPlanGenerator qpg = (QueryPlanGenerator) (Class.forName(queryClassname).newInstance());

    System.out.println("Begin start master");
    Server server = startMaster();
    System.out.println("End start master");

    System.out.println("Begin start workers");
    startWorkers(startWorkersScript);
    System.out.println("End start workers");

    MasterCatalog catalog = MasterCatalog.open(catalogFileName);
    Set<Integer> workerSet = catalog.getWorkers().keySet();
    catalog.close();

    int[] allWorkers = new int[workerSet.size()];
    int idx = 0;
    for (int wID : workerSet) {
      allWorkers[idx++] = wID;
    }

    final Map<Integer, RootOperator[]> workerPlans = qpg.getWorkerPlan(allWorkers);
    final LinkedBlockingQueue<TupleBatch> resultStore = new LinkedBlockingQueue<TupleBatch>();
    final RootOperator masterPlan = qpg.getMasterPlan(allWorkers, resultStore);

    long start = System.nanoTime();

    QueryFuture qf = server.submitQueryPlan(masterPlan, workerPlans);

    TupleBatch tb = null;
    int numResult = 0;
    while (!qf.isDone() || !resultStore.isEmpty()) {
      tb = resultStore.poll(100, TimeUnit.MILLISECONDS);
      System.out.print(tb);
      numResult += tb.numTuples();
    }

    System.out.println("Time spent: " + DateTimeUtils.nanoElapseToHumanReadable(System.nanoTime() - start));
    System.out.println("Total " + numResult + " tuples.");

    qf.get();
    server.shutdown();
  }

  static Server startMaster() throws Exception {
    Server server = new Server(catalogFileName);
    server.start();
    return server;
  }

}
