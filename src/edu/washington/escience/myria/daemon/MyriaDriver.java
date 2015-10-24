package edu.washington.escience.myria.daemon;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import javax.inject.Inject;

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.CompletedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.driver.evaluator.JVMProcess;
import org.apache.reef.driver.evaluator.JVMProcessFactory;
import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.driver.task.FailedTask;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.driver.task.TaskMessage;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.InvalidProtocolBufferException;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.parallel.Server;
import edu.washington.escience.myria.parallel.SocketInfo;
import edu.washington.escience.myria.parallel.Worker;
import edu.washington.escience.myria.proto.ControlProto.ControlMessage;
import edu.washington.escience.myria.proto.TransportProto.TransportMessage;
import edu.washington.escience.myria.tools.MyriaGlobalConfigurationModule;
import edu.washington.escience.myria.tools.MyriaWorkerConfigurationModule;
import edu.washington.escience.myria.util.IPCUtils;

/**
 * Driver for Myria API server/master. Each worker (including master) is mapped to an Evaluator and
 * a persistent Task.
 */
@Unit
public final class MyriaDriver {
  private static final Logger LOGGER = LoggerFactory.getLogger(MyriaDriver.class);
  private final LocalAddressProvider addressProvider;
  private final EvaluatorRequestor requestor;
  private final JVMProcessFactory jvmProcessFactory;
  private final Configuration globalConf;
  private final Injector globalConfInjector;
  private final ImmutableMap<Integer, Configuration> workerConfs;
  private final Set<Integer> workerIdsWithAllocatedEvaluators;
  private final Map<Integer, RunningTask> tasksByWorkerId;
  private final CountDownLatch masterRunning;
  private final CountDownLatch allWorkersRunning;

  /**
   * Possible states of the Myria driver. Can be one of:
   * <dl>
   * <du><code>INIT</code></du>
   * <dd>Initial state. Ready to request an evaluator.</dd>
   * <du><code>PREPARING_MASTER</code></du>
   * <dd>Waiting for master Evaluator/Task to be allocated.</dd>
   * <du><code>PREPARING_WORKERS</code></du>
   * <dd>Waiting for each worker's Evaluator/Task to be allocated.</dd>
   * <du><code>READY</code></du>
   * <dd>Each per-worker Task is ready to receive queries.</dd>
   * </dl>
   */

  private enum State {
    INIT, PREPARING_MASTER, PREPARING_WORKERS, RECOVERING_WORKER, READY
  };

  private volatile State state = State.INIT;

  @Inject
  public MyriaDriver(final LocalAddressProvider addressProvider,
      final EvaluatorRequestor requestor, final JVMProcessFactory jvmProcessFactory,
      final @Parameter(MyriaDriverLauncher.SerializedGlobalConf.class) String serializedGlobalConf)
      throws Exception {
    this.requestor = requestor;
    this.addressProvider = addressProvider;
    this.jvmProcessFactory = jvmProcessFactory;
    globalConf = new AvroConfigurationSerializer().fromString(serializedGlobalConf);
    globalConfInjector = Tang.Factory.getTang().newInjector(globalConf);
    workerConfs = getWorkerConfs();
    workerIdsWithAllocatedEvaluators = new HashSet<>();
    tasksByWorkerId = new HashMap<>();
    masterRunning = new CountDownLatch(1);
    allWorkersRunning = new CountDownLatch(workerConfs.size());
  }

  private String getMasterHost() throws InjectionException {
    final String masterHost =
        globalConfInjector.getNamedInstance(MyriaGlobalConfigurationModule.MasterHost.class);
    // REEF (org.apache.reef.wake.remote.address.HostnameBasedLocalAddressProvider) will
    // unpredictably pick a local DNS name or IP address instead of "localhost" or 127.0.0.1
    String reefMasterHost = masterHost;
    if (masterHost.equals("localhost") || masterHost.equals("127.0.0.1")) {
      try {
        reefMasterHost = InetAddress.getByName(addressProvider.getLocalAddress()).getHostName();
        LOGGER.info("Original host: {}, HostnameBasedLocalAddressProvider returned {}", masterHost,
            reefMasterHost);
      } catch (UnknownHostException e) {
        LOGGER.warn("Failed to get canonical hostname for host {}", masterHost);
      }
    }
    return reefMasterHost;
  }

  private ImmutableMap<Integer, Configuration> getWorkerConfs() throws InjectionException,
      BindException, IOException {
    final ImmutableMap.Builder<Integer, Configuration> workerConfsBuilder =
        new ImmutableMap.Builder<>();
    final Set<String> serializedWorkerConfs =
        globalConfInjector.getNamedInstance(MyriaGlobalConfigurationModule.WorkerConf.class);
    final ConfigurationSerializer serializer = new AvroConfigurationSerializer();
    for (final String serializedWorkerConf : serializedWorkerConfs) {
      final Configuration workerConf = serializer.fromString(serializedWorkerConf);
      workerConfsBuilder.put(getIdFromWorkerConf(workerConf), workerConf);
    }
    return workerConfsBuilder.build();
  }

  private Integer getIdFromWorkerConf(final Configuration workerConf) throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector(workerConf);
    return injector.getNamedInstance(MyriaWorkerConfigurationModule.WorkerId.class);
  }

  private String getHostFromWorkerConf(final Configuration workerConf) throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector(workerConf);
    String host = injector.getNamedInstance(MyriaWorkerConfigurationModule.WorkerHost.class);
    // REEF (org.apache.reef.wake.remote.address.HostnameBasedLocalAddressProvider) will
    // unpredictably pick a local DNS name or IP address instead of "localhost" or 127.0.0.1
    String reefHost = host;
    if (host.equals("localhost") || host.equals("127.0.0.1")) {
      try {
        reefHost = InetAddress.getByName(addressProvider.getLocalAddress()).getHostName();
        LOGGER.info("Original host: {}, HostnameBasedLocalAddressProvider returned {}", host,
            reefHost);
      } catch (UnknownHostException e) {
        LOGGER.warn("Failed to get canonical hostname for host {}", host);
      }
    }
    return reefHost;
  }

  private SocketInfo getSocketInfoForWorker(final int workerId) throws InjectionException {
    final Configuration workerConf = workerConfs.get(workerId);
    final Injector injector = Tang.Factory.getTang().newInjector(workerConf);
    // we don't use getHostFromWorkerConf() because we want to keep our original hostname
    String host = injector.getNamedInstance(MyriaWorkerConfigurationModule.WorkerHost.class);
    Integer port = injector.getNamedInstance(MyriaWorkerConfigurationModule.WorkerPort.class);
    return new SocketInfo(host, port);
  }

  private void requestWorkerEvaluator(final int workerId) throws InjectionException {
    LOGGER.info("Requesting evaluator for worker {}.", workerId);
    final int jvmMemoryQuotaMB =
        1024 * globalConfInjector
            .getNamedInstance(MyriaGlobalConfigurationModule.MemoryQuotaGB.class);
    final int numberVCores =
        globalConfInjector.getNamedInstance(MyriaGlobalConfigurationModule.NumberVCores.class);
    final Configuration workerConf = workerConfs.get(workerId);
    final String hostname = getHostFromWorkerConf(workerConf);
    final EvaluatorRequest workerRequest =
        EvaluatorRequest.newBuilder().setNumber(1).setMemory(jvmMemoryQuotaMB)
            .setNumberOfCores(numberVCores).addNodeName(hostname).build();
    requestor.submit(workerRequest);
  }

  private void requestMasterEvaluator() throws InjectionException {
    LOGGER.info("Requesting master evaluator.");
    final String masterHost = getMasterHost();
    final int jvmMemoryQuotaMB =
        1024 * globalConfInjector
            .getNamedInstance(MyriaGlobalConfigurationModule.MemoryQuotaGB.class);
    final int numberVCores =
        globalConfInjector.getNamedInstance(MyriaGlobalConfigurationModule.NumberVCores.class);
    final EvaluatorRequest masterRequest =
        EvaluatorRequest.newBuilder().setNumber(1).setMemory(jvmMemoryQuotaMB)
            .setNumberOfCores(numberVCores).addNodeName(masterHost).build();
    requestor.submit(masterRequest);
  }

  private void setJVMOptions(final AllocatedEvaluator evaluator) throws InjectionException {
    final int jvmHeapSizeMinGB =
        globalConfInjector.getNamedInstance(MyriaGlobalConfigurationModule.JvmHeapSizeMinGB.class);
    final int jvmHeapSizeMaxGB =
        globalConfInjector.getNamedInstance(MyriaGlobalConfigurationModule.JvmHeapSizeMaxGB.class);
    final Set<String> jvmOptions =
        globalConfInjector.getNamedInstance(MyriaGlobalConfigurationModule.JvmOptions.class);
    final JVMProcess jvmProcess =
        jvmProcessFactory.newEvaluatorProcess()
            .addOption(String.format("-Xms%dg", jvmHeapSizeMinGB))
            .addOption(String.format("-Xmx%dg", jvmHeapSizeMaxGB))
            // for native libraries
            .addOption("-Djava.library.path=./reef/global");
    for (String option : jvmOptions) {
      jvmProcess.addOption(option);
    }
    evaluator.setProcess(jvmProcess);
  }

  private void notifyWorkerFailure(final int workerId) {
    LOGGER.info("Sending REMOVE_WORKER for worker {} to coordinator", workerId);
    final TransportMessage workerFailed = IPCUtils.removeWorkerTM(workerId);
    RunningTask coordinatorTask = tasksByWorkerId.get(MyriaConstants.MASTER_ID);
    coordinatorTask.send(workerFailed.toByteArray());
  }

  private void notifyWorkerRecovered(final int workerId) {
    Preconditions.checkState(state == State.RECOVERING_WORKER);
    SocketInfo si;
    try {
      si = getSocketInfoForWorker(workerId);
    } catch (InjectionException e) {
      LOGGER.error("Failed to get SocketInfo for worker {}:\n{}", workerId, e);
      return;
    }
    LOGGER.info("Sending ADD_WORKER for worker {} to coordinator", workerId);
    final TransportMessage workerFailed = IPCUtils.addWorkerTM(workerId, si);
    RunningTask coordinatorTask = tasksByWorkerId.get(MyriaConstants.MASTER_ID);
    coordinatorTask.send(workerFailed.toByteArray());
  }

  private void scheduleTask(final ActiveContext context) {
    final Configuration taskConf;
    if (context.getId().equals(MyriaConstants.MASTER_ID + "")) {
      Preconditions.checkState(state == State.PREPARING_MASTER);
      taskConf =
          TaskConfiguration.CONF.set(TaskConfiguration.TASK, MasterDaemon.class)
              .set(TaskConfiguration.IDENTIFIER, context.getId())
              .set(TaskConfiguration.ON_SEND_MESSAGE, Server.class)
              .set(TaskConfiguration.ON_MESSAGE, Server.class).build();

    } else {
      Preconditions
          .checkState(state == State.PREPARING_WORKERS || state == State.RECOVERING_WORKER);
      taskConf =
          TaskConfiguration.CONF.set(TaskConfiguration.TASK, Worker.class)
              .set(TaskConfiguration.IDENTIFIER, context.getId()).build();
    }
    context.submitTask(taskConf);
  }

  private void allocateWorkerContext(final AllocatedEvaluator evaluator, final int workerId)
      throws InjectionException {
    Preconditions.checkState(state == State.PREPARING_WORKERS || state == State.RECOVERING_WORKER);
    LOGGER.info("Launching context for worker ID {} on {}", workerId, evaluator
        .getEvaluatorDescriptor().getNodeDescriptor().getName());
    Preconditions.checkState(!workerIdsWithAllocatedEvaluators.contains(workerId));
    Configuration contextConf =
        ContextConfiguration.CONF.set(ContextConfiguration.IDENTIFIER, workerId + "").build();
    setJVMOptions(evaluator);
    evaluator
        .submitContext(Configurations.merge(contextConf, globalConf, workerConfs.get(workerId)));
    workerIdsWithAllocatedEvaluators.add(workerId);
  }

  /**
   * The driver is ready to run.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public synchronized void onNext(final StartTime startTime) {
      // NB: because this handler blocks waiting for other handlers to run (to decrement the
      // CountDownLatch), synchronizing on the outer object (MyriaDriver.this) will deadlock.
      LOGGER.info("Driver started at {}", startTime);
      Preconditions.checkState(state == State.INIT);
      state = State.PREPARING_MASTER;
      try {
        requestMasterEvaluator();
      } catch (InjectionException e) {
        throw new RuntimeException(e);
      }
      // We need to wait for the master to be allocated before we request evaluators for the
      // workers, otherwise we can't tell which evaluator is which (because REEF doesn't let us
      // submit a client token with the evaluator requests).
      try {
        masterRunning.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      LOGGER.info("Master is running, starting {} workers...", workerConfs.size());
      state = State.PREPARING_WORKERS;
      try {
        for (final Integer workerId : workerConfs.keySet()) {
          requestWorkerEvaluator(workerId);
        }
      } catch (InjectionException e) {
        throw new RuntimeException(e);
      }
      try {
        allWorkersRunning.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      LOGGER.info("All workers running, ready for queries...");
      // We are now ready to receive requests.
      // TODO: send RPC to REST API that it can now service requests?
      state = State.READY;
    }
  }

  /**
   * Shutting down the job driver: close the evaluators.
   */
  final class StopHandler implements EventHandler<StopTime> {
    @Override
    public synchronized void onNext(final StopTime stopTime) {
      LOGGER.info("Driver stopped at {}", stopTime);
      for (final RunningTask task : tasksByWorkerId.values()) {
        task.getActiveContext().close();
      }
    }
  }

  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public synchronized void onNext(final AllocatedEvaluator evaluator) {
      String node = evaluator.getEvaluatorDescriptor().getNodeDescriptor().getName();
      LOGGER.info("Allocated evaluator {} on node {}", evaluator.getId(), node);
      LOGGER.info("State: {}", state);
      if (state == State.PREPARING_MASTER) {
        Configuration contextConf =
            ContextConfiguration.CONF.set(ContextConfiguration.IDENTIFIER,
                MyriaConstants.MASTER_ID + "").build();
        try {
          setJVMOptions(evaluator);
        } catch (InjectionException e) {
          throw new RuntimeException(e);
        }
        evaluator.submitContext(Configurations.merge(contextConf, globalConf));
      } else {
        Preconditions.checkState(state == State.PREPARING_WORKERS
            || state == State.RECOVERING_WORKER);
        // allocate the next worker ID associated with this host
        try {
          for (final Configuration workerConf : workerConfs.values()) {
            final String workerHost = getHostFromWorkerConf(workerConf);
            final Integer workerId = getIdFromWorkerConf(workerConf);
            if (workerHost.equals(node) && !workerIdsWithAllocatedEvaluators.contains(workerId)) {
              allocateWorkerContext(evaluator, workerId);
              break;
            }
          }
        } catch (InjectionException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  final class CompletedEvaluatorHandler implements EventHandler<CompletedEvaluator> {
    @Override
    public synchronized void onNext(final CompletedEvaluator eval) {
      LOGGER.info("CompletedEvaluator: {}", eval.getId());
    }
  }

  final class EvaluatorFailedHandler implements EventHandler<FailedEvaluator> {
    @Override
    public synchronized void onNext(final FailedEvaluator failedEvaluator) {
      LOGGER.error("FailedEvaluator: {}", failedEvaluator);
      // respawn evaluator and reschedule task if configured
      List<FailedContext> failedContexts = failedEvaluator.getFailedContextList();
      // we should have at most one context in the list (since we only allocate the root context)
      if (failedContexts.size() > 0) {
        FailedContext failedContext = failedContexts.get(0);
        int workerId = Integer.valueOf(failedContext.getId());
        // we don't handle coordinator failure yet
        if (workerId != MyriaConstants.MASTER_ID) {
          tasksByWorkerId.remove(workerId);
          workerIdsWithAllocatedEvaluators.remove(workerId);
          // notify coordinator this worker has failed
          notifyWorkerFailure(workerId);
          // allocate new evaluator
          LOGGER.info("Attempting to reallocate evaluator for worker {}", workerId);
          state = State.RECOVERING_WORKER;
          try {
            requestWorkerEvaluator(workerId);
          } catch (InjectionException e) {
            LOGGER.error("Failed to reallocate evaluator for worker {}:\n{}", workerId, e);
            state = State.READY;
          }
        }

      }
    }
  }

  final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public synchronized void onNext(final ActiveContext context) {
      String host = context.getEvaluatorDescriptor().getNodeDescriptor().getName();
      LOGGER.info("Context {} available on node {}", context.getId(), host);
      scheduleTask(context);
    }
  }

  final class ContextFailureHandler implements EventHandler<FailedContext> {
    @Override
    public synchronized void onNext(final FailedContext failedContext) {
      LOGGER.error("FailedContext: {}", failedContext);
    }
  }

  final class RunningTaskHandler implements EventHandler<RunningTask> {
    @Override
    public synchronized void onNext(final RunningTask task) {
      LOGGER.info("Running task: {}", task.getId());
      int workerId = Integer.valueOf(task.getId());
      Preconditions.checkState(!tasksByWorkerId.containsKey(workerId));
      if (state == State.PREPARING_MASTER) {
        Preconditions.checkState(workerId == MyriaConstants.MASTER_ID);
        masterRunning.countDown();
      } else if (state == State.PREPARING_WORKERS) {
        allWorkersRunning.countDown();
      } else if (state == State.RECOVERING_WORKER) {
        LOGGER.info("Task for worker {} successfully rescheduled", workerId);
        notifyWorkerRecovered(workerId);
      } else {
        throw new IllegalStateException("Should not be scheduling new task in state " + state);
      }
      tasksByWorkerId.put(workerId, task);
    }
  }

  final class CompletedTaskHandler implements EventHandler<CompletedTask> {
    @Override
    public synchronized void onNext(final CompletedTask task) {
      LOGGER.error("Unexpected CompletedTask: {}", task.getId());
    }
  }

  final class TaskFailureHandler implements EventHandler<FailedTask> {
    @Override
    public synchronized void onNext(final FailedTask failedTask) {
      LOGGER.error("FailedTask: {}", failedTask);
      int workerId = Integer.valueOf(failedTask.getId());
      // we don't handle coordinator failure yet
      if (workerId != MyriaConstants.MASTER_ID) {
        tasksByWorkerId.remove(workerId);
        // notify coordinator this worker has failed
        notifyWorkerFailure(workerId);
        // attempt to reschedule task on the same evaluator
        Optional<ActiveContext> context = failedTask.getActiveContext();
        if (context.isPresent()) {
          LOGGER.info("Attempting to reschedule failed task for worker {}", workerId);
          state = State.RECOVERING_WORKER;
          scheduleTask(context.get());
        }
      }
    }
  }

  final class TaskMessageHandler implements EventHandler<TaskMessage> {
    @Override
    public void onNext(final TaskMessage taskMessage) {
      // we should only be receiving messages from the coordinator
      Preconditions.checkState(taskMessage.getMessageSourceID().equals(
          MyriaConstants.MASTER_ID + ""));
      TransportMessage m;
      try {
        m = TransportMessage.parseFrom(taskMessage.get());
      } catch (InvalidProtocolBufferException e) {
        LOGGER.warn("Could not parse TransportMessage from task message", e);
        return;
      }
      final ControlMessage controlM = m.getControlMessage();
      LOGGER.info("Control message received: {}", controlM);
      // We received a failed worker ack or recovered worker ack from the coordinator.
      Preconditions.checkState(controlM.getType() == ControlMessage.Type.REMOVE_WORKER_ACK
          || controlM.getType() == ControlMessage.Type.ADD_WORKER_ACK);
      int workerId = controlM.getWorkerId();
      LOGGER.info("Received {} for worker {} from task {}", controlM.getType(), workerId,
          taskMessage.getMessageSourceID());
      if (controlM.getType() == ControlMessage.Type.ADD_WORKER_ACK) {
        Preconditions.checkState(state == State.RECOVERING_WORKER);
        state = State.READY;
      }
    }
  }
}
