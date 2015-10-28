package edu.washington.escience.myria.daemon;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableTable;
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
  private final Queue<Integer> workerIdsPendingEvaluatorAllocation;
  private final ConcurrentMap<Integer, RunningTask> tasksByWorkerId;
  private final ConcurrentMap<Integer, ActiveContext> contextsByWorkerId;
  private final ConcurrentMap<Integer, AllocatedEvaluator> evaluatorsByWorkerId;
  private final AtomicInteger numberWorkersPending;

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

  private enum DriverState {
    INIT, PREPARING_MASTER, PREPARING_WORKERS, READY
  };

  private volatile DriverState state = DriverState.INIT;

  private enum TaskState {
    PENDING_EVALUATOR_REQUEST, PENDING_EVALUATOR, PENDING_CONTEXT, PENDING_TASK, FAILED_PENDING_EVALUATOR_REQUEST, FAILED_PENDING_EVALUATOR, FAILED_PENDING_CONTEXT, FAILED_PENDING_TASK, READY, FAILED_EVALUATOR_PENDING_REMOVE_ACK, FAILED_CONTEXT_PENDING_REMOVE_ACK, FAILED_TASK_PENDING_REMOVE_ACK, FAILED_EVALUATOR_PENDING_ADD_ACK, FAILED_CONTEXT_PENDING_ADD_ACK, FAILED_TASK_PENDING_ADD_ACK, RUNNING_PENDING_ADD_ACK
  };

  private final ConcurrentMap<Integer, TaskState> workerStates;

  private enum TaskStateEvent {
    EVALUATOR_SUBMITTED, EVALUATOR_ALLOCATED, CONTEXT_ALLOCATED, TASK_RUNNING, TASK_FAILED, CONTEXT_FAILED, EVALUATOR_FAILED, REMOVE_WORKER_ACK, ADD_WORKER_ACK
  }

  private static class TaskStateTransition {

    @FunctionalInterface
    public interface Handler {
      void onTransition(final int workerId, final Object context) throws Exception;
    }

    public final TaskState newState;
    public final Handler handler;

    public static TaskStateTransition of(final TaskState newState, final Handler handler) {
      return new TaskStateTransition(newState, handler);
    }

    private TaskStateTransition(final TaskState newState, final Handler handler) {
      this.newState = newState;
      this.handler = handler;
    }
  }

  private final ImmutableTable<TaskState, TaskStateEvent, TaskStateTransition> taskStateTransitions;

  private ImmutableTable<TaskState, TaskStateEvent, TaskStateTransition> getTaskStateTransitions() {
    return new ImmutableTable.Builder<TaskState, TaskStateEvent, TaskStateTransition>()
        .put(TaskState.PENDING_EVALUATOR_REQUEST, TaskStateEvent.EVALUATOR_SUBMITTED,
            TaskStateTransition.of(TaskState.PENDING_EVALUATOR, (wid, ctx) -> {
              workerIdsPendingEvaluatorAllocation.add(wid);
              requestor.submit((EvaluatorRequest) ctx);
            }))
        .put(TaskState.PENDING_EVALUATOR, TaskStateEvent.EVALUATOR_ALLOCATED,
            TaskStateTransition.of(TaskState.PENDING_CONTEXT, (wid, ctx) -> {
              evaluatorsByWorkerId.put(wid, (AllocatedEvaluator) ctx);
              allocateWorkerContext(wid);
            }))
        .put(TaskState.PENDING_CONTEXT, TaskStateEvent.CONTEXT_ALLOCATED,
            TaskStateTransition.of(TaskState.PENDING_TASK, (wid, ctx) -> {
              contextsByWorkerId.put(wid, (ActiveContext) ctx);
              scheduleTask(wid);
            }))
        .put(TaskState.PENDING_CONTEXT, TaskStateEvent.EVALUATOR_FAILED,
            TaskStateTransition.of(TaskState.PENDING_EVALUATOR, (wid, ctx) -> {
              evaluatorsByWorkerId.remove(wid);
              updateFailedWorkerState(wid);
              requestWorkerEvaluator(wid);
            }))
        .put(TaskState.PENDING_TASK, TaskStateEvent.TASK_RUNNING,
            TaskStateTransition.of(TaskState.READY, (wid, ctx) -> {
              tasksByWorkerId.put(wid, (RunningTask) ctx);
              updateRunningWorkerState(wid);
            }))
        .put(TaskState.PENDING_TASK, TaskStateEvent.CONTEXT_FAILED,
            TaskStateTransition.of(TaskState.PENDING_CONTEXT, (wid, ctx) -> {
              contextsByWorkerId.remove(wid);
              updateFailedWorkerState(wid);
              allocateWorkerContext(wid);
            }))
        .put(TaskState.PENDING_TASK, TaskStateEvent.EVALUATOR_FAILED,
            TaskStateTransition.of(TaskState.PENDING_EVALUATOR_REQUEST, (wid, ctx) -> {
              contextsByWorkerId.remove(wid);
              evaluatorsByWorkerId.remove(wid);
              updateFailedWorkerState(wid);
              requestWorkerEvaluator(wid);
            }))
        .put(TaskState.READY, TaskStateEvent.TASK_FAILED,
            TaskStateTransition.of(TaskState.FAILED_TASK_PENDING_REMOVE_ACK, (wid, ctx) -> {
              tasksByWorkerId.remove(wid);
              updateFailedWorkerState(wid);
              notifyWorkerFailure(wid);
            }))
        .put(TaskState.READY, TaskStateEvent.CONTEXT_FAILED,
            TaskStateTransition.of(TaskState.FAILED_CONTEXT_PENDING_REMOVE_ACK, (wid, ctx) -> {
              tasksByWorkerId.remove(wid);
              contextsByWorkerId.remove(wid);
              updateFailedWorkerState(wid);
              notifyWorkerFailure(wid);
            }))
        .put(TaskState.READY, TaskStateEvent.EVALUATOR_FAILED,
            TaskStateTransition.of(TaskState.FAILED_EVALUATOR_PENDING_REMOVE_ACK, (wid, ctx) -> {
              tasksByWorkerId.remove(wid);
              contextsByWorkerId.remove(wid);
              evaluatorsByWorkerId.remove(wid);
              updateFailedWorkerState(wid);
              notifyWorkerFailure(wid);
            }))
        .put(
            TaskState.FAILED_EVALUATOR_PENDING_REMOVE_ACK,
            TaskStateEvent.REMOVE_WORKER_ACK,
            TaskStateTransition.of(TaskState.FAILED_PENDING_EVALUATOR_REQUEST,
                (wid, ctx) -> requestWorkerEvaluator(wid)))
        .put(
            TaskState.FAILED_CONTEXT_PENDING_REMOVE_ACK,
            TaskStateEvent.REMOVE_WORKER_ACK,
            TaskStateTransition.of(TaskState.FAILED_PENDING_CONTEXT,
                (wid, ctx) -> allocateWorkerContext(wid)))
        .put(TaskState.FAILED_TASK_PENDING_REMOVE_ACK, TaskStateEvent.REMOVE_WORKER_ACK,
            TaskStateTransition.of(TaskState.FAILED_PENDING_TASK, (wid, ctx) -> scheduleTask(wid)))
        .put(TaskState.FAILED_PENDING_EVALUATOR_REQUEST, TaskStateEvent.EVALUATOR_SUBMITTED,
            TaskStateTransition.of(TaskState.FAILED_PENDING_EVALUATOR, (wid, ctx) -> {
              workerIdsPendingEvaluatorAllocation.add(wid);
              requestor.submit((EvaluatorRequest) ctx);
            }))
        .put(TaskState.FAILED_PENDING_EVALUATOR, TaskStateEvent.EVALUATOR_ALLOCATED,
            TaskStateTransition.of(TaskState.FAILED_PENDING_CONTEXT, (wid, ctx) -> {
              evaluatorsByWorkerId.put(wid, (AllocatedEvaluator) ctx);
              allocateWorkerContext(wid);
            }))
        .put(TaskState.FAILED_PENDING_CONTEXT, TaskStateEvent.CONTEXT_ALLOCATED,
            TaskStateTransition.of(TaskState.FAILED_PENDING_TASK, (wid, ctx) -> {
              contextsByWorkerId.put(wid, (ActiveContext) ctx);
              scheduleTask(wid);
            }))
        .put(TaskState.FAILED_PENDING_TASK, TaskStateEvent.TASK_RUNNING,
            TaskStateTransition.of(TaskState.RUNNING_PENDING_ADD_ACK, (wid, ctx) -> {
              tasksByWorkerId.put(wid, (RunningTask) ctx);
              notifyWorkerRecovered(wid);
            }))
        .put(TaskState.RUNNING_PENDING_ADD_ACK, TaskStateEvent.ADD_WORKER_ACK,
            TaskStateTransition.of(TaskState.READY, (wid, ctx) -> updateRunningWorkerState(wid)))
        .put(TaskState.RUNNING_PENDING_ADD_ACK, TaskStateEvent.TASK_FAILED,
            TaskStateTransition.of(TaskState.FAILED_TASK_PENDING_ADD_ACK, (wid, ctx) -> {
              tasksByWorkerId.remove(wid);
              updateFailedWorkerState(wid);
            }))
        .put(TaskState.RUNNING_PENDING_ADD_ACK, TaskStateEvent.CONTEXT_FAILED,
            TaskStateTransition.of(TaskState.FAILED_CONTEXT_PENDING_ADD_ACK, (wid, ctx) -> {
              tasksByWorkerId.remove(wid);
              contextsByWorkerId.remove(wid);
              updateFailedWorkerState(wid);
            }))
        .put(TaskState.RUNNING_PENDING_ADD_ACK, TaskStateEvent.EVALUATOR_FAILED,
            TaskStateTransition.of(TaskState.FAILED_EVALUATOR_PENDING_ADD_ACK, (wid, ctx) -> {
              tasksByWorkerId.remove(wid);
              contextsByWorkerId.remove(wid);
              evaluatorsByWorkerId.remove(wid);
              updateFailedWorkerState(wid);
            }))
        .put(
            TaskState.FAILED_EVALUATOR_PENDING_ADD_ACK,
            TaskStateEvent.ADD_WORKER_ACK,
            TaskStateTransition.of(TaskState.FAILED_EVALUATOR_PENDING_REMOVE_ACK,
                (wid, ctx) -> notifyWorkerFailure(wid)))
        .put(
            TaskState.FAILED_CONTEXT_PENDING_ADD_ACK,
            TaskStateEvent.ADD_WORKER_ACK,
            TaskStateTransition.of(TaskState.FAILED_CONTEXT_PENDING_REMOVE_ACK,
                (wid, ctx) -> notifyWorkerFailure(wid)))
        .put(
            TaskState.FAILED_TASK_PENDING_ADD_ACK,
            TaskStateEvent.ADD_WORKER_ACK,
            TaskStateTransition.of(TaskState.FAILED_TASK_PENDING_REMOVE_ACK,
                (wid, ctx) -> notifyWorkerFailure(wid))).build();
  }

  public void doTransition(final int workerId, final TaskStateEvent event, final Object context) {
    // loop until workerStates.replace() succeeds (it will fail if the state has been replaced
    // between our getting and setting of the state for this worker)
    while (true) {
      TaskState workerState = workerStates.get(workerId);
      TaskStateTransition transition = taskStateTransitions.get(workerState, event);
      if (transition != null) {
        if (workerStates.replace(workerId, workerState, transition.newState)) {
          try {
            LOGGER
                .info(
                    "Performing transition on event {} from state {} to state {} (worker ID {}, context {})",
                    event, workerState, transition.newState, workerId, context);
            transition.handler.onTransition(workerId, context);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          break;
        }
      } else {
        throw new IllegalStateException(String.format(
            "No transition defined for state %s and event %s (worker ID %s)", workerState, event,
            workerId));
      }
    }
  }

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
    workerIdsPendingEvaluatorAllocation = new ConcurrentLinkedQueue<>();
    tasksByWorkerId = new ConcurrentHashMap<>();
    contextsByWorkerId = new ConcurrentHashMap<>();
    evaluatorsByWorkerId = new ConcurrentHashMap<>();
    numberWorkersPending = new AtomicInteger(workerConfs.size());
    workerStates = getWorkerStates();
    taskStateTransitions = getTaskStateTransitions();
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

  private ConcurrentMap<Integer, TaskState> getWorkerStates() {
    final ConcurrentMap<Integer, TaskState> workerStates =
        new ConcurrentHashMap<>(workerConfs.size() + 1);
    workerStates.put(MyriaConstants.MASTER_ID, TaskState.PENDING_EVALUATOR_REQUEST);
    for (Integer workerId : workerConfs.keySet()) {
      workerStates.put(workerId, TaskState.PENDING_EVALUATOR_REQUEST);
    }
    return workerStates;
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
    Preconditions.checkArgument(workerId != MyriaConstants.MASTER_ID);
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
    doTransition(workerId, TaskStateEvent.EVALUATOR_SUBMITTED, workerRequest);
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
    doTransition(MyriaConstants.MASTER_ID, TaskStateEvent.EVALUATOR_SUBMITTED, masterRequest);
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
    if (workerId != MyriaConstants.MASTER_ID) {
      LOGGER.info("Sending REMOVE_WORKER for worker {} to coordinator", workerId);
      final TransportMessage workerFailed = IPCUtils.removeWorkerTM(workerId);
      RunningTask coordinatorTask = tasksByWorkerId.get(MyriaConstants.MASTER_ID);
      coordinatorTask.send(workerFailed.toByteArray());
    }
  }

  private void notifyWorkerRecovered(final int workerId) {
    if (workerId != MyriaConstants.MASTER_ID) {
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
  }

  private void scheduleTask(final int workerId) {
    Preconditions.checkState(contextsByWorkerId.containsKey(workerId));
    ActiveContext context = contextsByWorkerId.get(workerId);
    LOGGER.info("Scheduling task for worker ID {} on context {}, evaluator {}", workerId,
        context.getId(), context.getEvaluatorId());
    final Configuration taskConf;
    if (workerId == MyriaConstants.MASTER_ID) {
      Preconditions.checkState(state == DriverState.PREPARING_MASTER);
      taskConf =
          TaskConfiguration.CONF.set(TaskConfiguration.TASK, MasterDaemon.class)
              .set(TaskConfiguration.IDENTIFIER, workerId + "")
              .set(TaskConfiguration.ON_SEND_MESSAGE, Server.class)
              .set(TaskConfiguration.ON_MESSAGE, Server.class).build();

    } else {
      Preconditions
          .checkState(state == DriverState.PREPARING_WORKERS || state == DriverState.READY);
      taskConf =
          TaskConfiguration.CONF.set(TaskConfiguration.TASK, Worker.class)
              .set(TaskConfiguration.IDENTIFIER, workerId + "").build();
    }
    context.submitTask(taskConf);
  }

  private void updateRunningWorkerState(final int workerId) throws InjectionException {
    Preconditions.checkState(tasksByWorkerId.containsKey(workerId));
    if (state == DriverState.PREPARING_MASTER) {
      Preconditions.checkState(workerId == MyriaConstants.MASTER_ID);
      LOGGER.info("Master is running, starting {} workers...", workerConfs.size());
      state = DriverState.PREPARING_WORKERS;
      launchWorkers();
    } else if (state == DriverState.PREPARING_WORKERS) {
      Preconditions.checkState(workerId != MyriaConstants.MASTER_ID);
      if (numberWorkersPending.decrementAndGet() == 0) {
        LOGGER.info("All {} workers running, ready for queries...", workerConfs.size());
        state = DriverState.READY;
      }
    }
  }

  private void updateFailedWorkerState(final int workerId) {
    if (workerId == MyriaConstants.MASTER_ID) {
      throw new RuntimeException("Shutting down driver on coordinator failure");
    } else if (state == DriverState.PREPARING_WORKERS) {
      int pendingWorkers = numberWorkersPending.incrementAndGet();
      LOGGER
          .warn("Worker failed in PREPARING_WORKERS phase, {} workers pending...", pendingWorkers);
    }
  }

  private void allocateWorkerContext(final int workerId) throws InjectionException {
    Preconditions.checkState(evaluatorsByWorkerId.containsKey(workerId));
    AllocatedEvaluator evaluator = evaluatorsByWorkerId.get(workerId);
    LOGGER.info("Launching context for worker ID {} on {}", workerId, evaluator
        .getEvaluatorDescriptor().getNodeDescriptor().getName());
    Preconditions.checkState(!contextsByWorkerId.containsKey(workerId));
    Configuration contextConf =
        ContextConfiguration.CONF.set(ContextConfiguration.IDENTIFIER, workerId + "").build();
    setJVMOptions(evaluator);
    if (workerId != MyriaConstants.MASTER_ID) {
      contextConf = Configurations.merge(contextConf, workerConfs.get(workerId));
    }
    evaluator.submitContext(Configurations.merge(contextConf, globalConf));
  }

  private void launchMaster() throws InjectionException {
    Preconditions.checkState(state == DriverState.PREPARING_MASTER);
    requestMasterEvaluator();
  }

  private void launchWorkers() throws InjectionException {
    Preconditions.checkState(state == DriverState.PREPARING_WORKERS);
    for (final Integer workerId : workerConfs.keySet()) {
      requestWorkerEvaluator(workerId);
    }
  }

  /**
   * The driver is ready to run.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      LOGGER.info("Driver started at {}", startTime);
      Preconditions.checkState(state == DriverState.INIT);
      state = DriverState.PREPARING_MASTER;
      try {
        launchMaster();
      } catch (InjectionException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Shutting down the job driver: close the evaluators.
   */
  final class StopHandler implements EventHandler<StopTime> {
    @Override
    public void onNext(final StopTime stopTime) {
      LOGGER.info("Driver stopped at {}", stopTime);
      for (final RunningTask task : tasksByWorkerId.values()) {
        task.getActiveContext().close();
      }
    }
  }

  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator evaluator) {
      String node = evaluator.getEvaluatorDescriptor().getNodeDescriptor().getName();
      LOGGER.info("Allocated evaluator {} on node {}", evaluator.getId(), node);
      Integer workerId = workerIdsPendingEvaluatorAllocation.poll();
      Preconditions.checkState(workerId != null, "No worker ID waiting for an evaluator!");
      doTransition(workerId, TaskStateEvent.EVALUATOR_ALLOCATED, evaluator);
    }
  }

  final class CompletedEvaluatorHandler implements EventHandler<CompletedEvaluator> {
    @Override
    public void onNext(final CompletedEvaluator eval) {
      throw new IllegalStateException("Unexpected CompletedEvaluator: " + eval.getId());
    }
  }

  final class EvaluatorFailureHandler implements EventHandler<FailedEvaluator> {
    @Override
    public void onNext(final FailedEvaluator failedEvaluator) {
      LOGGER.warn("FailedEvaluator: {}", failedEvaluator);
      // respawn evaluator and reschedule task if configured
      List<FailedContext> failedContexts = failedEvaluator.getFailedContextList();
      // we should have at most one context in the list (since we only allocate the root context)
      if (failedContexts.size() > 0) {
        Preconditions.checkState(failedContexts.size() == 1);
        FailedContext failedContext = failedContexts.get(0);
        int workerId = Integer.valueOf(failedContext.getId());
        doTransition(workerId, TaskStateEvent.EVALUATOR_FAILED, failedEvaluator);
      } else {
        throw new IllegalStateException("Could not find worker ID for failed evaluator: "
            + failedEvaluator);
      }
    }
  }

  final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext context) {
      String host = context.getEvaluatorDescriptor().getNodeDescriptor().getName();
      LOGGER.info("Context {} available on node {}", context.getId(), host);
      int workerId = Integer.valueOf(context.getId());
      doTransition(workerId, TaskStateEvent.CONTEXT_ALLOCATED, context);
    }
  }

  final class ContextFailureHandler implements EventHandler<FailedContext> {
    @Override
    public void onNext(final FailedContext failedContext) {
      LOGGER.error("FailedContext: {}", failedContext);
      int workerId = Integer.valueOf(failedContext.getId());
      doTransition(workerId, TaskStateEvent.CONTEXT_FAILED, failedContext);
    }
  }

  final class RunningTaskHandler implements EventHandler<RunningTask> {
    @Override
    public void onNext(final RunningTask task) {
      LOGGER.info("Running task: {}", task.getId());
      int workerId = Integer.valueOf(task.getId());
      doTransition(workerId, TaskStateEvent.TASK_RUNNING, task);
    }
  }

  final class CompletedTaskHandler implements EventHandler<CompletedTask> {
    @Override
    public void onNext(final CompletedTask task) {
      throw new IllegalStateException("Unexpected CompletedTask: " + task.getId());
    }
  }

  final class TaskFailureHandler implements EventHandler<FailedTask> {
    @Override
    public void onNext(final FailedTask failedTask) {
      LOGGER.warn("FailedTask (ID {}): {}\n{}", failedTask.getId(), failedTask.getMessage(),
          failedTask.getReason());
      int workerId = Integer.valueOf(failedTask.getId());
      doTransition(workerId, TaskStateEvent.TASK_FAILED, failedTask);
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
      // We received a failed worker ack or recovered worker ack from the coordinator.
      int workerId = controlM.getWorkerId();
      LOGGER.info("Received {} for worker {} from task {}", controlM.getType(), workerId,
          taskMessage.getMessageSourceID());
      if (controlM.getType() == ControlMessage.Type.REMOVE_WORKER_ACK) {
        doTransition(workerId, TaskStateEvent.REMOVE_WORKER_ACK, null);
      } else if (controlM.getType() == ControlMessage.Type.ADD_WORKER_ACK) {
        doTransition(workerId, TaskStateEvent.ADD_WORKER_ACK, null);
      } else {
        throw new IllegalStateException(
            "Expected control message from coordinator to be ADD_WORKER_ACK or REMOVE_WORKER_ACK, got "
                + controlM.getType());
      }
    }
  }
}
