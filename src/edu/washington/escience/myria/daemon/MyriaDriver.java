package edu.washington.escience.myria.daemon;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

import javax.inject.Inject;

import org.apache.reef.driver.client.JobMessageObserver;
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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Striped;
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
import edu.washington.escience.myria.util.concurrent.RenamingThreadFactory;

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
  private final JobMessageObserver launcher;
  private final Configuration globalConf;
  private final Injector globalConfInjector;
  private final ImmutableMap<Integer, Configuration> workerConfs;
  /**
   * This is a workaround for the fact that YARN (and therefore REEF) doesn't allow you to associate
   * an identifier with a container request, so we can't tell which EvaluatorRequest an
   * AllocatedEvaluator corresponds to. Therefore, we assign worker IDs to AllocatedEvaluators in
   * FIFO request order.
   */
  private final Queue<Integer> workerIdsPendingEvaluatorAllocation;
  private final ConcurrentMap<Integer, RunningTask> tasksByWorkerId;
  private final ConcurrentMap<Integer, ActiveContext> contextsByWorkerId;
  private final ConcurrentMap<Integer, AllocatedEvaluator> evaluatorsByWorkerId;
  private final AtomicInteger numberWorkersPending;
  private final ConcurrentMap<Integer, WorkerAckHandler> addWorkerAckHandlers;
  private final ConcurrentMap<Integer, WorkerAckHandler> removeWorkerAckHandlers;
  private final ConcurrentMap<Integer, CoordinatorAckHandler> addCoordinatorAckHandlers;
  private final ConcurrentMap<Integer, CoordinatorAckHandler> removeCoordinatorAckHandlers;
  private final ExecutorService transitionExecutor;

  private static final int WORKER_ACK_TIMEOUT_MILLIS = 5000;
  static final String DRIVER_PING_MSG = "PING";
  static final String DRIVER_PING_ACK_MSG = "PONG";

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
    PENDING_EVALUATOR_REQUEST, PENDING_EVALUATOR, PENDING_CONTEXT, PENDING_TASK, PENDING_TASK_RUNNING_ACK, READY, FAILED_EVALUATOR_PENDING_TASK_FAILED_ACK, FAILED_CONTEXT_PENDING_TASK_FAILED_ACK, FAILED_TASK_PENDING_TASK_FAILED_ACK
  };

  private final Striped<Lock> workerStateTransitionLocks;
  private final ConcurrentMap<Integer, TaskState> workerStates;

  private enum TaskStateEvent {
    EVALUATOR_SUBMITTED, EVALUATOR_ALLOCATED, CONTEXT_ALLOCATED, TASK_RUNNING, TASK_RUNNING_ACK, TASK_FAILED, TASK_FAILED_ACK, CONTEXT_FAILED, EVALUATOR_FAILED
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

  @SuppressWarnings("unchecked")
  private ImmutableTable<TaskState, TaskStateEvent, TaskStateTransition> initializeTaskStateTransitions() {
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
              updateDriverStateOnWorkerFailure(wid);
              requestWorkerEvaluator(wid);
            }))
        .put(TaskState.PENDING_TASK, TaskStateEvent.TASK_RUNNING,
            TaskStateTransition.of(TaskState.PENDING_TASK_RUNNING_ACK, (wid, ctx) -> {
              tasksByWorkerId.put(wid, (RunningTask) ctx);
              recoverWorker(wid);
            }))
        .put(TaskState.PENDING_TASK, TaskStateEvent.TASK_FAILED,
            TaskStateTransition.of(TaskState.PENDING_TASK, (wid, ctx) -> {
              updateDriverStateOnWorkerFailure(wid);
              scheduleTask(wid);
            }))
        .put(TaskState.PENDING_TASK, TaskStateEvent.CONTEXT_FAILED,
            TaskStateTransition.of(TaskState.PENDING_CONTEXT, (wid, ctx) -> {
              contextsByWorkerId.remove(wid);
              updateDriverStateOnWorkerFailure(wid);
              allocateWorkerContext(wid);
            }))
        .put(TaskState.PENDING_TASK, TaskStateEvent.EVALUATOR_FAILED,
            TaskStateTransition.of(TaskState.PENDING_EVALUATOR_REQUEST, (wid, ctx) -> {
              contextsByWorkerId.remove(wid);
              evaluatorsByWorkerId.remove(wid);
              updateDriverStateOnWorkerFailure(wid);
              requestWorkerEvaluator(wid);
            }))
        .put(
            TaskState.PENDING_TASK_RUNNING_ACK,
            TaskStateEvent.TASK_RUNNING_ACK,
            TaskStateTransition.of(TaskState.READY,
                (wid, ctx) -> updateDriverStateOnWorkerReady(wid)))
        .put(TaskState.PENDING_TASK_RUNNING_ACK, TaskStateEvent.TASK_FAILED,
            TaskStateTransition.of(TaskState.FAILED_TASK_PENDING_TASK_FAILED_ACK, (wid, ctx) -> {
              tasksByWorkerId.remove(wid);
              updateDriverStateOnWorkerFailure(wid);
              removeWorker(wid);
            }))
        .put(
            TaskState.PENDING_TASK_RUNNING_ACK,
            TaskStateEvent.CONTEXT_FAILED,
            TaskStateTransition.of(TaskState.FAILED_CONTEXT_PENDING_TASK_FAILED_ACK,
                (wid, ctx) -> {
                  contextsByWorkerId.remove(wid);
                  updateDriverStateOnWorkerFailure(wid);
                  removeWorker(wid);
                }))
        .put(
            TaskState.PENDING_TASK_RUNNING_ACK,
            TaskStateEvent.EVALUATOR_FAILED,
            TaskStateTransition.of(TaskState.FAILED_EVALUATOR_PENDING_TASK_FAILED_ACK,
                (wid, ctx) -> {
                  contextsByWorkerId.remove(wid);
                  evaluatorsByWorkerId.remove(wid);
                  updateDriverStateOnWorkerFailure(wid);
                  removeWorker(wid);
                }))
        .put(TaskState.READY, TaskStateEvent.TASK_FAILED,
            TaskStateTransition.of(TaskState.FAILED_TASK_PENDING_TASK_FAILED_ACK, (wid, ctx) -> {
              tasksByWorkerId.remove(wid);
              updateDriverStateOnWorkerFailure(wid);
              removeWorker(wid);
            }))
        .put(
            TaskState.READY,
            TaskStateEvent.CONTEXT_FAILED,
            TaskStateTransition.of(TaskState.FAILED_CONTEXT_PENDING_TASK_FAILED_ACK,
                (wid, ctx) -> {
                  tasksByWorkerId.remove(wid);
                  contextsByWorkerId.remove(wid);
                  updateDriverStateOnWorkerFailure(wid);
                  removeWorker(wid);
                }))
        .put(
            TaskState.READY,
            TaskStateEvent.EVALUATOR_FAILED,
            TaskStateTransition.of(TaskState.FAILED_EVALUATOR_PENDING_TASK_FAILED_ACK,
                (wid, ctx) -> {
                  tasksByWorkerId.remove(wid);
                  contextsByWorkerId.remove(wid);
                  evaluatorsByWorkerId.remove(wid);
                  updateDriverStateOnWorkerFailure(wid);
                  removeWorker(wid);
                }))
        .put(TaskState.FAILED_TASK_PENDING_TASK_FAILED_ACK, TaskStateEvent.TASK_FAILED_ACK,
            TaskStateTransition.of(TaskState.PENDING_TASK, (wid, ctx) -> {
              scheduleTask(wid);
            }))
        .put(TaskState.FAILED_CONTEXT_PENDING_TASK_FAILED_ACK, TaskStateEvent.TASK_FAILED_ACK,
            TaskStateTransition.of(TaskState.PENDING_CONTEXT, (wid, ctx) -> {
              allocateWorkerContext(wid);
            }))
        .put(TaskState.FAILED_EVALUATOR_PENDING_TASK_FAILED_ACK, TaskStateEvent.TASK_FAILED_ACK,
            TaskStateTransition.of(TaskState.PENDING_EVALUATOR_REQUEST, (wid, ctx) -> {
              requestWorkerEvaluator(wid);
            })).build();
  }

  public void doTransition(final int workerId, final TaskStateEvent event, final Object context) {
    // NB: this lock is reentrant, so any transitions induced by the transition handler will not
    // deadlock the thread.
    final Lock workerLock = workerStateTransitionLocks.get(workerId);
    workerLock.lock();
    try {
      final TaskState workerState = workerStates.get(workerId);
      final TaskStateTransition transition = taskStateTransitions.get(workerState, event);
      if (transition != null) {
        workerStates.replace(workerId, transition.newState);
        LOGGER
            .info(
                "Performing transition on event {} from state {} to state {} (worker ID {}, context {})",
                event, workerState, transition.newState, workerId, context);
        try {
          transition.handler.onTransition(workerId, context);
        } catch (final Exception e) {
          throw new RuntimeException(e);
        }
      } else {
        throw new IllegalStateException(String.format(
            "No transition defined for state %s and event %s (worker ID %s)", workerState, event,
            workerId));
      }
    } finally {
      workerLock.unlock();
    }
  }

  /**
   * Schedules a worker transition and its handler to be executed on a separate thread pool.
   * 
   * @param workerId
   * @param event
   * @param context
   */
  public void scheduleTransition(final int workerId, final TaskStateEvent event,
      final Object context) {
    transitionExecutor.execute(() -> doTransition(workerId, event, context));
  }

  @FunctionalInterface
  private interface WorkerAckHandler {
    public void onAck(final int workerId, final int senderId) throws Exception;
  }

  @FunctionalInterface
  private interface CoordinatorAckHandler {
    public void onAck(final int workerId) throws Exception;
  }

  @Inject
  public MyriaDriver(final LocalAddressProvider addressProvider,
      final EvaluatorRequestor requestor, final JVMProcessFactory jvmProcessFactory,
      final JobMessageObserver launcher,
      final @Parameter(MyriaDriverLauncher.SerializedGlobalConf.class) String serializedGlobalConf)
      throws Exception {
    this.requestor = requestor;
    this.addressProvider = addressProvider;
    this.jvmProcessFactory = jvmProcessFactory;
    this.launcher = launcher;
    globalConf = new AvroConfigurationSerializer().fromString(serializedGlobalConf);
    globalConfInjector = Tang.Factory.getTang().newInjector(globalConf);
    workerConfs = initializeWorkerConfs();
    workerIdsPendingEvaluatorAllocation = new ConcurrentLinkedQueue<>();
    tasksByWorkerId = new ConcurrentHashMap<>();
    contextsByWorkerId = new ConcurrentHashMap<>();
    evaluatorsByWorkerId = new ConcurrentHashMap<>();
    numberWorkersPending = new AtomicInteger(workerConfs.size());
    addWorkerAckHandlers = new ConcurrentHashMap<>();
    removeWorkerAckHandlers = new ConcurrentHashMap<>();
    addCoordinatorAckHandlers = new ConcurrentHashMap<>();
    removeCoordinatorAckHandlers = new ConcurrentHashMap<>();
    workerStates = initializeWorkerStates();
    taskStateTransitions = initializeTaskStateTransitions();
    workerStateTransitionLocks = Striped.lock(workerConfs.size() + 1); // +1 for coordinator
    // Since all worker transitions acquire a per-worker lock, concurrency is limited to the number
    // of workers.
    transitionExecutor =
        Executors.newFixedThreadPool(workerConfs.size() + 1, new RenamingThreadFactory(
            "WorkerTransitionThreadPool"));
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
      } catch (final UnknownHostException e) {
        LOGGER.warn("Failed to get canonical hostname for host {}", masterHost);
      }
    }
    return reefMasterHost;
  }

  private ImmutableMap<Integer, Configuration> initializeWorkerConfs() throws InjectionException,
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
    final String host = injector.getNamedInstance(MyriaWorkerConfigurationModule.WorkerHost.class);
    // REEF (org.apache.reef.wake.remote.address.HostnameBasedLocalAddressProvider) will
    // unpredictably pick a local DNS name or IP address instead of "localhost" or 127.0.0.1
    String reefHost = host;
    if (host.equals("localhost") || host.equals("127.0.0.1")) {
      try {
        reefHost = InetAddress.getByName(addressProvider.getLocalAddress()).getHostName();
        LOGGER.info("Original host: {}, HostnameBasedLocalAddressProvider returned {}", host,
            reefHost);
      } catch (final UnknownHostException e) {
        LOGGER.warn("Failed to get canonical hostname for host {}", host);
      }
    }
    return reefHost;
  }

  private ConcurrentMap<Integer, TaskState> initializeWorkerStates() {
    final ConcurrentMap<Integer, TaskState> workerStates =
        new ConcurrentHashMap<>(workerConfs.size() + 1);
    workerStates.put(MyriaConstants.MASTER_ID, TaskState.PENDING_EVALUATOR_REQUEST);
    for (final Integer workerId : workerConfs.keySet()) {
      workerStates.put(workerId, TaskState.PENDING_EVALUATOR_REQUEST);
    }
    return workerStates;
  }

  private SocketInfo getSocketInfoForWorker(final int workerId) throws InjectionException {
    final Configuration workerConf = workerConfs.get(workerId);
    final Injector injector = Tang.Factory.getTang().newInjector(workerConf);
    // we don't use getHostFromWorkerConf() because we want to keep our original hostname
    final String host = injector.getNamedInstance(MyriaWorkerConfigurationModule.WorkerHost.class);
    final Integer port = injector.getNamedInstance(MyriaWorkerConfigurationModule.WorkerPort.class);
    return new SocketInfo(host, port);
  }

  private void requestWorkerEvaluator(final int workerId) throws InjectionException {
    Preconditions.checkArgument(workerId != MyriaConstants.MASTER_ID);
    final int jvmMemoryQuotaMB =
        (int) (1024 * globalConfInjector
            .getNamedInstance(MyriaGlobalConfigurationModule.MemoryQuotaGB.class));
    final int numberVCores =
        globalConfInjector.getNamedInstance(MyriaGlobalConfigurationModule.NumberVCores.class);
    LOGGER.info("Requesting evaluator for worker {} with {} vcores, {} MB memory.", workerId,
        numberVCores, jvmMemoryQuotaMB);
    final Configuration workerConf = workerConfs.get(workerId);
    final String hostname = getHostFromWorkerConf(workerConf);
    final EvaluatorRequest workerRequest =
        EvaluatorRequest.newBuilder().setNumber(1).setMemory(jvmMemoryQuotaMB)
            .setNumberOfCores(numberVCores).addNodeName(hostname).build();
    doTransition(workerId, TaskStateEvent.EVALUATOR_SUBMITTED, workerRequest);
  }

  private void requestMasterEvaluator() throws InjectionException {
    final String masterHost = getMasterHost();
    final int jvmMemoryQuotaMB =
        (int) (1024 * globalConfInjector
            .getNamedInstance(MyriaGlobalConfigurationModule.MemoryQuotaGB.class));
    final int numberVCores =
        globalConfInjector.getNamedInstance(MyriaGlobalConfigurationModule.NumberVCores.class);
    LOGGER.info("Requesting master evaluator with {} vcores, {} MB memory.", numberVCores,
        jvmMemoryQuotaMB);
    final EvaluatorRequest masterRequest =
        EvaluatorRequest.newBuilder().setNumber(1).setMemory(jvmMemoryQuotaMB)
            .setNumberOfCores(numberVCores).addNodeName(masterHost).build();
    doTransition(MyriaConstants.MASTER_ID, TaskStateEvent.EVALUATOR_SUBMITTED, masterRequest);
  }

  private void setJVMOptions(final AllocatedEvaluator evaluator) throws InjectionException {
    final int jvmHeapSizeMinMB =
        (int) (1024 * globalConfInjector
            .getNamedInstance(MyriaGlobalConfigurationModule.JvmHeapSizeMinGB.class));
    final int jvmHeapSizeMaxMB =
        (int) (1024 * globalConfInjector
            .getNamedInstance(MyriaGlobalConfigurationModule.JvmHeapSizeMaxGB.class));
    final Set<String> jvmOptions =
        globalConfInjector.getNamedInstance(MyriaGlobalConfigurationModule.JvmOptions.class);
    final JVMProcess jvmProcess =
        jvmProcessFactory.newEvaluatorProcess()
            .addOption(String.format("-Xms%dm", jvmHeapSizeMinMB))
            .addOption(String.format("-Xmx%dm", jvmHeapSizeMaxMB))
            // for native libraries
            .addOption("-Djava.library.path=./reef/global");
    for (final String option : jvmOptions) {
      jvmProcess.addOption(option);
    }
    evaluator.setProcess(jvmProcess);
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

  private void allocateWorkerContext(final int workerId) throws InjectionException {
    Preconditions.checkState(evaluatorsByWorkerId.containsKey(workerId));
    final AllocatedEvaluator evaluator = evaluatorsByWorkerId.get(workerId);
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

  private void scheduleTask(final int workerId) {
    Preconditions.checkState(contextsByWorkerId.containsKey(workerId));
    final ActiveContext context = contextsByWorkerId.get(workerId);
    LOGGER.info("Scheduling task for worker ID {} on context {}, evaluator {}", workerId,
        context.getId(), context.getEvaluatorId());
    Configuration taskConf;
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
              .set(TaskConfiguration.IDENTIFIER, workerId + "")
              .set(TaskConfiguration.ON_SEND_MESSAGE, Worker.class)
              .set(TaskConfiguration.ON_MESSAGE, Worker.class).build();
    }
    context.submitTask(taskConf);
  }

  private ImmutableSet<Integer> getAliveWorkers() {
    final ImmutableSet.Builder<Integer> builder = ImmutableSet.builder();
    workerStates.forEach((wid, state) -> {
      if (!wid.equals(MyriaConstants.MASTER_ID) && state.equals(TaskState.READY)) {
        builder.add(wid);
      }
    });
    return builder.build();
  }

  private boolean sendMessageToWorker(final int workerId, final TransportMessage message) {
    boolean messageSent = false;
    // if the worker we're sending this message to is in a state transition, we abort
    if (workerStateTransitionLocks.get(workerId).tryLock()) {
      try {
        final RunningTask workerToNotifyTask = tasksByWorkerId.get(workerId);
        if (workerToNotifyTask != null) {
          workerToNotifyTask.send(message.toByteArray());
          messageSent = true;
        }
      } finally {
        workerStateTransitionLocks.get(workerId).unlock();
      }
    } else {
      LOGGER.warn(
          "worker {} is in a state transition (current state: {}), aborting send of message: {}",
          workerId, workerStates.get(workerId), message);
    }
    return messageSent;
  }

  private void sendMessageToCoordinator(final TransportMessage message) {
    // The coordinator can never be in a state transition after it comes up, so it should always be
    // safe to send it messages concurrently.
    Preconditions.checkState(workerStates.get(MyriaConstants.MASTER_ID).equals(TaskState.READY));
    final RunningTask coordinatorTask = tasksByWorkerId.get(MyriaConstants.MASTER_ID);
    coordinatorTask.send(message.toByteArray());
  }

  private void registerWorkerAddAckHandler(final int workerId, final WorkerAckHandler handler) {
    addWorkerAckHandlers.put(workerId, handler);
  }

  private void registerWorkerRemoveAckHandler(final int workerId, final WorkerAckHandler handler) {
    removeWorkerAckHandlers.put(workerId, handler);
  }

  private void registerCoordinatorAddAckHandler(final int workerId,
      final CoordinatorAckHandler handler) {
    addCoordinatorAckHandlers.put(workerId, handler);
  }

  private void registerCoordinatorRemoveAckHandler(final int workerId,
      final CoordinatorAckHandler handler) {
    removeCoordinatorAckHandlers.put(workerId, handler);
  }

  private void unregisterWorkerAddAckHandler(final int workerId) {
    addWorkerAckHandlers.remove(workerId);
  }

  private void unregisterWorkerRemoveAckHandler(final int workerId) {
    removeWorkerAckHandlers.remove(workerId);
  }

  private void unregisterCoordinatorAddAckHandler(final int workerId) {
    addCoordinatorAckHandlers.remove(workerId);
  }

  private void unregisterCoordinatorRemoveAckHandler(final int workerId) {
    removeCoordinatorAckHandlers.remove(workerId);
  }

  private void recoverWorker(final int workerId) throws InterruptedException {
    if (workerId != MyriaConstants.MASTER_ID) {
      // this is obviously racy but it doesn't matter since we timeout on acks
      final ImmutableSet<Integer> aliveWorkers = getAliveWorkers();
      final CountDownLatch acksPending = new CountDownLatch(aliveWorkers.size());
      final Set<Integer> ackedWorkers = Sets.newConcurrentHashSet();
      registerWorkerAddAckHandler(workerId, (wid, sid) -> {
        ackedWorkers.add(sid);
        acksPending.countDown();
      });
      LOGGER.info("Sending ADD_WORKER for worker {} to all {} alive workers", workerId,
          aliveWorkers.size());
      for (final Integer aliveWorkerId : aliveWorkers) {
        notifyWorkerOnRecovery(workerId, aliveWorkerId);
      }
      final boolean timedOut = !acksPending.await(WORKER_ACK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
      if (timedOut) {
        LOGGER.info("Timed out after {} ms while waiting for {} acks for ADD_WORKER on worker {}",
            WORKER_ACK_TIMEOUT_MILLIS, aliveWorkers.size(), workerId);
      }
      // Strictly speaking, this is incorrect since a stale ack could arrive during the next phase
      // in this state. We would need per-worker epoch counters to prevent this (advance the epoch
      // on each state transition, check the epoch of an ack message when it arrives and reject it
      // if stale). If this sort of bug ever shows up in tests, we'll consider this solution.
      unregisterWorkerAddAckHandler(workerId);
      LOGGER.info("Received {} of expected {} acks for ADD_WORKER on worker {}",
          ackedWorkers.size(), aliveWorkers.size(), workerId);
      final CountDownLatch coordinatorAcked = new CountDownLatch(1);
      registerCoordinatorAddAckHandler(workerId, (wid) -> {
        coordinatorAcked.countDown();
      });
      notifyCoordinatorOnRecovery(workerId, ackedWorkers);
      coordinatorAcked.await();
      unregisterCoordinatorAddAckHandler(workerId);
      // we need to perform the transition in our own thread or we can't re-enter the lock
      doTransition(workerId, TaskStateEvent.TASK_RUNNING_ACK, ackedWorkers);
    } else {
      // coordinator can't get any acks when it starts
      doTransition(workerId, TaskStateEvent.TASK_RUNNING_ACK, ImmutableSet.of());
    }
  }

  private void removeWorker(final int workerId) throws InterruptedException {
    Preconditions.checkState(workerId != MyriaConstants.MASTER_ID);
    // this is obviously racy but it doesn't matter since we timeout on acks
    final ImmutableSet<Integer> aliveWorkers = getAliveWorkers();
    final CountDownLatch acksPending = new CountDownLatch(aliveWorkers.size());
    final Set<Integer> ackedWorkers = Sets.newConcurrentHashSet();
    registerWorkerRemoveAckHandler(workerId, (wid, sid) -> {
      ackedWorkers.add(sid);
      acksPending.countDown();
    });
    LOGGER.info("Sending REMOVE_WORKER for worker {} to all {} alive workers", workerId,
        aliveWorkers.size());
    for (final Integer aliveWorkerId : aliveWorkers) {
      notifyWorkerOnFailure(workerId, aliveWorkerId);
    }
    final boolean timedOut = !acksPending.await(WORKER_ACK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    if (timedOut) {
      LOGGER.info("Timed out after {} ms while waiting for {} acks for REMOVE_WORKER on worker {}",
          WORKER_ACK_TIMEOUT_MILLIS, aliveWorkers.size(), workerId);
    }
    // Strictly speaking, this is incorrect since a stale ack could arrive during the next phase
    // in this state. We would need per-worker epoch counters to prevent this (advance the epoch
    // on each state transition, check the epoch of an ack message when it arrives and reject it
    // if stale). If this sort of bug ever shows up in tests, we'll consider this solution.
    unregisterWorkerRemoveAckHandler(workerId);
    LOGGER.info("Received {} of expected {} acks for REMOVE_WORKER on worker {}",
        ackedWorkers.size(), aliveWorkers.size(), workerId);
    final CountDownLatch coordinatorAcked = new CountDownLatch(1);
    registerCoordinatorRemoveAckHandler(workerId, (wid) -> {
      coordinatorAcked.countDown();
    });
    notifyCoordinatorOnFailure(workerId, ackedWorkers);
    coordinatorAcked.await();
    unregisterCoordinatorRemoveAckHandler(workerId);
    // we need to perform the transition in our own thread or we can't re-enter the lock
    doTransition(workerId, TaskStateEvent.TASK_FAILED_ACK, ackedWorkers);
  }

  private void notifyWorkerOnFailure(final int workerId, final int workerToNotifyId) {
    // we should never get here on coordinator failure
    Preconditions.checkArgument(workerId != MyriaConstants.MASTER_ID);
    LOGGER.info("Sending REMOVE_WORKER for worker {} to worker {}", workerId, workerToNotifyId);
    final TransportMessage workerFailed = IPCUtils.removeWorkerTM(workerId, null);
    if (!sendMessageToWorker(workerToNotifyId, workerFailed)) {
      LOGGER.warn("Unable to send REMOVE_WORKER for worker {} to worker {}", workerId,
          workerToNotifyId);
    }
  }

  private void notifyWorkerOnRecovery(final int workerId, final int workerToNotifyId) {
    // we should never get here on coordinator failure
    Preconditions.checkArgument(workerId != MyriaConstants.MASTER_ID);
    SocketInfo si;
    try {
      si = getSocketInfoForWorker(workerId);
    } catch (final InjectionException e) {
      LOGGER.error("Failed to get SocketInfo for worker {}:\n{}", workerId, e);
      return;
    }
    LOGGER.info("Sending ADD_WORKER for worker {} to worker {}", workerId, workerToNotifyId);
    final TransportMessage workerRecovered = IPCUtils.addWorkerTM(workerId, si, null);
    if (!sendMessageToWorker(workerToNotifyId, workerRecovered)) {
      LOGGER.warn("Unable to send ADD_WORKER for worker {} to worker {}", workerId,
          workerToNotifyId);
    }
  }

  private void notifyCoordinatorOnFailure(final int workerId, final Set<Integer> ackedWorkers) {
    Preconditions.checkState(workerId != MyriaConstants.MASTER_ID);
    LOGGER.info("Sending REMOVE_WORKER for worker {} to coordinator", workerId);
    final TransportMessage workerFailed = IPCUtils.removeWorkerTM(workerId, ackedWorkers);
    sendMessageToCoordinator(workerFailed);
  }

  private void notifyCoordinatorOnRecovery(final int workerId, final Set<Integer> ackedWorkers) {
    Preconditions.checkState(workerId != MyriaConstants.MASTER_ID);
    SocketInfo si;
    try {
      si = getSocketInfoForWorker(workerId);
    } catch (final InjectionException e) {
      LOGGER.error("Failed to get SocketInfo for worker {}:\n{}", workerId, e);
      return;
    }
    LOGGER.info("Sending ADD_WORKER for worker {} to coordinator", workerId);
    final TransportMessage workerRecovered = IPCUtils.addWorkerTM(workerId, si, ackedWorkers);
    sendMessageToCoordinator(workerRecovered);
  }

  private void onWorkerAddAck(final int workerId, final int senderId) {
    final WorkerAckHandler ackHandler =
        addWorkerAckHandlers.getOrDefault(workerId, (wid, sid) -> LOGGER.warn(
            "No worker handler registered for ADD_WORKER_ACK (worker ID {}, sender ID {})", wid,
            sid));
    try {
      ackHandler.onAck(workerId, senderId);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void onWorkerRemoveAck(final int workerId, final int senderId) {
    final WorkerAckHandler ackHandler =
        removeWorkerAckHandlers.getOrDefault(workerId, (wid, sid) -> LOGGER.warn(
            "No worker handler registered for REMOVE_WORKER_ACK (worker ID {}, sender ID {})", wid,
            sid));
    try {
      ackHandler.onAck(workerId, senderId);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void onCoordinatorAddAck(final int workerId) {
    final CoordinatorAckHandler ackHandler =
        addCoordinatorAckHandlers.getOrDefault(workerId, (wid) -> LOGGER.warn(
            "No coordinator handler registered for ADD_WORKER_ACK (worker ID {})", wid));
    try {
      ackHandler.onAck(workerId);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void onCoordinatorRemoveAck(final int workerId) {
    final CoordinatorAckHandler ackHandler =
        removeCoordinatorAckHandlers.getOrDefault(workerId, (wid) -> LOGGER.warn(
            "No coordinator handler registered for REMOVE_WORKER_ACK (worker ID {})", wid));
    try {
      ackHandler.onAck(workerId);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void updateDriverStateOnWorkerReady(final int workerId) throws InjectionException {
    Preconditions.checkState(tasksByWorkerId.containsKey(workerId));
    String message = String.format("Worker %s ready", workerId);
    launcher.sendMessageToClient(message.getBytes(StandardCharsets.UTF_8));
    if (state == DriverState.PREPARING_MASTER) {
      Preconditions.checkState(workerId == MyriaConstants.MASTER_ID);
      message = String.format("Master is running, starting %s workers...", workerConfs.size());
      LOGGER.info(message);
      launcher.sendMessageToClient(message.getBytes(StandardCharsets.UTF_8));
      state = DriverState.PREPARING_WORKERS;
      launchWorkers();
    } else if (state == DriverState.PREPARING_WORKERS) {
      Preconditions.checkState(workerId != MyriaConstants.MASTER_ID);
      if (numberWorkersPending.decrementAndGet() == 0) {
        message = String.format("All %s workers running, ready for queries...", workerConfs.size());
        LOGGER.info(message);
        launcher.sendMessageToClient(message.getBytes(StandardCharsets.UTF_8));
        state = DriverState.READY;
      }
    }
  }

  private void updateDriverStateOnWorkerFailure(final int workerId) {
    final String message = String.format("Worker %s failed", workerId);
    launcher.sendMessageToClient(message.getBytes(StandardCharsets.UTF_8));
    if (workerId == MyriaConstants.MASTER_ID) {
      throw new RuntimeException("Shutting down driver on coordinator failure");
    } else if (state == DriverState.PREPARING_WORKERS) {
      LOGGER.warn("Worker failed in PREPARING_WORKERS phase, {} workers pending...",
          numberWorkersPending.get());
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
      } catch (final InjectionException e) {
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
      final String node = evaluator.getEvaluatorDescriptor().getNodeDescriptor().getName();
      LOGGER.info("Allocated evaluator {} on node {}", evaluator.getId(), node);
      final Integer workerId = workerIdsPendingEvaluatorAllocation.poll();
      Preconditions.checkState(workerId != null, "No worker ID waiting for an evaluator!");
      scheduleTransition(workerId, TaskStateEvent.EVALUATOR_ALLOCATED, evaluator);
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
      final List<FailedContext> failedContexts = failedEvaluator.getFailedContextList();
      // we should have at most one context in the list (since we only allocate the root context)
      if (failedContexts.size() > 0) {
        Preconditions.checkState(failedContexts.size() == 1);
        final FailedContext failedContext = failedContexts.get(0);
        final int workerId = Integer.valueOf(failedContext.getId());
        scheduleTransition(workerId, TaskStateEvent.EVALUATOR_FAILED, failedEvaluator);
      } else {
        throw new IllegalStateException("Could not find worker ID for failed evaluator: "
            + failedEvaluator);
      }
    }
  }

  final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext context) {
      final String host = context.getEvaluatorDescriptor().getNodeDescriptor().getName();
      LOGGER.info("Context {} available on node {}", context.getId(), host);
      final int workerId = Integer.valueOf(context.getId());
      scheduleTransition(workerId, TaskStateEvent.CONTEXT_ALLOCATED, context);
    }
  }

  final class ContextFailureHandler implements EventHandler<FailedContext> {
    @Override
    public void onNext(final FailedContext failedContext) {
      LOGGER.error("FailedContext: {}", failedContext);
      final int workerId = Integer.valueOf(failedContext.getId());
      scheduleTransition(workerId, TaskStateEvent.CONTEXT_FAILED, failedContext);
    }
  }

  final class RunningTaskHandler implements EventHandler<RunningTask> {
    @Override
    public void onNext(final RunningTask task) {
      LOGGER.info("Running task: {}", task.getId());
      final int workerId = Integer.valueOf(task.getId());
      scheduleTransition(workerId, TaskStateEvent.TASK_RUNNING, task);
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
      final int workerId = Integer.valueOf(failedTask.getId());
      scheduleTransition(workerId, TaskStateEvent.TASK_FAILED, failedTask);
    }
  }

  // NB: REEF only uses a single TaskMessage dispatch thread per Evaluator, so TaskMessage handlers
  // should never block!
  final class TaskMessageHandler implements EventHandler<TaskMessage> {
    @Override
    public void onNext(final TaskMessage taskMessage) {
      final int senderId = Integer.valueOf(taskMessage.getMessageSourceID());
      TransportMessage m;
      try {
        m = TransportMessage.parseFrom(taskMessage.get());
      } catch (final InvalidProtocolBufferException e) {
        LOGGER.warn("Could not parse TransportMessage from task message", e);
        return;
      }
      final ControlMessage controlM = m.getControlMessage();
      // We received a failed worker ack or recovered worker ack from the coordinator.
      final int workerId = controlM.getWorkerId();
      LOGGER.info("Received {} for worker {} from worker {}", controlM.getType(), workerId,
          senderId);
      if (controlM.getType() == ControlMessage.Type.REMOVE_WORKER_ACK) {
        if (senderId == MyriaConstants.MASTER_ID) {
          onCoordinatorRemoveAck(workerId);
        } else {
          onWorkerRemoveAck(workerId, senderId);
        }
      } else if (controlM.getType() == ControlMessage.Type.ADD_WORKER_ACK) {
        if (senderId == MyriaConstants.MASTER_ID) {
          onCoordinatorAddAck(workerId);
        } else {
          onWorkerAddAck(workerId, senderId);
        }
      } else {
        throw new IllegalStateException(
            "Expected control message to be ADD_WORKER_ACK or REMOVE_WORKER_ACK, got "
                + controlM.getType());
      }
    }
  }

  final class ClientMessageHandler implements EventHandler<byte[]> {
    @Override
    public void onNext(final byte[] message) {
      final String msgStr = new String(message, StandardCharsets.UTF_8);
      Preconditions.checkArgument(msgStr.equals(MyriaDriver.DRIVER_PING_MSG));
      LOGGER.info("Message from Myria launcher: {}", msgStr);
      launcher
          .sendMessageToClient(MyriaDriver.DRIVER_PING_ACK_MSG.getBytes(StandardCharsets.UTF_8));
    }
  }
}
