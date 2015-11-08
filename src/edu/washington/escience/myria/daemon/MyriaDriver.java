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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

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
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
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
  private final ConcurrentMap<Integer, CountDownLatch> workerAddAcksPending;
  private final ConcurrentMap<Integer, CountDownLatch> workerRemoveAcksPending;
  private final ConcurrentMap<Integer, CountDownLatch> coordinatorAddAckPending;
  private final ConcurrentMap<Integer, CountDownLatch> coordinatorRemoveAckPending;
  private final SetMultimap<Integer, Integer> workerAddAcksReceived;
  private final SetMultimap<Integer, Integer> workerRemoveAcksReceived;
  private static final int WORKER_ACK_TIMEOUT_MILLIS = 5000;

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
    Lock workerLock = workerStateTransitionLocks.get(workerId);
    workerLock.lock();
    try {
      TaskState workerState = workerStates.get(workerId);
      TaskStateTransition transition = taskStateTransitions.get(workerState, event);
      if (transition != null) {
        workerStates.replace(workerId, transition.newState);
        LOGGER
            .info(
                "Performing transition on event {} from state {} to state {} (worker ID {}, context {})",
                event, workerState, transition.newState, workerId, context);
        try {
          transition.handler.onTransition(workerId, context);
        } catch (Exception e) {
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

  // TODO: inject JobMessageObserver so we can send messages to the driver launcher (using
  // JobMessageObserver.sendMessageToClient() in handler registered with
  // DriverConfiguration.ON_CLIENT_MESSAGE)
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
    workerConfs = initializeWorkerConfs();
    workerIdsPendingEvaluatorAllocation = new ConcurrentLinkedQueue<>();
    tasksByWorkerId = new ConcurrentHashMap<>();
    contextsByWorkerId = new ConcurrentHashMap<>();
    evaluatorsByWorkerId = new ConcurrentHashMap<>();
    numberWorkersPending = new AtomicInteger(workerConfs.size());
    workerAddAcksPending = new ConcurrentHashMap<>();
    workerRemoveAcksPending = new ConcurrentHashMap<>();
    coordinatorAddAckPending = new ConcurrentHashMap<>();
    coordinatorRemoveAckPending = new ConcurrentHashMap<>();
    workerAddAcksReceived =
        Multimaps.synchronizedSetMultimap(HashMultimap.<Integer, Integer>create());
    workerRemoveAcksReceived =
        Multimaps.synchronizedSetMultimap(HashMultimap.<Integer, Integer>create());
    workerStates = initializeWorkerStates();
    taskStateTransitions = initializeTaskStateTransitions();
    workerStateTransitionLocks = Striped.lock(workerConfs.size() + 1); // +1 for coordinator
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

  private ConcurrentMap<Integer, TaskState> initializeWorkerStates() {
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
              .set(TaskConfiguration.IDENTIFIER, workerId + "")
              .set(TaskConfiguration.ON_SEND_MESSAGE, Worker.class)
              .set(TaskConfiguration.ON_MESSAGE, Worker.class).build();
    }
    context.submitTask(taskConf);
  }

  private ImmutableSet<Integer> getAliveWorkers() {
    ImmutableSet.Builder<Integer> builder = ImmutableSet.builder();
    workerStates.forEach((wid, state) -> {
      if (!wid.equals(MyriaConstants.MASTER_ID) && state.equals(TaskState.READY)) {
        builder.add(wid);
      }
    });
    return builder.build();
  }

  private boolean sendMessageToWorker(final int workerId, final TransportMessage message) {
    boolean messageSent = false;
    // if the worker we're sending to is in the middle of a state transition, we abort
    if (workerStateTransitionLocks.get(workerId).tryLock()) {
      try {
        RunningTask workerToNotifyTask = tasksByWorkerId.get(workerId);
        if (workerToNotifyTask != null) {
          workerToNotifyTask.send(message.toByteArray());
          messageSent = true;
        }
      } finally {
        workerStateTransitionLocks.get(workerId).unlock();
      }
    }
    return messageSent;
  }

  private void recoverWorker(final int workerId) throws InterruptedException {
    if (workerId != MyriaConstants.MASTER_ID) {
      // this is obviously racy but it doesn't matter since we timeout on acks
      ImmutableSet<Integer> aliveWorkers = getAliveWorkers();
      CountDownLatch acksPending = new CountDownLatch(aliveWorkers.size());
      workerAddAcksPending.put(workerId, acksPending);
      for (Integer aliveWorkerId : aliveWorkers) {
        notifyWorkerOnRecovery(workerId, aliveWorkerId);
      }
      boolean timedOut = !acksPending.await(WORKER_ACK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
      if (timedOut) {
        LOGGER.info("Timed out after {} ms while waiting for {} acks for ADD_WORKER on worker {}",
            WORKER_ACK_TIMEOUT_MILLIS, aliveWorkers.size(), workerId);
      }
      Set<Integer> ackedWorkers = workerAddAcksReceived.removeAll(workerId);
      LOGGER.info("Received {} of expected {} acks for ADD_WORKER on worker {}",
          ackedWorkers.size(), aliveWorkers.size(), workerId);
      CountDownLatch coordinatorAcked = new CountDownLatch(1);
      coordinatorAddAckPending.put(workerId, coordinatorAcked);
      notifyCoordinatorOnRecovery(workerId, ackedWorkers);
      coordinatorAcked.await();
    } else {
      // coordinator can't get any acks when it starts
      doTransition(workerId, TaskStateEvent.TASK_RUNNING_ACK, ImmutableSet.of());
    }
  }

  private void removeWorker(final int workerId) throws InterruptedException {
    Preconditions.checkState(workerId != MyriaConstants.MASTER_ID);
    // this is obviously racy but it doesn't matter since we timeout on acks
    ImmutableSet<Integer> aliveWorkers = getAliveWorkers();
    CountDownLatch acksPending = new CountDownLatch(aliveWorkers.size());
    workerRemoveAcksPending.put(workerId, acksPending);
    for (Integer aliveWorkerId : aliveWorkers) {
      notifyWorkerOnFailure(workerId, aliveWorkerId);
    }
    boolean timedOut = !acksPending.await(WORKER_ACK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    if (timedOut) {
      LOGGER.info("Timed out after {} ms while waiting for {} acks for REMOVE_WORKER on worker {}",
          WORKER_ACK_TIMEOUT_MILLIS, aliveWorkers.size(), workerId);
    }
    Set<Integer> ackedWorkers = workerRemoveAcksReceived.removeAll(workerId);
    LOGGER.info("Received {} of expected {} acks for REMOVE_WORKER on worker {}",
        ackedWorkers.size(), aliveWorkers.size(), workerId);
    CountDownLatch coordinatorAcked = new CountDownLatch(1);
    coordinatorRemoveAckPending.put(workerId, coordinatorAcked);
    notifyCoordinatorOnFailure(workerId, ackedWorkers);
    coordinatorAcked.await();
  }

  private void notifyWorkerOnFailure(final int workerId, final int workerToNotifyId) {
    // we should never get here on coordinator failure
    Preconditions.checkArgument(workerId != MyriaConstants.MASTER_ID);
    LOGGER.info("Sending ADD_WORKER for worker {} to all alive workers (excluding coordinator)",
        workerId);
    final TransportMessage workerRecovered = IPCUtils.removeWorkerTM(workerId, null);
    sendMessageToWorker(workerToNotifyId, workerRecovered);
  }

  private void notifyWorkerOnRecovery(final int workerId, final int workerToNotifyId) {
    // we should never get here on coordinator failure
    Preconditions.checkArgument(workerId != MyriaConstants.MASTER_ID);
    SocketInfo si;
    try {
      si = getSocketInfoForWorker(workerId);
    } catch (InjectionException e) {
      LOGGER.error("Failed to get SocketInfo for worker {}:\n{}", workerId, e);
      return;
    }
    LOGGER.info("Sending ADD_WORKER for worker {} to all alive workers (excluding coordinator)",
        workerId);
    final TransportMessage workerRecovered = IPCUtils.addWorkerTM(workerId, si, null);
    sendMessageToWorker(workerToNotifyId, workerRecovered);
  }

  private void onRemoveAckFromWorker(final int workerId, final int senderId) {
    workerRemoveAcksReceived.put(workerId, senderId);
    workerRemoveAcksPending.get(workerId).countDown();
  }

  private void onRemoveAckFromCoordinator(final int workerId) {
    coordinatorRemoveAckPending.get(workerId).countDown();
    doTransition(workerId, TaskStateEvent.TASK_FAILED_ACK, null);
  }

  private void onAddAckFromWorker(final int workerId, final int senderId) {
    workerAddAcksReceived.put(workerId, senderId);
    workerAddAcksPending.get(workerId).countDown();
  }

  private void onAddAckFromCoordinator(final int workerId) {
    coordinatorAddAckPending.get(workerId).countDown();
    doTransition(workerId, TaskStateEvent.TASK_RUNNING_ACK, null);
  }

  private void notifyCoordinatorOnFailure(final int workerId, final Set<Integer> ackedWorkers) {
    Preconditions.checkState(workerId != MyriaConstants.MASTER_ID);
    LOGGER.info("Sending REMOVE_WORKER for worker {} to coordinator", workerId);
    final TransportMessage workerFailed = IPCUtils.removeWorkerTM(workerId, ackedWorkers);
    sendMessageToWorker(MyriaConstants.MASTER_ID, workerFailed);
  }

  private void notifyCoordinatorOnRecovery(final int workerId, final Set<Integer> ackedWorkers) {
    Preconditions.checkState(workerId != MyriaConstants.MASTER_ID);
    SocketInfo si;
    try {
      si = getSocketInfoForWorker(workerId);
    } catch (InjectionException e) {
      LOGGER.error("Failed to get SocketInfo for worker {}:\n{}", workerId, e);
      return;
    }
    LOGGER.info("Sending ADD_WORKER for worker {} to coordinator", workerId);
    final TransportMessage workerRecovered = IPCUtils.addWorkerTM(workerId, si, ackedWorkers);
    sendMessageToWorker(MyriaConstants.MASTER_ID, workerRecovered);
  }

  private void updateDriverStateOnWorkerReady(final int workerId) throws InjectionException {
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

  private void updateDriverStateOnWorkerFailure(final int workerId) {
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
      final int senderId = Integer.valueOf(taskMessage.getMessageSourceID());
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
      LOGGER.info("Received {} for worker {} from worker {}", controlM.getType(), workerId,
          senderId);
      if (controlM.getType() == ControlMessage.Type.REMOVE_WORKER_ACK) {
        if (senderId == MyriaConstants.MASTER_ID) {
          onRemoveAckFromCoordinator(workerId);
        } else {
          onRemoveAckFromWorker(workerId, senderId);
        }
      } else if (controlM.getType() == ControlMessage.Type.ADD_WORKER_ACK) {
        if (senderId == MyriaConstants.MASTER_ID) {
          onAddAckFromCoordinator(workerId);
        } else {
          onAddAckFromWorker(workerId, senderId);
        }
      } else {
        throw new IllegalStateException(
            "Expected control message to be ADD_WORKER_ACK or REMOVE_WORKER_ACK, got "
                + controlM.getType());
      }
    }
  }
}
