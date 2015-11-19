package edu.washington.escience.myria.daemon;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FilenameUtils;
import org.apache.reef.client.ClientConfiguration;
import org.apache.reef.client.CompletedJob;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.FailedJob;
import org.apache.reef.client.FailedRuntime;
import org.apache.reef.client.JobMessage;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.client.REEF;
import org.apache.reef.client.RunningJob;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
// import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.coordinator.ConfigFileException;
import edu.washington.escience.myria.tools.MyriaConfigurationParser;

@Unit
public final class MyriaDriverLauncher {

  private static final Logger LOGGER = LoggerFactory.getLogger(MyriaDriverLauncher.class);

  private static final String USAGE_STRING =
      "Usage: MyriaDriverLauncher -configPath <configPath> -javaLibPath <javaLibPath> -nativeLibPath <nativeLibPath>";

  private static final long DRIVER_PING_TIMEOUT_MILLIS = 60 * 1000;

  /**
   * @param args full path to the configuration directory.
   * @throws Exception if the Driver can't start.
   */
  public static void main(final String[] args) throws Exception {
    if (args.length != 6) {
      System.err.println(USAGE_STRING);
      System.exit(-1);
    }
    // final Configuration runtimeConfiguration = YarnClientConfiguration.CONF.build();
    final Configuration runtimeConfiguration = LocalRuntimeConfiguration.CONF.build();
    run(runtimeConfiguration, args);
  }

  private final REEF reef;
  @GuardedBy("this")
  private Optional<RunningJob> driver = Optional.empty();
  @GuardedBy("this")
  private LauncherStatus status = LauncherStatus.INIT;

  @Inject
  private MyriaDriverLauncher(final REEF reef) {
    this.reef = reef;
  }

  private final static Configuration getClientConf() {
    return ClientConfiguration.CONF
        .set(ClientConfiguration.ON_JOB_RUNNING, RunningJobHandler.class)
        .set(ClientConfiguration.ON_JOB_MESSAGE, JobMessageHandler.class)
        .set(ClientConfiguration.ON_JOB_COMPLETED, CompletedJobHandler.class)
        .set(ClientConfiguration.ON_JOB_FAILED, FailedJobHandler.class)
        .set(ClientConfiguration.ON_RUNTIME_ERROR, RuntimeErrorHandler.class).build();
  }

  /**
   * @return The Driver configuration.
   * @throws IOException
   */
  private final static Configuration getDriverConf(final String[] libPaths, final String[] filePaths)
      throws IOException {
    ConfigurationModule driverConf =
        DriverConfiguration.CONF
            .set(DriverConfiguration.DRIVER_IDENTIFIER, "MyriaDriver")
            .set(DriverConfiguration.ON_DRIVER_STARTED, MyriaDriver.StartHandler.class)
            .set(DriverConfiguration.ON_DRIVER_STOP, MyriaDriver.StopHandler.class)
            .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED,
                MyriaDriver.EvaluatorAllocatedHandler.class)
            .set(DriverConfiguration.ON_EVALUATOR_COMPLETED,
                MyriaDriver.CompletedEvaluatorHandler.class)
            .set(DriverConfiguration.ON_EVALUATOR_FAILED, MyriaDriver.EvaluatorFailureHandler.class)
            .set(DriverConfiguration.ON_CONTEXT_ACTIVE, MyriaDriver.ActiveContextHandler.class)
            .set(DriverConfiguration.ON_CONTEXT_FAILED, MyriaDriver.ContextFailureHandler.class)
            .set(DriverConfiguration.ON_TASK_RUNNING, MyriaDriver.RunningTaskHandler.class)
            .set(DriverConfiguration.ON_TASK_COMPLETED, MyriaDriver.CompletedTaskHandler.class)
            .set(DriverConfiguration.ON_TASK_FAILED, MyriaDriver.TaskFailureHandler.class)
            .set(DriverConfiguration.ON_TASK_MESSAGE, MyriaDriver.TaskMessageHandler.class)
            .set(DriverConfiguration.ON_CLIENT_MESSAGE, MyriaDriver.ClientMessageHandler.class);

    for (String dirPath : libPaths) {
      for (String filePath : getFileNamesInDirectory(Paths.get(dirPath))) {
        driverConf = driverConf.set(DriverConfiguration.GLOBAL_LIBRARIES, filePath);
      }
    }
    for (String dirPath : filePaths) {
      for (String filePath : getFileNamesInDirectory(Paths.get(dirPath))) {
        driverConf = driverConf.set(DriverConfiguration.GLOBAL_FILES, filePath);
      }
    }
    return driverConf.build();
  }

  /**
   * @param configPath path to directory of containing configuration files
   * @return Configuration object.
   */
  private static Configuration getMyriaGlobalConf(final String configPath) throws IOException,
      BindException, ConfigFileException {
    final String configFile = FilenameUtils.concat(configPath, MyriaConstants.DEPLOYMENT_CONF_FILE);
    return MyriaConfigurationParser.loadConfiguration(configFile);
  }

  private static List<String> getFileNamesInDirectory(final Path root) throws IOException {
    final List<String> fileNames = new ArrayList<>();
    getFileNamesInDirectoryHelper(root, fileNames);
    return fileNames;
  }

  private static void getFileNamesInDirectoryHelper(final Path dir, final List<String> acc)
      throws IOException {
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
      for (Path path : stream) {
        if (path.toFile().isDirectory()) {
          getFileNamesInDirectoryHelper(path, acc);
        } else {
          acc.add(path.toAbsolutePath().toString());
        }
      }
    }
  }

  /**
   * Launch the Myria driver.
   * 
   * @param runtimeConf The runtime configuration (e.g. Local, YARN, etc)
   * @param args Command line arguments.
   * @throws InjectionException
   * @throws java.io.IOException
   * @throws ParseException
   * @throws ConfigFileException
   */
  public static LauncherStatus run(final Configuration runtimeConf, final String[] args)
      throws InjectionException, IOException, ParseException, ConfigFileException {
    final Tang tang = Tang.Factory.getTang();
    // TODO: implement ClientConfiguration event handlers so we can get messages from the Driver
    @SuppressWarnings("unchecked")
    final Configuration commandLineConf =
        CommandLine.parseToConfiguration(args, ConfigPath.class, JavaLibPath.class,
            NativeLibPath.class);
    final Injector commandLineInjector = tang.newInjector(commandLineConf);
    final String configPath = commandLineInjector.getNamedInstance(ConfigPath.class);
    final String javaLibPath = commandLineInjector.getNamedInstance(JavaLibPath.class);
    final String nativeLibPath = commandLineInjector.getNamedInstance(NativeLibPath.class);
    final String serializedGlobalConf =
        new AvroConfigurationSerializer().toString(getMyriaGlobalConf(configPath));
    final Configuration globalConfWrapper =
        tang.newConfigurationBuilder()
            .bindNamedParameter(SerializedGlobalConf.class, serializedGlobalConf).build();
    final Configuration driverConf =
        Configurations.merge(
            getDriverConf(new String[] {javaLibPath}, new String[] {nativeLibPath}),
            globalConfWrapper);

    return tang.newInjector(runtimeConf, getClientConf()).getInstance(MyriaDriverLauncher.class)
        .run(driverConf);
  }

  private LauncherStatus run(final Configuration driverConf) {
    LOGGER.info("Submitting Myria driver to REEF...");
    reef.submit(driverConf);

    synchronized (this) {
      while (!status.isDone()) {
        LOGGER.info("Waiting for Myria driver to finish...");
        try {
          this.wait(DRIVER_PING_TIMEOUT_MILLIS);
          if (driver.isPresent()) {
            final byte[] driverMsg = MyriaDriver.DRIVER_PING_MSG.getBytes(StandardCharsets.UTF_8);
            LOGGER.info("Sending message to Myria driver: {}", MyriaDriver.DRIVER_PING_MSG);
            driver.get().send(driverMsg);
          }
        } catch (final InterruptedException ex) {
          LOGGER.info("Interrupted while waiting for Myria driver to finish, exiting...");
          close(); // this sets status to FORCE_CLOSED
        }
      }
    }

    reef.close();
    return status;
  }

  public synchronized void close() {
    if (status.isRunning()) {
      status = LauncherStatus.FORCE_CLOSED;
    }
    if (driver.isPresent()) {
      driver.get().close();
    }
    notify();
  }

  /**
   * Command line parameter: directory containing configuration file on driver launch host.
   */
  @NamedParameter(doc = "local configuration file directory", short_name = "configPath")
  public static final class ConfigPath implements Name<String> {
  }

  /**
   * Command line parameter: directory containing JAR/class files on driver launch host.
   */
  @NamedParameter(doc = "local JAR/class file directory", short_name = "javaLibPath")
  public static final class JavaLibPath implements Name<String> {
  }

  /**
   * Command line parameter: directory containing native shared libraries on driver launch host.
   */
  @NamedParameter(doc = "local native shared library directory", short_name = "nativeLibPath")
  public static final class NativeLibPath implements Name<String> {
  }

  /**
   * Serialized Myria global configuration (which itself contains serialized configuration for each
   * worker).
   */
  @NamedParameter(doc = "serialized Myria global configuration")
  public static final class SerializedGlobalConf implements Name<String> {
  }

  final class JobMessageHandler implements EventHandler<JobMessage> {
    @Override
    public void onNext(final JobMessage message) {
      final byte[] msg = message.get();
      final String msgStr = new String(msg, StandardCharsets.UTF_8);
      LOGGER.info("Message from Myria driver: {}", msgStr);
    }
  }

  final class RunningJobHandler implements EventHandler<RunningJob> {
    @Override
    public void onNext(final RunningJob job) {
      LOGGER.info("Myria driver is running...");
      synchronized (MyriaDriverLauncher.this) {
        status = LauncherStatus.RUNNING;
        driver = Optional.of(job);
      }
    }
  }

  final class CompletedJobHandler implements EventHandler<CompletedJob> {
    @Override
    public void onNext(final CompletedJob job) {
      LOGGER.info("Myria driver exited");
      synchronized (MyriaDriverLauncher.this) {
        status = LauncherStatus.COMPLETED;
        MyriaDriverLauncher.this.notify();
      }
    }
  }

  final class FailedJobHandler implements EventHandler<FailedJob> {
    @Override
    public void onNext(final FailedJob job) {
      LOGGER.error("Myria driver failed: {}", job.getReason().orElse(null));
      synchronized (MyriaDriverLauncher.this) {
        status = LauncherStatus.failed(job.getReason());
        MyriaDriverLauncher.this.notify();
      }
    }
  }

  final class RuntimeErrorHandler implements EventHandler<FailedRuntime> {
    @Override
    public void onNext(final FailedRuntime error) {
      LOGGER.error("Myria driver runtime error {}: {}", error, error.getReason().orElse(null));
      synchronized (MyriaDriverLauncher.this) {
        status = LauncherStatus.failed(error.getReason());
        MyriaDriverLauncher.this.notify();
      }
    }
  }
}
