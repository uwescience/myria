package edu.washington.escience.myria.daemon;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Nullable;
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
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
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
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.coordinator.ConfigFileException;
import edu.washington.escience.myria.tools.MyriaConfigurationParser;
import edu.washington.escience.myria.tools.MyriaGlobalConfigurationModule;

@Unit
public final class MyriaDriverLauncher {

  private static final Logger LOGGER = LoggerFactory.getLogger(MyriaDriverLauncher.class);

  private static final long DRIVER_PING_TIMEOUT_MILLIS = 60 * 1000;

  /**
   * @param args full path to the configuration directory.
   * @throws Exception if the Driver can't start.
   */
  public static void main(final String[] args) throws Exception {
    LauncherStatus status = run(args);
    LOGGER.info("Exit with status " + status);
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

  private static Configuration getRuntimeConf(final String runtimeClassName)
      throws ClassNotFoundException, IllegalArgumentException, IllegalAccessException,
          NoSuchFieldException, SecurityException {
    final Class<?> runtimeClass = Class.forName(runtimeClassName);
    ConfigurationModule cm = (ConfigurationModule) runtimeClass.getField("CONF").get(null);
    // need to allow some room for non-heap memory in the Driver
    if (cm.equals(YarnClientConfiguration.CONF)) {
      cm.set(YarnClientConfiguration.JVM_HEAP_SLACK, "0.1");
    }
    return cm.build();
  }

  private static Configuration getClientConf() {
    return ClientConfiguration.CONF
        .set(ClientConfiguration.ON_JOB_RUNNING, RunningJobHandler.class)
        .set(ClientConfiguration.ON_JOB_MESSAGE, JobMessageHandler.class)
        .set(ClientConfiguration.ON_JOB_COMPLETED, CompletedJobHandler.class)
        .set(ClientConfiguration.ON_JOB_FAILED, FailedJobHandler.class)
        .set(ClientConfiguration.ON_RUNTIME_ERROR, RuntimeErrorHandler.class)
        .build();
  }

  /**
   * @return The Driver configuration.
   */
  private static Configuration getDriverConf(
      @Nullable final String driverJobSubmissionDirectory,
      final String driverHostName,
      final int driverMemoryMB,
      final String[] libPaths,
      final String[] filePaths)
      throws IOException {
    ConfigurationModule driverConf =
        DriverConfiguration.CONF
            .set(DriverConfiguration.DRIVER_IDENTIFIER, "MyriaDriver")
            .set(DriverConfiguration.DRIVER_NODE, driverHostName)
            .set(DriverConfiguration.DRIVER_MEMORY, driverMemoryMB)
            .set(DriverConfiguration.ON_DRIVER_STARTED, MyriaDriver.StartHandler.class)
            .set(DriverConfiguration.ON_DRIVER_STOP, MyriaDriver.StopHandler.class)
            .set(
                DriverConfiguration.ON_EVALUATOR_ALLOCATED,
                MyriaDriver.EvaluatorAllocatedHandler.class)
            .set(
                DriverConfiguration.ON_EVALUATOR_COMPLETED,
                MyriaDriver.CompletedEvaluatorHandler.class)
            .set(DriverConfiguration.ON_EVALUATOR_FAILED, MyriaDriver.EvaluatorFailureHandler.class)
            .set(DriverConfiguration.ON_CONTEXT_ACTIVE, MyriaDriver.ActiveContextHandler.class)
            .set(DriverConfiguration.ON_CONTEXT_FAILED, MyriaDriver.ContextFailureHandler.class)
            .set(DriverConfiguration.ON_TASK_RUNNING, MyriaDriver.RunningTaskHandler.class)
            .set(DriverConfiguration.ON_TASK_COMPLETED, MyriaDriver.CompletedTaskHandler.class)
            .set(DriverConfiguration.ON_TASK_FAILED, MyriaDriver.TaskFailureHandler.class)
            .set(DriverConfiguration.ON_TASK_MESSAGE, MyriaDriver.TaskMessageHandler.class)
            .set(DriverConfiguration.ON_CLIENT_MESSAGE, MyriaDriver.ClientMessageHandler.class)
            .set(DriverConfiguration.ON_CLIENT_CLOSED, MyriaDriver.ClientCloseHandler.class);

    if (driverJobSubmissionDirectory != null) {
      driverConf =
          driverConf.set(
              DriverConfiguration.DRIVER_JOB_SUBMISSION_DIRECTORY, driverJobSubmissionDirectory);
    }
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
  private static Configuration getMyriaGlobalConf(final String configPath)
      throws IOException, BindException, ConfigFileException {
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

  private static String getMasterHost(final Configuration conf) throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector(conf);
    LocalAddressProvider addressProvider = injector.getInstance(LocalAddressProvider.class);
    final String masterHost =
        injector.getNamedInstance(MyriaGlobalConfigurationModule.MasterHost.class);
    // REEF (org.apache.reef.wake.remote.address.HostnameBasedLocalAddressProvider) will
    // unpredictably pick a local DNS name or IP address instead of "localhost" or 127.0.0.1
    String reefMasterHost = masterHost;
    if (masterHost.equals("localhost") || masterHost.equals("127.0.0.1")) {
      try {
        reefMasterHost = InetAddress.getByName(addressProvider.getLocalAddress()).getHostName();
        LOGGER.info(
            "Original host: {}, HostnameBasedLocalAddressProvider returned {}",
            masterHost,
            reefMasterHost);
      } catch (final UnknownHostException e) {
        LOGGER.warn("Failed to get canonical hostname for host {}", masterHost);
      }
    }
    return reefMasterHost;
  }

  private static int getDriverMemory(final Configuration conf) throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector(conf);
    final float driverMemoryGB =
        injector.getNamedInstance(MyriaGlobalConfigurationModule.DriverMemoryQuotaGB.class);
    final int driverMemoryMB = (int) (driverMemoryGB * 1024);
    return driverMemoryMB;
  }

  /**
   * Prints the expected configuration, compared to the actual configuration given.
   * Used for logging command line arguments.
   */
  private static String genStartupMessage(Class<? extends Name<?>>[] classes, Injector inj) {
    String allparams =
        Arrays.stream(classes)
            .map(
                c -> {
                  final NamedParameter annotation = c.getAnnotation(NamedParameter.class);

                  final String fullName = c.getName();
                  final String simpleName = c.getSimpleName();
                  final String shortName = annotation.short_name();
                  final String doc = annotation.doc();
                  final String defaultVal;
                  {
                    if (!annotation
                        .default_value()
                        .equals(NamedParameter.REEF_UNINITIALIZED_VALUE)) {
                      defaultVal = annotation.default_value();
                    } else if (!annotation.default_class().equals(Void.class)) {
                      defaultVal = annotation.default_class().getSimpleName();
                    } else if (annotation.default_values().length > 0) {
                      defaultVal = Arrays.toString(annotation.default_values());
                    } else if (annotation.default_classes().length > 0) {
                      final String classNames =
                          Arrays.stream(annotation.default_classes())
                              .map(Class::getSimpleName)
                              .reduce("", (a, b) -> a + ", " + b);
                      defaultVal = String.format("[%s]", classNames);
                    } else {
                      defaultVal = "";
                    }
                  }

                  final StringBuilder sb = new StringBuilder(simpleName);
                  if (!shortName.isEmpty()) {
                    sb.append(" [-").append(shortName).append("]");
                  }
                  // If desired, the type of the parameter can be obtained from
                  // commandLineConf.getNamedParameters() --> .getSimpleArgName()
                  sb.append(": ").append(doc).append("\n -> ");

                  boolean ok;
                  try {
                    ok = inj.isParameterSet(fullName);
                    if (ok) {
                      sb.append(inj.getInstance(fullName).toString());
                    }
                  } catch (InjectionException | BindException e) {
                    ok = false;
                  }
                  if (!ok) {
                    if (!defaultVal.isEmpty())
                      sb.append("[cannot parse; using default] ").append(defaultVal);
                    else sb.append("[cannot parse; no default]");
                  }
                  return sb.toString();
                })
            .reduce("", (a, b) -> a + "\n" + b);

    return "MyriaDriverLauncher configuration:\n" + allparams;
  }

  /**
   * Launch the Myria driver.
   *
   * @param args Command line arguments.
   */
  @SuppressWarnings("unchecked")
  public static LauncherStatus run(final String[] args)
      throws InjectionException, IOException, ParseException, ConfigFileException,
          ClassNotFoundException, IllegalArgumentException, IllegalAccessException,
          NoSuchFieldException, SecurityException {
    final Tang tang = Tang.Factory.getTang();
    try {
      final Class<? extends Name<?>>[] commandLineClasses =
          new Class[] {
            RuntimeClassName.class,
            DriverJobSubmissionDirectory.class,
            ConfigPath.class,
            JavaLibPath.class,
            NativeLibPath.class
          };
      final Configuration commandLineConf =
          CommandLine.parseToConfiguration(args, commandLineClasses);
      final Injector commandLineInjector = tang.newInjector(commandLineConf);
      LOGGER.info(genStartupMessage(commandLineClasses, commandLineInjector));
      final String runtimeClassName = commandLineInjector.getNamedInstance(RuntimeClassName.class);
      final String driverJobSubmissionDirectory;
      if (commandLineInjector.isParameterSet(DriverJobSubmissionDirectory.class)) {
        driverJobSubmissionDirectory =
            commandLineInjector.getNamedInstance(DriverJobSubmissionDirectory.class);
      } else {
        driverJobSubmissionDirectory = null;
      }
      final String configPath = commandLineInjector.getNamedInstance(ConfigPath.class);
      final String javaLibPath = commandLineInjector.getNamedInstance(JavaLibPath.class);
      final String nativeLibPath = commandLineInjector.getNamedInstance(NativeLibPath.class);

      final Configuration globalConf = getMyriaGlobalConf(configPath);
      final String serializedGlobalConf = new AvroConfigurationSerializer().toString(globalConf);
      final Configuration globalConfWrapper =
          tang.newConfigurationBuilder()
              .bindNamedParameter(SerializedGlobalConf.class, serializedGlobalConf)
              .build();
      final String driverHostName = getMasterHost(globalConf);
      final int driverMemoryMB = getDriverMemory(globalConf);
      final Configuration driverConf =
          Configurations.merge(
              getDriverConf(
                  driverJobSubmissionDirectory,
                  driverHostName,
                  driverMemoryMB,
                  new String[] {javaLibPath},
                  new String[] {nativeLibPath}),
              globalConfWrapper);

      return tang.newInjector(getRuntimeConf(runtimeClassName), getClientConf())
          .getInstance(MyriaDriverLauncher.class)
          .run(driverConf);
    } catch (ParseException | InjectionException e) {
      LOGGER.error("Problem with command line options (see previous log message)", e);
      return LauncherStatus.FAILED;
    }
  }

  private LauncherStatus run(final Configuration driverConf) {
    // Most UNIX signals will not throw an exception, so need to be trapped here.
    Runtime.getRuntime().addShutdownHook(new Thread(this::close));
    try {
      LOGGER.info("Submitting Myria driver to REEF...");
      reef.submit(driverConf);

      synchronized (this) {
        while (!status.isDone()) {
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

      return status;
    } finally {
      reef.close();
    }
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
   * Command line parameter: runtime configuration class to use (defaults to local runtime).
   */
  @NamedParameter(
    doc = "Fully qualified name of runtime configuration class",
    short_name = "runtimeClass",
    default_value = "org.apache.reef.runtime.local.client.LocalRuntimeConfiguration"
  )
  public static final class RuntimeClassName implements Name<String> {}

  /**
   * Command line parameter: path of driver job submission directory;
   * must be visible to the driver launcher and the driver
   */
  @NamedParameter(
    doc =
        "Full path of driver job submission directory; must be visible to the driver launcher and the driver",
    short_name = "driverDir"
  )
  public static final class DriverJobSubmissionDirectory implements Name<String> {}

  /**
   * Command line parameter: directory containing configuration file on driver launch host.
   */
  @NamedParameter(doc = "local configuration file directory", short_name = "configPath")
  public static final class ConfigPath implements Name<String> {}

  /**
   * Command line parameter: directory containing JAR/class files on driver launch host.
   */
  @NamedParameter(doc = "local JAR/class file directory", short_name = "javaLibPath")
  public static final class JavaLibPath implements Name<String> {}

  /**
   * Command line parameter: directory containing native shared libraries on driver launch host.
   */
  @NamedParameter(doc = "local native shared library directory", short_name = "nativeLibPath")
  public static final class NativeLibPath implements Name<String> {}

  /**
   * Serialized Myria global configuration (which itself contains serialized configuration for each worker).
   */
  @NamedParameter(doc = "serialized Myria global configuration")
  public static final class SerializedGlobalConf implements Name<String> {}

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
