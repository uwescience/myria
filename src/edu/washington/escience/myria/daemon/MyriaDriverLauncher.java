package edu.washington.escience.myria.daemon;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FilenameUtils;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.REEF;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
// import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.tang.formats.ConfigurationModule;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.coordinator.ConfigFileException;
import edu.washington.escience.myria.daemon.MyriaDriver.ActiveContextHandler;
import edu.washington.escience.myria.daemon.MyriaDriver.CompletedEvaluatorHandler;
import edu.washington.escience.myria.daemon.MyriaDriver.CompletedTaskHandler;
import edu.washington.escience.myria.daemon.MyriaDriver.ContextFailureHandler;
import edu.washington.escience.myria.daemon.MyriaDriver.EvaluatorAllocatedHandler;
import edu.washington.escience.myria.daemon.MyriaDriver.EvaluatorFailureHandler;
import edu.washington.escience.myria.daemon.MyriaDriver.RunningTaskHandler;
import edu.washington.escience.myria.daemon.MyriaDriver.StartHandler;
import edu.washington.escience.myria.daemon.MyriaDriver.StopHandler;
import edu.washington.escience.myria.daemon.MyriaDriver.TaskFailureHandler;
import edu.washington.escience.myria.daemon.MyriaDriver.TaskMessageHandler;
import edu.washington.escience.myria.tools.MyriaConfigurationParser;

public final class MyriaDriverLauncher {

  private static final String USAGE_STRING =
      "Usage: MyriaDriverLauncher -configPath <configPath> -javaLibPath <javaLibPath> -nativeLibPath <nativeLibPath>";

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
    launchDriver(runtimeConfiguration, args);
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

  /**
   * @return The Driver configuration.
   * @throws IOException
   */
  private final static Configuration getDriverConf(final String[] libPaths, final String[] filePaths)
      throws IOException {
    ConfigurationModule driverConf =
        DriverConfiguration.CONF.set(DriverConfiguration.DRIVER_IDENTIFIER, "MyriaDriver")
            .set(DriverConfiguration.ON_DRIVER_STARTED, StartHandler.class)
            .set(DriverConfiguration.ON_DRIVER_STOP, StopHandler.class)
            .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, EvaluatorAllocatedHandler.class)
            .set(DriverConfiguration.ON_EVALUATOR_COMPLETED, CompletedEvaluatorHandler.class)
            .set(DriverConfiguration.ON_EVALUATOR_FAILED, EvaluatorFailureHandler.class)
            .set(DriverConfiguration.ON_CONTEXT_ACTIVE, ActiveContextHandler.class)
            .set(DriverConfiguration.ON_CONTEXT_FAILED, ContextFailureHandler.class)
            .set(DriverConfiguration.ON_TASK_RUNNING, RunningTaskHandler.class)
            .set(DriverConfiguration.ON_TASK_COMPLETED, CompletedTaskHandler.class)
            .set(DriverConfiguration.ON_TASK_FAILED, TaskFailureHandler.class)
            .set(DriverConfiguration.ON_TASK_MESSAGE, TaskMessageHandler.class);

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
  public static void launchDriver(final Configuration runtimeConf, final String[] args)
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
    final REEF reef = tang.newInjector(runtimeConf).getInstance(REEF.class);
    reef.submit(driverConf);
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

}
