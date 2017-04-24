/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package edu.washington.escience.myria.tools;

import java.util.Set;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.OptionalParameter;
import org.apache.reef.tang.formats.RequiredParameter;

import edu.washington.escience.myria.MyriaConstants;

public final class MyriaGlobalConfigurationModule extends ConfigurationModuleBuilder {
  public static final RequiredParameter<String> INSTANCE_NAME = new RequiredParameter<>();
  public static final RequiredParameter<String> DEFAULT_INSTANCE_PATH = new RequiredParameter<>();
  public static final OptionalParameter<String> STORAGE_DBMS = new OptionalParameter<>();
  public static final OptionalParameter<String> DEFAULT_STORAGE_DB_NAME = new OptionalParameter<>();
  public static final OptionalParameter<String> DEFAULT_STORAGE_DB_USERNAME =
      new OptionalParameter<>();
  public static final OptionalParameter<String> DEFAULT_STORAGE_DB_PASSWORD =
      new OptionalParameter<>();
  public static final OptionalParameter<Integer> DEFAULT_STORAGE_DB_PORT =
      new OptionalParameter<>();
  public static final OptionalParameter<Integer> REST_API_PORT = new OptionalParameter<>();
  public static final OptionalParameter<String> API_ADMIN_PASSWORD = new OptionalParameter<>();
  public static final OptionalParameter<Boolean> USE_SSL = new OptionalParameter<>();
  public static final OptionalParameter<String> SSL_KEYSTORE_PATH = new OptionalParameter<>();
  public static final OptionalParameter<String> SSL_KEYSTORE_PASSWORD = new OptionalParameter<>();
  public static final OptionalParameter<Boolean> ENABLE_DEBUG = new OptionalParameter<>();
  public static final RequiredParameter<String> WORKER_CONF = new RequiredParameter<>();
  public static final OptionalParameter<Integer> MASTER_NUMBER_VCORES = new OptionalParameter<>();
  public static final OptionalParameter<Integer> WORKER_NUMBER_VCORES = new OptionalParameter<>();
  public static final OptionalParameter<Float> DRIVER_MEMORY_QUOTA_GB = new OptionalParameter<>();
  public static final OptionalParameter<Float> MASTER_MEMORY_QUOTA_GB = new OptionalParameter<>();
  public static final OptionalParameter<Float> WORKER_MEMORY_QUOTA_GB = new OptionalParameter<>();
  public static final OptionalParameter<Float> MASTER_JVM_HEAP_SIZE_MIN_GB =
      new OptionalParameter<>();
  public static final OptionalParameter<Float> WORKER_JVM_HEAP_SIZE_MIN_GB =
      new OptionalParameter<>();
  public static final OptionalParameter<Float> MASTER_JVM_HEAP_SIZE_MAX_GB =
      new OptionalParameter<>();
  public static final OptionalParameter<Float> WORKER_JVM_HEAP_SIZE_MAX_GB =
      new OptionalParameter<>();
  public static final OptionalParameter<String> JVM_OPTIONS = new OptionalParameter<>();
  public static final OptionalParameter<Integer> FLOW_CONTROL_WRITE_BUFFER_HIGH_MARK_BYTES =
      new OptionalParameter<>();
  public static final OptionalParameter<Integer> FLOW_CONTROL_WRITE_BUFFER_LOW_MARK_BYTES =
      new OptionalParameter<>();
  public static final OptionalParameter<Integer> OPERATOR_INPUT_BUFFER_CAPACITY =
      new OptionalParameter<>();
  public static final OptionalParameter<Integer> OPERATOR_INPUT_BUFFER_RECOVER_TRIGGER =
      new OptionalParameter<>();
  public static final OptionalParameter<Integer> TCP_CONNECTION_TIMEOUT_MILLIS =
      new OptionalParameter<>();
  public static final OptionalParameter<Integer> TCP_RECEIVE_BUFFER_SIZE_BYTES =
      new OptionalParameter<>();
  public static final OptionalParameter<Integer> TCP_SEND_BUFFER_SIZE_BYTES =
      new OptionalParameter<>();
  public static final OptionalParameter<Integer> LOCAL_FRAGMENT_WORKER_THREADS =
      new OptionalParameter<>();
  public static final RequiredParameter<String> MASTER_HOST = new RequiredParameter<>();
  public static final RequiredParameter<Integer> MASTER_RPC_PORT = new RequiredParameter<>();
  public static final RequiredParameter<String> PERSIST_URI = new RequiredParameter<>();
  public static final OptionalParameter<Boolean> ENABLE_ELASTIC_MODE = new OptionalParameter<>();

  public static final ConfigurationModule CONF =
      new MyriaGlobalConfigurationModule()
          .bindNamedParameter(InstanceName.class, INSTANCE_NAME)
          .bindNamedParameter(DefaultInstancePath.class, DEFAULT_INSTANCE_PATH)
          .bindNamedParameter(StorageDbms.class, STORAGE_DBMS)
          .bindNamedParameter(DefaultStorageDbName.class, DEFAULT_STORAGE_DB_NAME)
          .bindNamedParameter(DefaultStorageDbUser.class, DEFAULT_STORAGE_DB_USERNAME)
          .bindNamedParameter(DefaultStorageDbPassword.class, DEFAULT_STORAGE_DB_PASSWORD)
          .bindNamedParameter(DefaultStorageDbPort.class, DEFAULT_STORAGE_DB_PORT)
          .bindNamedParameter(RestApiPort.class, REST_API_PORT)
          .bindNamedParameter(ApiAdminPassword.class, API_ADMIN_PASSWORD)
          .bindNamedParameter(UseSsl.class, USE_SSL)
          .bindNamedParameter(SslKeystorePath.class, SSL_KEYSTORE_PATH)
          .bindNamedParameter(SslKeystorePassword.class, SSL_KEYSTORE_PASSWORD)
          .bindNamedParameter(EnableDebug.class, ENABLE_DEBUG)
          .bindSetEntry(WorkerConf.class, WORKER_CONF)
          .bindNamedParameter(MasterNumberVCores.class, MASTER_NUMBER_VCORES)
          .bindNamedParameter(WorkerNumberVCores.class, WORKER_NUMBER_VCORES)
          .bindNamedParameter(DriverMemoryQuotaGB.class, DRIVER_MEMORY_QUOTA_GB)
          .bindNamedParameter(MasterMemoryQuotaGB.class, MASTER_MEMORY_QUOTA_GB)
          .bindNamedParameter(WorkerMemoryQuotaGB.class, WORKER_MEMORY_QUOTA_GB)
          .bindNamedParameter(MasterJvmHeapSizeMinGB.class, MASTER_JVM_HEAP_SIZE_MIN_GB)
          .bindNamedParameter(WorkerJvmHeapSizeMinGB.class, WORKER_JVM_HEAP_SIZE_MIN_GB)
          .bindNamedParameter(MasterJvmHeapSizeMaxGB.class, MASTER_JVM_HEAP_SIZE_MAX_GB)
          .bindNamedParameter(WorkerJvmHeapSizeMaxGB.class, WORKER_JVM_HEAP_SIZE_MAX_GB)
          .bindSetEntry(JvmOptions.class, JVM_OPTIONS)
          .bindNamedParameter(
              FlowControlWriteBufferHighMarkBytes.class, FLOW_CONTROL_WRITE_BUFFER_HIGH_MARK_BYTES)
          .bindNamedParameter(
              FlowControlWriteBufferLowMarkBytes.class, FLOW_CONTROL_WRITE_BUFFER_LOW_MARK_BYTES)
          .bindNamedParameter(OperatorInputBufferCapacity.class, OPERATOR_INPUT_BUFFER_CAPACITY)
          .bindNamedParameter(
              OperatorInputBufferRecoverTrigger.class, OPERATOR_INPUT_BUFFER_RECOVER_TRIGGER)
          .bindNamedParameter(TcpConnectionTimeoutMillis.class, TCP_CONNECTION_TIMEOUT_MILLIS)
          .bindNamedParameter(TcpReceiveBufferSizeBytes.class, TCP_RECEIVE_BUFFER_SIZE_BYTES)
          .bindNamedParameter(TcpSendBufferSizeBytes.class, TCP_SEND_BUFFER_SIZE_BYTES)
          .bindNamedParameter(LocalFragmentWorkerThreads.class, LOCAL_FRAGMENT_WORKER_THREADS)
          .bindNamedParameter(MasterHost.class, MASTER_HOST)
          .bindNamedParameter(MasterRpcPort.class, MASTER_RPC_PORT)
          .bindNamedParameter(PersistUri.class, PERSIST_URI)
          .bindNamedParameter(EnableElasticMode.class, ENABLE_ELASTIC_MODE)
          .build();

  @NamedParameter
  public class InstanceName implements Name<String> {}

  @NamedParameter
  public class DefaultInstancePath implements Name<String> {}

  @NamedParameter(default_value = MyriaConstants.STORAGE_SYSTEM_POSTGRESQL)
  public class StorageDbms implements Name<String> {}

  @NamedParameter(default_value = MyriaConstants.STORAGE_DATABASE_NAME)
  public class DefaultStorageDbName implements Name<String> {}

  @NamedParameter(default_value = MyriaConstants.STORAGE_JDBC_USERNAME)
  public class DefaultStorageDbUser implements Name<String> {}

  @NamedParameter(default_value = "password")
  public class DefaultStorageDbPassword implements Name<String> {}

  @NamedParameter(default_value = MyriaConstants.STORAGE_POSTGRESQL_PORT + "")
  public class DefaultStorageDbPort implements Name<Integer> {}

  @NamedParameter(default_value = MyriaConstants.DEFAULT_MYRIA_API_PORT + "")
  public class RestApiPort implements Name<Integer> {}

  @NamedParameter(default_value = "admin")
  public class ApiAdminPassword implements Name<String> {}

  @NamedParameter(default_value = "false")
  public class UseSsl implements Name<Boolean> {}

  @NamedParameter(default_value = "keystore")
  public class SslKeystorePath implements Name<String> {}

  @NamedParameter(default_value = "password")
  public class SslKeystorePassword implements Name<String> {}

  @NamedParameter(default_value = "false")
  public class EnableDebug implements Name<Boolean> {}

  @NamedParameter
  public class WorkerConf implements Name<Set<String>> {}

  @NamedParameter(default_value = "1")
  public class MasterNumberVCores implements Name<Integer> {}

  @NamedParameter(default_value = "2")
  public class WorkerNumberVCores implements Name<Integer> {}

  @NamedParameter(default_value = "0.5")
  public class DriverMemoryQuotaGB implements Name<Float> {}

  @NamedParameter(default_value = "1.0")
  public class MasterMemoryQuotaGB implements Name<Float> {}

  @NamedParameter(default_value = "2.0")
  public class WorkerMemoryQuotaGB implements Name<Float> {}

  @NamedParameter(default_value = "0.9")
  public class MasterJvmHeapSizeMinGB implements Name<Float> {}

  @NamedParameter(default_value = "0.9")
  public class WorkerJvmHeapSizeMinGB implements Name<Float> {}

  @NamedParameter(default_value = "0.9")
  public class MasterJvmHeapSizeMaxGB implements Name<Float> {}

  @NamedParameter(default_value = "1.8")
  public class WorkerJvmHeapSizeMaxGB implements Name<Float> {}

  @NamedParameter(default_values = {"-XX:+UseG1GC"})
  public class JvmOptions implements Name<Set<String>> {}

  @NamedParameter(default_value = (5 * MyriaConstants.MB) + "")
  public class FlowControlWriteBufferHighMarkBytes implements Name<Integer> {}

  @NamedParameter(default_value = (512 * MyriaConstants.KB) + "")
  public class FlowControlWriteBufferLowMarkBytes implements Name<Integer> {}

  @NamedParameter(default_value = "100")
  public class OperatorInputBufferCapacity implements Name<Integer> {}

  @NamedParameter(default_value = "80")
  public class OperatorInputBufferRecoverTrigger implements Name<Integer> {}

  @NamedParameter(default_value = "3000")
  public class TcpConnectionTimeoutMillis implements Name<Integer> {}

  @NamedParameter(default_value = (2 * MyriaConstants.MB) + "")
  public class TcpReceiveBufferSizeBytes implements Name<Integer> {}

  @NamedParameter(default_value = (5 * MyriaConstants.MB) + "")
  public class TcpSendBufferSizeBytes implements Name<Integer> {}

  @NamedParameter(default_value = "4")
  public class LocalFragmentWorkerThreads implements Name<Integer> {}

  @NamedParameter
  public class MasterHost implements Name<String> {}

  @NamedParameter
  public class MasterRpcPort implements Name<Integer> {}

  @NamedParameter
  public class PersistUri implements Name<String> {}

  @NamedParameter(default_value = "false")
  public class EnableElasticMode implements Name<Boolean> {}
}
