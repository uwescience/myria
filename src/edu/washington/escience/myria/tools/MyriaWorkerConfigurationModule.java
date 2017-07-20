/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package edu.washington.escience.myria.tools;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.OptionalParameter;
import org.apache.reef.tang.formats.RequiredParameter;

public final class MyriaWorkerConfigurationModule extends ConfigurationModuleBuilder {
  public static final RequiredParameter<Integer> WORKER_ID = new RequiredParameter<>();
  public static final RequiredParameter<String> WORKER_HOST = new RequiredParameter<>();
  public static final RequiredParameter<Integer> WORKER_PORT = new RequiredParameter<>();
  public static final OptionalParameter<Integer> WORKER_JVM_PORT = new OptionalParameter<>();
  public static final RequiredParameter<String> WORKER_STORAGE_DB_NAME = new RequiredParameter<>();
  public static final OptionalParameter<String> WORKER_STORAGE_DB_PASSWORD =
      new OptionalParameter<>();
  public static final OptionalParameter<Integer> WORKER_STORAGE_DB_PORT = new OptionalParameter<>();
  public static final RequiredParameter<String> WORKER_FILESYSTEM_PATH = new RequiredParameter<>();

  public static final ConfigurationModule CONF =
      new MyriaWorkerConfigurationModule()
          .bindNamedParameter(WorkerId.class, WORKER_ID)
          .bindNamedParameter(WorkerHost.class, WORKER_HOST)
          .bindNamedParameter(WorkerPort.class, WORKER_PORT)
          .bindNamedParameter(WorkerJVMPort.class, WORKER_JVM_PORT)
          .bindNamedParameter(WorkerStorageDbName.class, WORKER_STORAGE_DB_NAME)
          .bindNamedParameter(WorkerStorageDbPassword.class, WORKER_STORAGE_DB_PASSWORD)
          .bindNamedParameter(WorkerStorageDbPort.class, WORKER_STORAGE_DB_PORT)
          .bindNamedParameter(WorkerFilesystemPath.class, WORKER_FILESYSTEM_PATH)
          .build();

  @NamedParameter
  public class WorkerId implements Name<Integer> {}

  @NamedParameter
  public class WorkerHost implements Name<String> {}

  @NamedParameter
  public class WorkerPort implements Name<Integer> {}

  @NamedParameter
  public class WorkerJVMPort implements Name<Integer> {}

  @NamedParameter
  public class WorkerStorageDbName implements Name<String> {}

  @NamedParameter
  public class WorkerStorageDbPassword implements Name<String> {}

  @NamedParameter
  public class WorkerStorageDbPort implements Name<Integer> {}

  @NamedParameter
  public class WorkerFilesystemPath implements Name<String> {}
}
