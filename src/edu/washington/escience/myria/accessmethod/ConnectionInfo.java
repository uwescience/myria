/**
 * 
 */
package edu.washington.escience.myria.accessmethod;

import java.io.IOException;
import java.util.Objects;

import org.apache.commons.io.FilenameUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.api.MyriaJsonMapperProvider;

/**
 * @author valmeida
 * 
 */
public abstract class ConnectionInfo {

  /**
   * Creates a connection info.
   * 
   * @param dbms the DBMS
   * @param jsonConnInfo the connection info packed into a JSON string
   * @return the connection info
   */
  public static ConnectionInfo of(final String dbms, final String jsonConnInfo) {
    ObjectMapper mapper = MyriaJsonMapperProvider.newMapper();
    try {
      switch (dbms) {
        case MyriaConstants.STORAGE_SYSTEM_SQLITE:
          return mapper.readValue(jsonConnInfo, SQLiteInfo.class);
        case MyriaConstants.STORAGE_SYSTEM_MONETDB:
        case MyriaConstants.STORAGE_SYSTEM_MYSQL:
        case MyriaConstants.STORAGE_SYSTEM_POSTGRESQL:
          return mapper.readValue(jsonConnInfo, JdbcInfo.class);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return null;
  }

  /**
   * Returns a JSON string representation of the connection info.
   * 
   * @return the JSON representation
   */
  public String toJson() {
    ObjectMapper mapper = MyriaJsonMapperProvider.newMapper();
    try {
      return mapper.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Constructs a database connection information from the input and returns its JSON string representation.
   * 
   * @param dbms the database system.
   * @param hostName the host name.
   * @param description the description of the myria system.
   * @param dirName the working directory.
   * @param workerId the worker identification.
   * @param databaseName the database name for JDBC databases; ignored for non-JDBC
   * @param databasePassword the database password for JDBC databases; ignored for non-JDBC
   * @return the JSON string representation of the connection information.
   */
  public static String toJson(final String dbms, final String hostName, final String description, final String dirName,
      final String workerId, final String databaseName, final String databasePassword) {
    Objects.requireNonNull(dbms);
    String result = "";
    String host;
    int port;
    String user;
    String jdbcDriverName;
    JdbcInfo jdbcInfo;

    switch (dbms) {
      case MyriaConstants.STORAGE_SYSTEM_SQLITE:
        Objects.requireNonNull(workerId);
        String fileName = "";
        if (description != null) {
          /* created from deployment.cfg, use relative path */
          fileName = FilenameUtils.concat(Objects.requireNonNull(description), "worker_" + workerId);
          fileName = FilenameUtils.concat(fileName, "worker_" + workerId + "_data.db");
        } else {
          /* created from SystemTestBase, use absolute path */
          fileName = FilenameUtils.concat(Objects.requireNonNull(dirName), "worker_" + workerId);
          fileName = FilenameUtils.concat(fileName, "worker_" + workerId + "_data.db");
        }
        SQLiteInfo sqliteInfo = SQLiteInfo.of(fileName);
        result = sqliteInfo.toJson();
        break;
      case MyriaConstants.STORAGE_SYSTEM_MONETDB:
        // TODO: Allow using the parameters to create the connection info.
        // Now it is hardcoded to use a specific connection info, which allows only one
        // myria instance per machine in the cluster
        host = hostName;
        port = MyriaConstants.STORAGE_MONETDB_PORT;
        user = MyriaConstants.STORAGE_JDBC_USERNAME;
        jdbcDriverName = "nl.cwi.monetdb.jdbc.MonetDriver";
        jdbcInfo = JdbcInfo.of(jdbcDriverName, dbms, host, port, databaseName, user, databasePassword);
        result = jdbcInfo.toJson();
        break;

      case MyriaConstants.STORAGE_SYSTEM_POSTGRESQL:
        // TODO: Allow using the parameters to craete the connection info.
        // Now it is hardcoded to use a specific connection info, which allows only one
        // myria instance per machine in the cluster
        host = hostName;
        port = MyriaConstants.STORAGE_POSTGRESQL_PORT;
        user = MyriaConstants.STORAGE_JDBC_USERNAME;
        jdbcDriverName = "org.postgresql.Driver";
        jdbcInfo = JdbcInfo.of(jdbcDriverName, dbms, host, port, databaseName, user, databasePassword);
        result = jdbcInfo.toJson();
        break;

      case MyriaConstants.STORAGE_SYSTEM_MYSQL:
        // TODO: Allow using the parameters to craete the connection info.
        // Now it is hardcoded to use a specific connection info, which allows only one
        // myria instance per machine in the cluster
        host = hostName;
        port = MyriaConstants.STORAGE_MYSQL_PORT;
        user = MyriaConstants.STORAGE_JDBC_USERNAME;
        jdbcDriverName = "com.mysql.jdbc.Driver";
        jdbcInfo = JdbcInfo.of(jdbcDriverName, dbms, host, port, databaseName, user, databasePassword);
        result = jdbcInfo.toJson();
        break;
    }
    return result;
  }

  /**
   * @return the DBMS, e.g., MyriaConstants.STORAGE_SYSTEM_MYSQL or MyriaConstants.STORAGE_SYSTEM_MONETDB.
   */
  public abstract String getDbms();
}
