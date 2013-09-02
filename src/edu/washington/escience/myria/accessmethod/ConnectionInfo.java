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
   * @return the JSON string representation of the connection information.
   */
  public static String toJson(final String dbms, final String hostName, final String description, final String dirName,
      final String workerId) {
    Objects.requireNonNull(dbms);
    ObjectMapper mapper = MyriaJsonMapperProvider.newMapper();
    String result = "";
    switch (dbms) {
      case MyriaConstants.STORAGE_SYSTEM_SQLITE:
        String databaseName = "";
        if (description != null) {
          /* created from deployment.cfg, use relative path */
          databaseName = FilenameUtils.concat(description, "worker_" + workerId);
          databaseName = FilenameUtils.concat(databaseName, "worker_" + workerId + "_data.db");
        } else {
          /* created from SystemTestBase, use absolute path */
          databaseName = FilenameUtils.concat(dirName, "worker_" + workerId);
          databaseName = FilenameUtils.concat(databaseName, "worker_" + workerId + "_data.db");
        }
        SQLiteInfo sqliteInfo = SQLiteInfo.of(databaseName);
        try {
          result = mapper.writeValueAsString(sqliteInfo);
        } catch (JsonProcessingException e) {
          throw new RuntimeException(e);
        }
        break;
      case MyriaConstants.STORAGE_SYSTEM_MONETDB:
        // TODO: Allow using the parameters to create the connection info.
        // Now it is hardcoded to use a specific connection info, which allows only one
        // myria instance per machine in the cluster
        final String host = hostName;
        final int port = 50000;
        final String user = "monetdb";
        final String password = "monetdb";
        final String myDatabaseName = "myria";
        final String jdbcDriverName = "nl.cwi.monetdb.jdbc.MonetDriver";
        final JdbcInfo jdbcInfo = JdbcInfo.of(jdbcDriverName, dbms, host, port, myDatabaseName, user, password);
        try {
          result = mapper.writeValueAsString(jdbcInfo);
        } catch (JsonProcessingException e) {
          throw new RuntimeException(e);
        }
        break;

      case MyriaConstants.STORAGE_SYSTEM_MYSQL:
        result = "";
        break;
    }
    return result;
  }

  /**
   * @return the DBMS, e.g., MyriaConstants.STORAGE_SYSTEM_MYSQL or MyriaConstants.STORAGE_SYSTEM_MONETDB.
   */
  public abstract String getDbms();
}
