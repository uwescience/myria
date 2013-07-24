/**
 * 
 */
package edu.washington.escience.myriad.accessmethod;

import java.util.Objects;
import java.util.Properties;

import org.apache.commons.io.FilenameUtils;

import com.google.gson.Gson;

import edu.washington.escience.myriad.MyriaConstants;

/**
 * @author valmeida
 * 
 */
public abstract class ConnectionInfo {

  /**
   * Creates a connection info.
   * 
   * @param dbms the DBMS
   * @param jsonConnInfo the connection info packed into a json string
   * @return the connection info
   */
  public static ConnectionInfo of(final String dbms, final String jsonConnInfo) {
    Gson gson = new Gson();
    switch (dbms) {
      case MyriaConstants.STORAGE_SYSTEM_SQLITE:
        SQLiteInfo sqliteInfo = gson.fromJson(jsonConnInfo, SQLiteInfo.class);
        return sqliteInfo;
      case MyriaConstants.STORAGE_SYSTEM_MONETDB:
      case MyriaConstants.STORAGE_SYSTEM_MYSQL:
        JdbcInfo jdbcInfo = gson.fromJson(jsonConnInfo, JdbcInfo.class);
        return jdbcInfo;
    }
    return null;
  }

  /*
   * Returns a json string representation of the connection info.
   * 
   * @return the json representation
   */
  public String toJson() {
    Gson gson = new Gson();
    return gson.toJson(this);
  }

  public static String toJson(final String dbms, final String hostName, final String description, final String dirName,
      final String workerId) {
    Objects.requireNonNull(dbms);
    Gson gson = new Gson();
    String result = "";
    switch (dbms) {
      case MyriaConstants.STORAGE_SYSTEM_SQLITE:
        String databaseName = "";
        if (description != null) {
          databaseName = FilenameUtils.concat(description, "worker_" + workerId);
          databaseName = FilenameUtils.concat(databaseName, "worker_" + workerId + "_data.db");
        } else {
          databaseName = FilenameUtils.concat(dirName, "worker_" + workerId + "_data.db");
        }
        SQLiteInfo sqliteInfo = SQLiteInfo.of(databaseName);
        result = gson.toJson(sqliteInfo);
        break;
      case MyriaConstants.STORAGE_SYSTEM_MONETDB:
        // TODO: Allow using the parameters to craete the connection info.
        // Now it is hardcoded to use a specific connection info, which allows only one
        // myria instance per machine in the cluster
        final String host = hostName;
        final int port = 50000;
        final String user = "monetdb";
        final String password = "monetdb";
        final String myDatabaseName = "myria";
        final String jdbcDriverName = "nl.cwi.monetdb.jdbc.MonetDriver";
        final JdbcInfo jdbcInfo = JdbcInfo.of(jdbcDriverName, dbms, host, port, myDatabaseName, user, password);
        result = gson.toJson(jdbcInfo);
        break;

      case MyriaConstants.STORAGE_SYSTEM_MYSQL:
        result = "";
        break;
    }
    return result;
  }

  /**
   * @return the DBMS, e.g., "mysql" or "monetdb.
   */
  public abstract String getDbms();

  /**
   * @return the hostname/IP.
   */
  public abstract String getHost();

  /**
   * @return the port.
   */
  public abstract int getPort();

  /**
   * @return the database name
   */
  public abstract String getDatabase();

  /**
   * @return the username.
   */
  public abstract String getUsername();

  /**
   * @return the password.
   */
  public abstract String getPassword();

  /**
   * @return additional properties.
   */
  public abstract Properties getProperties();

  /**
   * @return the connection string.
   */
  public abstract String getConnectionString();

}
