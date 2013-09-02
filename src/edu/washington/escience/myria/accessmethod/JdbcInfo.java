package edu.washington.escience.myria.accessmethod;

import java.io.Serializable;
import java.util.Objects;
import java.util.Properties;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Holds the info for a JDBC Connection.
 * 
 * @author dhalperi
 * 
 */
public final class JdbcInfo extends ConnectionInfo implements Serializable {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The classname of the JDBC driver. */
  private final String driverClass;
  /** The DBMS, e.g., "mysql". */
  private final String dbms;
  /** The hostname/IP. */
  private final String host;
  /** The port. */
  private final int port;
  /** The database to connect to. */
  private final String database;
  /** Any additional properties the connection may need. */
  private final Properties properties;

  /**
   * Creates a new JdbcInfo object.
   * 
   * @param driverClass the classname of the JDBC driver.
   * @param dbms the DBMS, e.g., "mysql".
   * @param host the hostname/IP.
   * @param port the port.
   * @param database the database to connect to
   * @param username the username.
   * @param password the password.
   * @return a new JdbcInfo containing this information.
   */
  @JsonCreator
  public static JdbcInfo of(@JsonProperty("driver_class") final String driverClass,
      @JsonProperty("dbms") final String dbms, @JsonProperty("host") final String host,
      @JsonProperty("port") final int port, @JsonProperty("database") final String database,
      @JsonProperty("username") final String username, @JsonProperty("password") final String password) {
    return new JdbcInfo(driverClass, dbms, host, port, database, username, password, null);
  }

  /**
   * Creates a new JdbcInfo object.
   * 
   * @param driverClass the classname of the JDBC driver.
   * @param dbms the DBMS, e.g., "mysql".
   * @param host the hostname/IP.
   * @param port the port.
   * @param database the database to connect to
   * @param username the username.
   * @param password the password.
   * @param properties extra properties for the JDBC connection. May be null.
   * @return a new JdbcInfo containing this information.
   */
  public static JdbcInfo of(final String driverClass, final String dbms, final String host, final int port,
      final String database, final String username, final String password, final Properties properties) {
    return new JdbcInfo(driverClass, dbms, host, port, database, username, password, properties);
  }

  /**
   * Creates a new JdbcInfo object.
   * 
   * @param driverClass the classname of the JDBC driver.
   * @param dbms the DBMS, e.g., "mysql".
   * @param host the hostname/IP.
   * @param port the port.
   * @param database the database to connect to
   * @param username the username.
   * @param password the password.
   * @param properties extra properties for the JDBC connection.
   */
  private JdbcInfo(final String driverClass, final String dbms, final String host, final int port,
      final String database, final String username, final String password, final Properties properties) {
    Objects.requireNonNull(driverClass);
    Objects.requireNonNull(dbms);
    Objects.requireNonNull(host);
    Objects.requireNonNull(port);
    Objects.requireNonNull(database);
    Objects.requireNonNull(username);
    Objects.requireNonNull(password);
    this.driverClass = driverClass;
    this.dbms = dbms;
    this.host = host;
    this.port = port;
    this.database = database;
    if (properties == null) {
      this.properties = new Properties();
    } else {
      this.properties = properties;
    }
    this.properties.setProperty("user", username);
    this.properties.setProperty("password", password);
  }

  /**
   * @return the classname of the JDBC driver.
   */
  public String getDriverClass() {
    return driverClass;
  }

  @Override
  public String getDbms() {
    return dbms;
  }

  /**
   * @return the hostname/IP.
   */
  public String getHost() {
    return host;
  }

  /**
   * @return the port.
   */
  public int getPort() {
    return port;
  }

  /**
   * @return the database to connect to.
   */
  public String getDatabase() {
    return database;
  }

  /**
   * @return the username.
   */
  public String getUsername() {
    return properties.getProperty("user");
  }

  /**
   * @return the password.
   */
  public String getPassword() {
    return properties.getProperty("password");
  }

  /**
   * @return the properties.
   */
  public Properties getProperties() {
    return properties;
  }

  /**
   * @return the JDBC connection string.
   */
  public String getConnectionString() {
    return "jdbc:" + dbms + "://" + host + ":" + port + "/" + database;
  }
}