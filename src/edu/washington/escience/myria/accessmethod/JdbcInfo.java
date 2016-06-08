package edu.washington.escience.myria.accessmethod;

import java.io.Serializable;
import java.util.Objects;
import java.util.Properties;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Holds the info for a JDBC Connection.
 *
 *
 */
public final class JdbcInfo extends ConnectionInfo implements Serializable {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The key under which username is stored in the Properties file. */
  public static final String JDBC_PROP_USERNAME_KEY = "user";
  /** The key under which password is stored in the Properties file. */
  public static final String JDBC_PROP_PASSWORD_KEY = "password";
  /** The classname of the JDBC driver. */
  @JsonProperty private final String driverClass;
  /** The DBMS, e.g., "mysql". */
  @JsonProperty private final String dbms;
  /** The hostname/IP. */
  @JsonProperty private final String host;
  /** The port. */
  @JsonProperty private final int port;
  /** The database to connect to. */
  @JsonProperty private final String database;
  /** Any additional properties the connection may need. */
  @JsonProperty private final Properties properties;

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
  public static JdbcInfo of(
      final String driverClass,
      final String dbms,
      final String host,
      final int port,
      final String database,
      final String username,
      final String password) {
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
  @JsonCreator
  public static JdbcInfo of(
      @JsonProperty("driverClass") final String driverClass,
      @JsonProperty("dbms") final String dbms,
      @JsonProperty("host") final String host,
      @JsonProperty("port") final int port,
      @JsonProperty("database") final String database,
      @JsonProperty("username") final String username,
      @JsonProperty("password") final String password,
      @JsonProperty("properties") final Properties properties) {
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
  private JdbcInfo(
      final String driverClass,
      final String dbms,
      final String host,
      final int port,
      final String database,
      final String username,
      final String password,
      final Properties properties) {
    this.driverClass = Objects.requireNonNull(driverClass);
    this.dbms = Objects.requireNonNull(dbms);
    this.host = Objects.requireNonNull(host);
    this.port = Objects.requireNonNull(port);
    this.database = Objects.requireNonNull(database);
    if (properties == null) {
      this.properties = new Properties();
    } else {
      this.properties = properties;
    }
    if (username != null) {
      this.properties.setProperty(JDBC_PROP_USERNAME_KEY, username);
    }
    if (password != null) {
      this.properties.setProperty(JDBC_PROP_PASSWORD_KEY, password);
    }
    Objects.requireNonNull(this.properties.getProperty(JDBC_PROP_USERNAME_KEY));
    Objects.requireNonNull(this.properties.getProperty(JDBC_PROP_PASSWORD_KEY));
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
    return properties.getProperty(JDBC_PROP_USERNAME_KEY);
  }

  /**
   * @return the password.
   */
  public String getPassword() {
    return properties.getProperty(JDBC_PROP_PASSWORD_KEY);
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
  @JsonIgnore
  public String getConnectionString() {
    return "jdbc:" + dbms + "://" + host + ":" + port + "/" + database;
  }
}
