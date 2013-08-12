package edu.washington.escience.myriad.accessmethod;

import java.io.Serializable;
import java.util.Objects;
import java.util.Properties;

/**
 * Holds the info for a SQLite Connection.
 * 
 * @author dhalperi
 * 
 */
public final class SQLiteInfo extends ConnectionInfo implements Serializable {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
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
   * Creates a new SQLiteInfo object.
   * 
   * @param database the database to connect to
   * @return a new SQLiteInfo containing this information.
   */
  public static SQLiteInfo of(final String database) {
    return new SQLiteInfo("sqlite", "", 0, database, "", "", null);
  }

  /**
   * Creates a new SQLiteInfo object.
   * 
   * @param dbms the DBMS, e.g., "mysql".
   * @param host the hostname/IP.
   * @param port the port.
   * @param database the database to connect to
   * @param username the username.
   * @param password the password.
   * @return a new SQLiteInfo containing this information.
   */
  public static SQLiteInfo of(final String dbms, final String host, final int port, final String database,
      final String username, final String password) {
    return new SQLiteInfo(dbms, host, port, database, username, password, null);
  }

  /**
   * Creates a new SQLiteInfo object.
   * 
   * @param dbms the DBMS, e.g., "mysql".
   * @param host the hostname/IP.
   * @param port the port.
   * @param database the database to connect to
   * @param username the username.
   * @param password the password.
   * @param properties extra properties for the SQLite connection. May be null.
   * @return a new SQLiteInfo containing this information.
   */
  public static SQLiteInfo of(final String dbms, final String host, final int port, final String database,
      final String username, final String password, final Properties properties) {
    return new SQLiteInfo(dbms, host, port, database, username, password, properties);
  }

  /**
   * Creates a new SQLiteInfo object.
   * 
   * @param dbms the DBMS, e.g., "mysql".
   * @param host the hostname/IP.
   * @param port the port.
   * @param database the database to connect to
   * @param username the username.
   * @param password the password.
   * @param properties extra properties for the SQLite connection.
   */
  private SQLiteInfo(final String dbms, final String host, final int port, final String database,
      final String username, final String password, final Properties properties) {
    Objects.requireNonNull(dbms);
    Objects.requireNonNull(host);
    Objects.requireNonNull(port);
    Objects.requireNonNull(database);
    Objects.requireNonNull(username);
    Objects.requireNonNull(password);
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
   * @return the DBMS, e.g., "mysql".
   */
  @Override
  public String getDbms() {
    return dbms;
  }

  /**
   * @return the hostname/IP.
   */
  @Override
  public String getHost() {
    return host;
  }

  /**
   * @return the port.
   */
  @Override
  public int getPort() {
    return port;
  }

  /**
   * @return the database to connect to.
   */
  @Override
  public String getDatabase() {
    return database;
  }

  /**
   * @return the username.
   */
  @Override
  public String getUsername() {
    return properties.getProperty("user");
  }

  /**
   * @return the password.
   */
  @Override
  public String getPassword() {
    return properties.getProperty("password");
  }

  /**
   * @return the properties.
   */
  @Override
  public Properties getProperties() {
    return properties;
  }

  /**
   * @return the JDBC connection string.
   */
  @Override
  public String getConnectionString() {
    return "sqlite:" + dbms + "://" + host + ":" + port + "/" + database;
  }

  /**
   * @return a JSON string format.
   */
  @Override
  public String toString() {
    return "{dbms:" + dbms + ",database:" + database + "}";
  }
}