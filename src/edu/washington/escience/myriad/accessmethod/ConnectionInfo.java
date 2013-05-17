/**
 * 
 */
package edu.washington.escience.myriad.accessmethod;

import java.util.Properties;

/**
 * @author valmeida
 * 
 */
public interface ConnectionInfo {

  /**
   * @return the DBMS, e.g., "mysql" or "monetdb.
   */
  String getDbms();

  /**
   * @return the hostname/IP.
   */
  String getHost();

  /**
   * @return the port.
   */
  int getPort();

  /**
   * @return the database name
   */
  String getDatabase();

  /**
   * @return the username.
   */
  String getUsername();

  /**
   * @return the password.
   */
  String getPassword();

  /**
   * @return additional properties.
   */
  Properties getProperties();

  /**
   * @return the connection string.
   */
  String getConnectionString();

}
