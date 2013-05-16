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
  public String getDbms();

  /**
   * @return the hostname/IP.
   */
  public String getHost();

  /**
   * @return the port.
   */
  public int getPort();

  /**
   * @return the database name
   */
  public String getDatabase();

  /**
   * @return the username.
   */
  public String getUsername();

  /**
   * @return the password.
   */
  public String getPassword();

  /**
   * @return additional properties.
   */
  public Properties getProperties();

  /**
   * @return the connection string.
   */
  public String getConnectionString();
	
}
