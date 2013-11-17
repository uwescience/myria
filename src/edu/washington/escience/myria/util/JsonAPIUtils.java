package edu.washington.escience.myria.util;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.commons.io.FileUtils;

/**
 * Util methods for handling of Json API stuff.
 * */
public final class JsonAPIUtils {

  /**
   * Util class allows no instance.
   * */
  private JsonAPIUtils() {
  }

  /**
   * @param masterHostname master hostname
   * @param apiPort rest api port
   * @param queryFile query file
   * @return a HTTPURLConnection instance of retrieving responses.
   * @throws IOException if IO errors
   * */
  public static HttpURLConnection submitQuery(final String masterHostname, final int apiPort, final File queryFile)
      throws IOException {
    return submitQuery(masterHostname, apiPort, FileUtils.readFileToString(queryFile));
  }

  /**
   * @param masterHostname master hostname
   * @param apiPort rest api port
   * @param queryString query string
   * @return a HTTPURLConnection instance of retrieving responses.
   * @throws IOException if IO errors
   * */
  public static HttpURLConnection submitQuery(final String masterHostname, final int apiPort, final String queryString)
      throws IOException {
    String type = "application/json";
    URL u = new URL("http://" + masterHostname + ":" + apiPort + "/query");
    HttpURLConnection conn = (HttpURLConnection) u.openConnection();
    conn.setDoOutput(true);
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Content-Type", type);
    byte[] e = queryString.getBytes();
    conn.setRequestProperty("Content-Length", String.valueOf(e.length));
    OutputStream os = conn.getOutputStream();
    os.write(e);
    os.close();
    conn.connect();
    conn.getResponseCode();
    return conn;
  }

  /**
   * @param masterHostname master hostname
   * @param apiPort rest api port
   * @param queryFile query file
   * @return a HTTPURLConnection instance of retrieving responses.
   * @throws IOException if IO errors
   * */
  public static HttpURLConnection ingestData(final String masterHostname, final int apiPort, final File queryFile)
      throws IOException {
    return ingestData(masterHostname, apiPort, FileUtils.readFileToString(queryFile));
  }

  /**
   * @param masterHostname master hostname
   * @param apiPort rest api port
   * @param queryString query string
   * @return a HTTPURLConnection instance of retrieving responses.
   * @throws IOException if IO errors
   * */
  public static HttpURLConnection ingestData(final String masterHostname, final int apiPort, final String queryString)
      throws IOException {

    String type = "application/json";
    URL u = new URL("http://" + masterHostname + ":" + apiPort + "/dataset");
    HttpURLConnection conn = (HttpURLConnection) u.openConnection();
    conn.setDoOutput(true);
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Content-Type", type);
    byte[] e = queryString.getBytes();
    conn.setRequestProperty("Content-Length", String.valueOf(e.length));
    OutputStream os = conn.getOutputStream();
    os.write(e);
    os.close();
    conn.connect();
    conn.getResponseCode();
    return conn;
  }

  /**
   * @param masterHostname master hostname
   * @param apiPort rest api port
   * @param queryFile query file
   * @return a HTTPURLConnection instance of retrieving responses.
   * @throws IOException if IO errors
   * */
  public static HttpURLConnection ingestTipsyData(final String masterHostname, final int apiPort, final File queryFile)
      throws IOException {
    String type = "application/json";
    URL u = new URL("http://" + masterHostname + ":" + apiPort + "/dataset/tipsy");
    HttpURLConnection conn = (HttpURLConnection) u.openConnection();
    conn.setDoOutput(true);
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Content-Type", type);
    byte[] e = FileUtils.readFileToString(queryFile).getBytes();
    conn.setRequestProperty("Content-Length", String.valueOf(e.length));
    OutputStream os = conn.getOutputStream();
    os.write(e);
    os.close();
    conn.connect();
    conn.getResponseCode();
    return conn;
  }

}
