package edu.washington.escience.myria.util;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
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

  private static String getDatasetUrlString(final String host, int port, final String user, final String program,
      final String relation, final String format) {
    return String.format("http://%s:%d/dataset/user-%s/program-%s/relation-%s/data?format=%s", host, port, user,
        program, relation, format);
  }

  /**
   * Download a dataset
   * 
   * @param host master hostname
   * @param port master port
   * @param user user parameter of the dataset
   * @param program program parameter of the dataset
   * @param relation relation parameter of the dataset
   * @param format the format of the relation ("json", "csv", "tsv")
   * 
   * @return dataset encoded in the requested format
   * @throws IOException if an error occurs
   */
  public static String download(final String host, final int port, final String user, final String program,
      final String relation, final String format) throws IOException {
    HttpClient client = new HttpClient();

    String s = getDatasetUrlString(host, port, user, program, relation, format);
    GetMethod method = new GetMethod(s);

    try {
      // Execute the method.
      int statusCode = client.executeMethod(method);

      if (statusCode != HttpStatus.SC_OK) {
        throw new IOException("Failed download: " + statusCode);
      }

      byte[] responseBody = method.getResponseBody();
      return new String(responseBody, "UTF-8");
    } finally {
      method.releaseConnection();
    }
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
}
