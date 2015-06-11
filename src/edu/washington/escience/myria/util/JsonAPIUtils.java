package edu.washington.escience.myria.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

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
   * Construct a URL for get/put of a dataset.
   * 
   * @param host master hostname
   * @param port master port
   * @param user user parameter of the dataset
   * @param program program parameter of the dataset
   * @param relation relation parameter of the dataset
   * @param format the format of the relation ("json", "csv", "tsv")
   * @return a URL for the dataset
   * @throws IOException if an error occurs
   */
  private static URL getDatasetUrl(final String host, final int port, final String user, final String program,
      final String relation, final String format) throws IOException {
    return new URL(String.format("http://%s:%d/dataset/user-%s/program-%s/relation-%s/data?format=%s", host, port,
        user, program, relation, format));
  }

  /**
   * Download a dataset.
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
    URL url = getDatasetUrl(host, port, user, program, relation, format);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setDoOutput(true);
    conn.setRequestMethod("GET");

    if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new IOException("Failed to download result:" + conn.getResponseCode());
    }

    try {
      InputStream is = conn.getInputStream();
      return IOUtils.toString(is, "UTF-8");
    } finally {
      conn.disconnect();
    }
  }

  /**
   * Replace the contents of a dataset.
   * 
   * @param host master hostname
   * @param port master port
   * @param user user parameter of the dataset
   * @param program program parameter of the dataset
   * @param relation relation parameter of the dataset
   * @param dataset the dataset to upload
   * @param format the format of the relation ("json", "csv", "tsv")
   * 
   * @throws IOException if an error occurs
   */
  public static void replace(final String host, final int port, final String user, final String program,
      final String relation, final String dataset, final String format) throws IOException {
    URL url = getDatasetUrl(host, port, user, program, relation, format);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setDoOutput(true);
    conn.setRequestProperty("Content-Type", "application/octet-stream");
    conn.setRequestMethod("PUT");

    byte[] payload = dataset.getBytes("UTF-8");
    conn.setFixedLengthStreamingMode(payload.length);
    try {
      OutputStream out = conn.getOutputStream();
      out.write(payload);
      if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
        throw new IOException("Failed upload: " + conn.getResponseCode());
      }
    } finally {
      conn.disconnect();
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

  /**
   * @param masterHostname master hostname
   * @param apiPort rest api port
   * @param user user parameter of the dataset
   * @param program program parameter of the dataset
   * @param relation relation parameter of the dataset
   * @return a HTTPURLConnection instance of retrieving responses.
   * @throws IOException if IO errors
   * */
  public static HttpURLConnection deleteDataset(final String masterHostname, final int apiPort, final String user,
      final String program, final String relation) throws IOException {
    String type = "application/json";
    URL u =
        new URL(String.format("http://%s:%d/dataset/user-%s/program-%s/relation-%s/", masterHostname, apiPort, user,
            program, relation));
    HttpURLConnection conn = (HttpURLConnection) u.openConnection();
    conn.setRequestMethod("DELETE");
    conn.setRequestProperty("Content-Type", type);
    conn.connect();
    conn.getResponseCode();
    conn.disconnect();
    return conn;
  }
}
