package edu.washington.escience.myria.tools;

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * Read from the eclipse .classpath file and generate a java classpath string and library string.
 * */
public final class EclipseClasspathReader {
  /**
   * usage.
   * */
  public static final String USAGE = "java EclipseClasspathReader [eclipse CP file path]";

  /**
   * entry point.
   *
   * @param args commandline args.
   * @throws IOException if file system error occurs.
   * */
  public static void main(final String[] args) throws IOException {
    if (args.length < 1) {
      System.out.println(USAGE);
    }

    final String eclipseCPFilePath = args[0];
    String type = "cp"; // cp: classpath; lib: libpath
    if (args.length >= 2) {
      if (args[1].equalsIgnoreCase("lib")) {
        type = "lib";
      }
    }

    final File eclipseCPFile = new File(eclipseCPFilePath);
    if (!eclipseCPFile.exists()) {
      System.err.println("Eclipse classpath file does not exist");
      return;
    }

    final String[] cp = readEclipseClasspath(eclipseCPFile);

    if (type.equals("cp")) {
      System.out.print(cp[0]);
      // System.out.print(" -classpath " + cp[0] + " ");
    } else {
      // System.out.print(" -Djava.library.path=" + cp[1] + " ");
      System.out.print(cp[1]);
    }
  }

  /**
   * @return if the provided path is an absolute path
   * @param path .
   * */
  public static boolean isAbsolutePath(final String path) {
    if (System.getProperty("os.name").startsWith("Windows")) {
      return path.toUpperCase().matches("[A-Z]:.*");
    } else {
      return path.startsWith("/");
    }
  }

  /**
   * @param eclipseClasspathXMLFile the eclipse .classpath file.
   * @return [0] is the classpath and [1] is the lib path.
   * @throws IOException if any IO errors.
   * */
  public static String[] readEclipseClasspath(final File eclipseClasspathXMLFile)
      throws IOException {

    final DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
    DocumentBuilder dBuilder;
    try {
      dBuilder = dbFactory.newDocumentBuilder();
    } catch (final ParserConfigurationException e) {
      throw new IOException(e);
    }

    File projectRoot = eclipseClasspathXMLFile.getParentFile();

    Document doc;
    try {
      doc = dBuilder.parse(eclipseClasspathXMLFile);
    } catch (final SAXException e) {
      throw new IOException(e);
    }
    doc.getDocumentElement().normalize();

    final NodeList nList = doc.getElementsByTagName("classpathentry");

    final String separator = System.getProperty("path.separator");

    final StringBuilder classpathSB = new StringBuilder();
    for (int i = 0; i < nList.getLength(); i++) {
      final Node node = nList.item(i);
      if (node.getNodeType() == Node.ELEMENT_NODE) {
        final Element e = (Element) node;

        final String kind = e.getAttribute("kind");
        if (kind.equals("output")) {
          String path = e.getAttribute("path");
          if (isAbsolutePath(path)) {
            classpathSB.append(new File(path).getAbsolutePath() + separator);
          } else {
            classpathSB.append(new File(projectRoot, path).getAbsolutePath() + separator);
          }
        }
        if (kind.equals("lib")) {
          String path = e.getAttribute("path");
          if (isAbsolutePath(path)) {
            classpathSB.append(new File(path).getAbsolutePath() + separator);
          } else {
            classpathSB.append(new File(projectRoot, path).getAbsolutePath() + separator);
          }
        }
        if (kind.equals("src")) {
          String path = e.getAttribute("output");
          if (isAbsolutePath(path)) {
            classpathSB.append(new File(path).getAbsoluteFile() + separator);
          } else {
            classpathSB.append(new File(projectRoot, path).getAbsoluteFile() + separator);
          }
        }
      }
    }
    final NodeList attributeList = doc.getElementsByTagName("attribute");
    final StringBuilder libPathSB = new StringBuilder();
    for (int i = 0; i < attributeList.getLength(); i++) {
      final Node node = attributeList.item(i);

      if (node.getNodeType() == Node.ELEMENT_NODE
          && ("org.eclipse.jdt.launching.CLASSPATH_ATTR_LIBRARY_PATH_ENTRY"
              .equals(((Element) node).getAttribute("name")))) {
        String value = ((Element) node).getAttribute("value");

        if (value != null) {
          // remove the project name
          File f = new File(projectRoot, value);
          while (value != null
              && value.length() > 0
              && !f.exists()
              && value.indexOf(File.separator) >= 0) {
            value = value.substring(value.indexOf(File.separator) + 1);
            f = new File(projectRoot, value);
          }
          if (f.exists()) {
            libPathSB.append(f.getAbsolutePath() + separator);
          }
        }
      }
    }
    return new String[] {classpathSB.toString(), libPathSB.toString()};
  }

  /** Prevent construction of this class. */
  private EclipseClasspathReader() {}
}
