package edu.washington.escience.myriad.tool;

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
    final File eclipseCPFile = new File(eclipseCPFilePath);
    if (!eclipseCPFile.exists()) {
      System.err.println("Eclipse classpath file does not exist");
      return;
    }

    final String[] cp = readEclipseClasspath(eclipseCPFile);

    // 1: need -Djava.library.path, for runtime
    // 0: doesn't need -Djava.library.path, for compiling
    // default: 1
    int needLibpath = 1;
    if (args.length > 1) {
      needLibpath = Integer.parseInt(args[1]);
    }
    if (needLibpath == 1) {
      System.out.print(" -Djava.library.path=" + cp[1] + " ");
      // if another classpath is provided, use it instead of eclipse classpath
      if (args.length > 2) {
        cp[0] = args[2] + System.getProperty("path.separator") + cp[0].substring(cp[0].indexOf("build/eclipse") + 14);
      }
    } else if (needLibpath == 0) {
      cp[0] = cp[0].substring(cp[0].indexOf("build/eclipse") + 14);
    }
    System.out.print(" -classpath " + cp[0] + " ");
  }

  /**
   * @param eclipseClasspathXMLFile the eclipse .classpath file.
   * @return [0] is the classpath and [1] is the lib path.
   * @throws IOException if any IO errors.
   * */
  public static String[] readEclipseClasspath(final File eclipseClasspathXMLFile) throws IOException {

    final DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
    DocumentBuilder dBuilder;
    try {
      dBuilder = dbFactory.newDocumentBuilder();
    } catch (final ParserConfigurationException e) {
      throw new IOException(e);
    }

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
          classpathSB.append(new File(e.getAttribute("path")).getAbsolutePath() + separator);
        }
        if (kind.equals("lib")) {
          classpathSB.append(new File(e.getAttribute("path")).getAbsolutePath() + separator);
        }
        if (kind.equals("src")) {
          classpathSB.append(new File(e.getAttribute("output")).getAbsoluteFile() + separator);
        }
      }
    }
    final NodeList attributeList = doc.getElementsByTagName("attribute");
    final StringBuilder libPathSB = new StringBuilder();
    for (int i = 0; i < attributeList.getLength(); i++) {
      final Node node = attributeList.item(i);

      if (node.getNodeType() == Node.ELEMENT_NODE
          && ("org.eclipse.jdt.launching.CLASSPATH_ATTR_LIBRARY_PATH_ENTRY".equals(((Element) node)
              .getAttribute("name")))) {
        String value = ((Element) node).getAttribute("value");
        if (value != null) {
          File f = new File(value);
          while (value != null && value.length() > 0 && !f.exists()) {
            value = value.substring(value.indexOf(File.separator) + 1);
            f = new File(value);
          }
          if (f.exists()) {
            libPathSB.append(f.getAbsolutePath() + separator);
          }
        }
      }
    }
    return new String[] { classpathSB.toString(), libPathSB.toString() };
  }

  /** Prevent construction of this class. */
  private EclipseClasspathReader() {
  }
}
