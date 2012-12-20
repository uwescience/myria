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

public final class EclipseClasspathReader {
  /** Prevent construction of this class. */
  private EclipseClasspathReader() {
  }

  public static final String usage = "java EclipseClasspathReader [eclipse CP file path]";

  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.out.println(usage);
    }

    String eclipseCPFilePath = args[0];
    File eclipseCPFile = new File(eclipseCPFilePath);
    if (!eclipseCPFile.exists()) {
      System.err.println("Eclipse classpath file does not exist");
      return;
    }

    String[] cp = readEclipseClasspath(eclipseCPFile);

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
        cp[0] = args[2] + ":" + cp[0].substring(cp[0].indexOf("build/eclipse") + 14);
      }
    } else if (needLibpath == 0) {
      cp[0] = cp[0].substring(cp[0].indexOf("build/eclipse") + 14);
    }
    System.out.print(" -classpath " + cp[0] + " ");
  }

  public static String[] readEclipseClasspath(final File eclipseClasspathXMLFile) throws IOException {

    final DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
    DocumentBuilder dBuilder;
    try {
      dBuilder = dbFactory.newDocumentBuilder();
    } catch (ParserConfigurationException e) {
      throw new IOException(e);
    }

    Document doc;
    try {
      doc = dBuilder.parse(eclipseClasspathXMLFile);
    } catch (SAXException e) {
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
      }
    }
    final NodeList attributeList = doc.getElementsByTagName("attribute");
    final StringBuilder libPathSB = new StringBuilder();
    for (int i = 0; i < attributeList.getLength(); i++) {
      final Node node = attributeList.item(i);
      String value = null;
      if (node.getNodeType() == Node.ELEMENT_NODE
          && ("org.eclipse.jdt.launching.CLASSPATH_ATTR_LIBRARY_PATH_ENTRY".equals(((Element) node)
              .getAttribute("name"))) && ((value = ((Element) node).getAttribute("value")) != null)) {
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
    return new String[] { classpathSB.toString(), libPathSB.toString() };
  }
}
