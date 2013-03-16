package edu.washington.escience.myriad.util;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

/**
 * This class is from JavaWorld at: http://www.javaworld.com/javaworld/javatips/jw-javatip49.html
 * 
 * It's free of use according to the editor: http://www.javaworld.com/community/node/8181
 * 
 * The author should be Arthur Choi from IBM.
 * */
public class JarResources {

  // external debug flag
  public boolean debugOn = false;

  // jar resource mapping tables
  private final Hashtable<String, Integer> htSizes = new Hashtable<String, Integer>();
  private final Hashtable<String, byte[]> htJarContents = new Hashtable<String, byte[]>();

  // a jar file
  private final String jarFileName;

  /**
   * creates a JarResources. It extracts all resources from a Jar into an internal hashtable, keyed by resource names.
   * 
   * @param jarFileName a jar or zip file
   */
  public JarResources(String jarFileName) {
    this.jarFileName = jarFileName;
    init();
  }

  /**
   * Extracts a jar resource as a blob.
   * 
   * @param name a resource name.
   */
  public byte[] getResource(String name) {
    return htJarContents.get(name);
  }

  /**
   * initializes internal hash tables with Jar file resources.
   */
  private void init() {
    try {
      // extracts just sizes only.
      ZipFile zf = new ZipFile(jarFileName);
      Enumeration<? extends ZipEntry> e = zf.entries();
      while (e.hasMoreElements()) {
        ZipEntry ze = e.nextElement();
        if (debugOn) {
          System.out.println(dumpZipEntry(ze));
        }
        htSizes.put(ze.getName(), new Integer((int) ze.getSize()));
      }
      zf.close();

      // extract resources and put them into the hashtable.
      FileInputStream fis = new FileInputStream(jarFileName);
      BufferedInputStream bis = new BufferedInputStream(fis);
      ZipInputStream zis = new ZipInputStream(bis);
      ZipEntry ze = null;
      while ((ze = zis.getNextEntry()) != null) {
        if (ze.isDirectory()) {
          continue;
        }
        if (debugOn) {
          System.out.println("ze.getName()=" + ze.getName() + "," + "getSize()=" + ze.getSize());
        }
        int size = (int) ze.getSize();
        // -1 means unknown size.
        if (size == -1) {
          size = htSizes.get(ze.getName()).intValue();
        }
        byte[] b = new byte[size];
        int rb = 0;
        int chunk = 0;
        while ((size - rb) > 0) {
          chunk = zis.read(b, rb, size - rb);
          if (chunk == -1) {
            break;
          }
          rb += chunk;
        }
        // add to internal resource hashtable
        htJarContents.put(ze.getName(), b);
        if (debugOn) {
          System.out.println(ze.getName() + "  rb=" + rb + ",size=" + size + ",csize=" + ze.getCompressedSize());
        }
      }
    } catch (NullPointerException e) {
      System.out.println("done.");
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Dumps a zip entry into a string.
   * 
   * @param ze a ZipEntry
   */
  private String dumpZipEntry(ZipEntry ze) {
    StringBuffer sb = new StringBuffer();
    if (ze.isDirectory()) {
      sb.append("d ");
    } else {
      sb.append("f ");
    }
    if (ze.getMethod() == ZipEntry.STORED) {
      sb.append("stored   ");
    } else {
      sb.append("defalted ");
    }
    sb.append(ze.getName());
    sb.append("\t");
    sb.append("" + ze.getSize());
    if (ze.getMethod() == ZipEntry.DEFLATED) {
      sb.append("/" + ze.getCompressedSize());
    }
    return (sb.toString());
  }

}
