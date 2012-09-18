/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

/**
 * Modified from Configuration.java of hadoop 0.21.0
 * */

package edu.washington.escience.myriad.parallel;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.WeakHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Comment;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;

/**
 * Provides access to configuration parameters.
 * 
 * <h4 id="Resources">Resources</h4>
 * 
 * <p>
 * Configurations are specified by resources. A resource contains a set of name/value pairs as XML data. Each resource
 * is named by either a <code>String</code> or by a {@link Path}. If named by a <code>String</code>, then the classpath
 * is examined for a file with that name. If named by a <code>Path</code>, then the local filesystem is examined
 * directly, without referring to the classpath.
 * 
 * The configuration file is site.xml found in classpath.
 * 
 * <h4 id="FinalParams">Final Parameters</h4>
 * 
 * <p>
 * Configuration parameters may be declared <i>final</i>. Once a resource declares a value final, no subsequently-loaded
 * resource can alter that value. For example, one might define a final parameter with: <tt><pre>
 *  &lt;property&gt;
 *    &lt;name&gt;dfs.client.buffer.dir&lt;/name&gt;
 *    &lt;value&gt;/tmp/hadoop/dfs/client&lt;/value&gt;
 *    <b>&lt;final&gt;true&lt;/final&gt;</b>
 *  &lt;/property&gt;</pre></tt>
 * 
 * 
 * <h4 id="VariableExpansion">Variable Expansion</h4>
 * 
 * <p>
 * Value strings are first processed for <i>variable expansion</i>. The available properties are:
 * <ol>
 * <li>Other properties defined in this Configuration; and, if a name is undefined here,</li>
 * <li>Properties in {@link System#getProperties()}.</li>
 * </ol>
 * 
 * <p>
 * For example, if a configuration resource contains the following property definitions: <tt><pre>
 *  &lt;property&gt;
 *    &lt;name&gt;basedir&lt;/name&gt;
 *    &lt;value&gt;/user/${<i>user.name</i>}&lt;/value&gt;
 *  &lt;/property&gt;
 *  
 *  &lt;property&gt;
 *    &lt;name&gt;tempdir&lt;/name&gt;
 *    &lt;value&gt;${<i>basedir</i>}/tmp&lt;/value&gt;
 *  &lt;/property&gt;</pre></tt>
 * 
 * When <tt>conf.get("tempdir")</tt> is called, then <tt>${<i>basedir</i>}</tt> will be resolved to another property in
 * this Configuration, while <tt>${<i>user.name</i>}</tt> would then ordinarily be resolved to the value of the System
 * property with that name.
 */
public class Configuration implements Iterable<Map.Entry<String, String>> {

  protected static Logger LOG = LoggerFactory.getLogger(Configuration.class);

  /**
   * default conf files are searched in java classpath
   * */
  private final SocketInfo server;
  private final SocketInfo[] workers;
  public static final String DEFAULT_CONF_DIR = "conf";

  public Configuration(File confDir) throws IOException {
    this(true, confDir);
  }

  public Configuration() throws IOException {
    this(true, null);
  }

  public SocketInfo getServer() {
    return this.server;
  }

  public SocketInfo[] getWorkers() {
    return this.workers;
  }

  protected static SocketInfo[] loadWorkers(File confDir) throws IOException {
    ArrayList<SocketInfo> workers = new ArrayList<SocketInfo>();
    BufferedReader br =
        new BufferedReader(new InputStreamReader(new FileInputStream(new File(confDir + "/workers.conf"))));
    String line = null;
    while ((line = br.readLine()) != null) {
      String[] ts = line.replaceAll("[ \t]+", "").replaceAll("#.*$", "").split(":");
      if (ts.length >= 2)
        workers.add(new SocketInfo(ts[0], Integer.parseInt(ts[1])));
    }
    return workers.toArray(new SocketInfo[] {});
  }

  protected static SocketInfo loadServer(File confDir) throws IOException {
    BufferedReader br =
        new BufferedReader(new InputStreamReader(new FileInputStream(new File(confDir + "/server.conf"))));
    String line = null;
    while ((line = br.readLine()) != null) {
      String[] ts = line.replaceAll("[ \t]+", "").replaceAll("#.*$", "").split(":");
      if (ts.length == 2)
        return new SocketInfo(ts[0], Integer.parseInt(ts[1]));
    }
    throw new IOException("Wrong server conf file.");
  }

  private boolean quietmode = true;

  /**
   * List of configuration resources.
   */
  private ArrayList<Object> resources = new ArrayList<Object>();

  /**
   * The value reported as the setting resource when a key is set by code rather than a file resource.
   */
  static final String UNKNOWN_RESOURCE = "Unknown";

  /**
   * List of configuration parameters marked <b>final</b>.
   */
  private Set<String> finalParameters = new HashSet<String>();

  private boolean loadDefaults = true;

  /**
   * Configuration objects
   */
  private static final WeakHashMap<Configuration, Object> REGISTRY = new WeakHashMap<Configuration, Object>();

  /**
   * List of default Resources. Resources are loaded in the order of the list entries
   */
  private static final CopyOnWriteArrayList<String> defaultResources = new CopyOnWriteArrayList<String>();

  private static final Map<ClassLoader, Map<String, Class<?>>> CACHE_CLASSES =
      new WeakHashMap<ClassLoader, Map<String, Class<?>>>();

  /**
   * Stores the mapping of key to the resource which modifies or loads the key most recently
   */
  private HashMap<String, String> updatingResource;

  static {
    ClassLoader cL = Thread.currentThread().getContextClassLoader();
    if (cL == null) {
      cL = Configuration.class.getClassLoader();
    }
    addDefaultResource("site.xml");
  }

  private Properties properties;
  private Properties overlay;
  private ClassLoader classLoader;
  {
    classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = Configuration.class.getClassLoader();
    }
  }

  /**
   * A new configuration where the behavior of reading from the default resources can be turned off.
   * 
   * If the parameter {@code loadDefaults} is false, the new instance will not load resources from the default files.
   * 
   * @param loadDefaults specifies whether to load from the default files
   * @throws IOException
   */
  public Configuration(boolean loadDefaults, File confDir) throws IOException {
    // this.confDir = confDir;
    if (confDir == null) {
      confDir = new File("conf");
    }
    this.server = loadServer(new File("conf"));
    this.workers = loadWorkers(new File("conf"));

    this.loadDefaults = loadDefaults;
    updatingResource = new HashMap<String, String>();
    synchronized (Configuration.class) {
      REGISTRY.put(this, null);
    }
    if (confDir != null) {
      this.addResource(new FileInputStream(confDir.getAbsolutePath() + "/site.xml"));
    }
  }

  /**
   * A new configuration with the same settings cloned from another.
   * 
   * @param other the configuration from which to clone settings.
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public Configuration(Configuration other) throws IOException {
    this();
    this.resources = (ArrayList<Object>) other.resources.clone();
    synchronized (other) {
      if (other.properties != null) {
        this.properties = (Properties) other.properties.clone();
      }

      if (other.overlay != null) {
        this.overlay = (Properties) other.overlay.clone();
      }

      this.updatingResource = new HashMap<String, String>(other.updatingResource);
    }

    this.finalParameters = new HashSet<String>(other.finalParameters);
    synchronized (Configuration.class) {
      REGISTRY.put(this, null);
    }
    this.classLoader = other.classLoader;
    this.loadDefaults = other.loadDefaults;
    setQuietMode(other.getQuietMode());
  }

  /**
   * Add a default resource. Resources are loaded in the order of the resources added.
   * 
   * @param name file name. File should be present in the classpath.
   */
  public static synchronized void addDefaultResource(String name) {
    if (!defaultResources.contains(name)) {
      defaultResources.add(name);
      for (Configuration conf : REGISTRY.keySet()) {
        if (conf.loadDefaults) {
          conf.reloadConfiguration();
        }
      }
    }
  }

  /**
   * Add a configuration resource.
   * 
   * The properties of this resource will override properties of previously added resources, unless they were marked <a
   * href="#Final">final</a>.
   * 
   * @param name resource to be added, the classpath is examined for a file with that name.
   */
  public void addResource(String name) {
    addResourceObject(name);
  }

  /**
   * Add a configuration resource.
   * 
   * The properties of this resource will override properties of previously added resources, unless they were marked <a
   * href="#Final">final</a>.
   * 
   * @param url url of the resource to be added, the local filesystem is examined directly to find the resource, without
   *          referring to the classpath.
   */
  public void addResource(URL url) {
    addResourceObject(url);
  }

  /**
   * Add a configuration resource.
   * 
   * The properties of this resource will override properties of previously added resources, unless they were marked <a
   * href="#Final">final</a>.
   * 
   * @param in InputStream to deserialize the object from.
   */
  public void addResource(InputStream in) {
    addResourceObject(in);
  }

  /**
   * Reload configuration from previously added resources.
   * 
   * This method will clear all the configuration read from the added resources, and final parameters. This will make
   * the resources to be read again before accessing the values. Values that are added via set methods will overlay
   * values read from the resources.
   */
  public synchronized void reloadConfiguration() {
    properties = null; // trigger reload
    finalParameters.clear(); // clear site-limits
  }

  private synchronized void addResourceObject(Object resource) {
    resources.add(resource); // add to resources
    reloadConfiguration();
  }

  private static Pattern varPat = Pattern.compile("\\$\\{[^\\}\\$\u0020]+\\}");
  private static int MAX_SUBST = 20;

  private String substituteVars(String expr) {
    if (expr == null) {
      return null;
    }
    Matcher match = varPat.matcher("");
    String eval = expr;
    for (int s = 0; s < MAX_SUBST; s++) {
      match.reset(eval);
      if (!match.find()) {
        return eval;
      }
      String var = match.group();
      var = var.substring(2, var.length() - 1); // remove ${ .. }
      String val = null;
      try {
        val = System.getProperty(var);
      } catch (SecurityException se) {
        LOG.warn("Unexpected SecurityException in Configuration", se);
      }
      if (val == null) {
        val = getRaw(var);
      }
      if (val == null) {
        return eval; // return literal ${var}: var is unbound
      }
      // substitute
      eval = eval.substring(0, match.start()) + val + eval.substring(match.end());
    }
    throw new IllegalStateException("Variable substitution depth too large: " + MAX_SUBST + " " + expr);
  }

  /**
   * Get the value of the <code>name</code> property, <code>null</code> if no such property exists. If the key is
   * deprecated, it returns the value of the first key which replaces the deprecated key and is not null
   * 
   * Values are processed for <a href="#VariableExpansion">variable expansion</a> before being returned.
   * 
   * @param name the property name.
   * @return the value of the <code>name</code> or its replacing property, or null if no such property exists.
   */
  public String get(String name) {
    // name = handleDeprecation(name);
    return substituteVars(getProps().getProperty(name));
  }

  /**
   * Get the value of the <code>name</code> property, without doing <a href="#VariableExpansion">variable
   * expansion</a>.If the key is deprecated, it returns the value of the first key which replaces the deprecated key and
   * is not null.
   * 
   * @param name the property name.
   * @return the value of the <code>name</code> property or its replacing property and null if no such property exists.
   */
  public String getRaw(String name) {
    // name = handleDeprecation(name);
    return getProps().getProperty(name);
  }

  /**
   * Set the <code>value</code> of the <code>name</code> property. If <code>name</code> is deprecated, it sets the
   * <code>value</code> to the keys that replace the deprecated key.
   * 
   * @param name property name.
   * @param value property value.
   */
  public void set(String name, String value) {
    // if (deprecatedKeyMap.isEmpty()) {
    getProps();
    // }
    // if (!isDeprecated(name)) {
    getOverlay().setProperty(name, value);
    getProps().setProperty(name, value);
    updatingResource.put(name, UNKNOWN_RESOURCE);
    // } else {
    // DeprecatedKeyInfo keyInfo = deprecatedKeyMap.get(name);
    // LOG.warn(keyInfo.getWarningMessage(name));
    // for (String newKey : keyInfo.newKeys) {
    // getOverlay().setProperty(newKey, value);
    // getProps().setProperty(newKey, value);
    // }
    // }
  }

  /**
   * Sets a property if it is currently unset.
   * 
   * @param name the property name
   * @param value the new value
   */
  public void setIfUnset(String name, String value) {
    if (get(name) == null) {
      set(name, value);
    }
  }

  private synchronized Properties getOverlay() {
    if (overlay == null) {
      overlay = new Properties();
    }
    return overlay;
  }

  /**
   * Get the value of the <code>name</code>. If the key is deprecated, it returns the value of the first key which
   * replaces the deprecated key and is not null. If no such property exists, then <code>defaultValue</code> is
   * returned.
   * 
   * @param name property name.
   * @param defaultValue default value.
   * @return property value, or <code>defaultValue</code> if the property doesn't exist.
   */
  public String get(String name, String defaultValue) {
    // name = handleDeprecation(name);
    return substituteVars(getProps().getProperty(name, defaultValue));
  }

  /**
   * Get the value of the <code>name</code> property as an <code>int</code>.
   * 
   * If no such property exists, or if the specified value is not a valid <code>int</code>, then
   * <code>defaultValue</code> is returned.
   * 
   * @param name property name.
   * @param defaultValue default value.
   * @return property value as an <code>int</code>, or <code>defaultValue</code>.
   */
  public int getInt(String name, int defaultValue) {
    String valueString = get(name);
    if (valueString == null)
      return defaultValue;
    try {
      String hexString = getHexDigits(valueString);
      if (hexString != null) {
        return Integer.parseInt(hexString, 16);
      }
      return Integer.parseInt(valueString);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  /**
   * Set the value of the <code>name</code> property to an <code>int</code>.
   * 
   * @param name property name.
   * @param value <code>int</code> value of the property.
   */
  public void setInt(String name, int value) {
    set(name, Integer.toString(value));
  }

  /**
   * Get the value of the <code>name</code> property as a <code>long</code>. If no such property is specified, or if the
   * specified value is not a valid <code>long</code>, then <code>defaultValue</code> is returned.
   * 
   * @param name property name.
   * @param defaultValue default value.
   * @return property value as a <code>long</code>, or <code>defaultValue</code>.
   */
  public long getLong(String name, long defaultValue) {
    String valueString = get(name);
    if (valueString == null)
      return defaultValue;
    try {
      String hexString = getHexDigits(valueString);
      if (hexString != null) {
        return Long.parseLong(hexString, 16);
      }
      return Long.parseLong(valueString);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  private String getHexDigits(String value) {
    boolean negative = false;
    String str = value;
    String hexString = null;
    if (value.startsWith("-")) {
      negative = true;
      str = value.substring(1);
    }
    if (str.startsWith("0x") || str.startsWith("0X")) {
      hexString = str.substring(2);
      if (negative) {
        hexString = "-" + hexString;
      }
      return hexString;
    }
    return null;
  }

  /**
   * Set the value of the <code>name</code> property to a <code>long</code>.
   * 
   * @param name property name.
   * @param value <code>long</code> value of the property.
   */
  public void setLong(String name, long value) {
    set(name, Long.toString(value));
  }

  /**
   * Get the value of the <code>name</code> property as a <code>float</code>. If no such property is specified, or if
   * the specified value is not a valid <code>float</code>, then <code>defaultValue</code> is returned.
   * 
   * @param name property name.
   * @param defaultValue default value.
   * @return property value as a <code>float</code>, or <code>defaultValue</code>.
   */
  public float getFloat(String name, float defaultValue) {
    String valueString = get(name);
    if (valueString == null)
      return defaultValue;
    try {
      return Float.parseFloat(valueString);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  /**
   * Set the value of the <code>name</code> property to a <code>float</code>.
   * 
   * @param name property name.
   * @param value property value.
   */
  public void setFloat(String name, float value) {
    set(name, Float.toString(value));
  }

  /**
   * Get the value of the <code>name</code> property as a <code>boolean</code>. If no such property is specified, or if
   * the specified value is not a valid <code>boolean</code>, then <code>defaultValue</code> is returned.
   * 
   * @param name property name.
   * @param defaultValue default value.
   * @return property value as a <code>boolean</code>, or <code>defaultValue</code>.
   */
  public boolean getBoolean(String name, boolean defaultValue) {
    String valueString = get(name);
    if ("true".equals(valueString))
      return true;
    else if ("false".equals(valueString))
      return false;
    else
      return defaultValue;
  }

  /**
   * Set the value of the <code>name</code> property to a <code>boolean</code>.
   * 
   * @param name property name.
   * @param value <code>boolean</code> value of the property.
   */
  public void setBoolean(String name, boolean value) {
    set(name, Boolean.toString(value));
  }

  /**
   * Set the given property, if it is currently unset.
   * 
   * @param name property name
   * @param value new value
   */
  public void setBooleanIfUnset(String name, boolean value) {
    setIfUnset(name, Boolean.toString(value));
  }

  /**
   * Set the value of the <code>name</code> property to the given type. This is equivalent to
   * <code>set(&lt;name&gt;, value.toString())</code>.
   * 
   * @param name property name
   * @param value new value
   */
  public <T extends Enum<T>> void setEnum(String name, T value) {
    set(name, value.toString());
  }

  /**
   * Return value matching this enumerated type.
   * 
   * @param name Property name
   * @param defaultValue Value returned if no mapping exists
   * @throws IllegalArgumentException If mapping is illegal for the type provided
   */
  public <T extends Enum<T>> T getEnum(String name, T defaultValue) {
    final String val = get(name);
    return null == val ? defaultValue : Enum.valueOf(defaultValue.getDeclaringClass(), val);
  }

  /**
   * Get the value of the <code>name</code> property as a <code>Pattern</code>. If no such property is specified, or if
   * the specified value is not a valid <code>Pattern</code>, then <code>DefaultValue</code> is returned.
   * 
   * @param name property name
   * @param defaultValue default value
   * @return property value as a compiled Pattern, or defaultValue
   */
  public Pattern getPattern(String name, Pattern defaultValue) {
    String valString = get(name);
    if (null == valString || "".equals(valString)) {
      return defaultValue;
    }
    try {
      return Pattern.compile(valString);
    } catch (PatternSyntaxException pse) {
      LOG.warn("Regular expression '" + valString + "' for property '" + name + "' not valid. Using default", pse);
      return defaultValue;
    }
  }

  /**
   * Set the given property to <code>Pattern</code>. If the pattern is passed as null, sets the empty pattern which
   * results in further calls to getPattern(...) returning the default value.
   * 
   * @param name property name
   * @param pattern new value
   */
  public void setPattern(String name, Pattern pattern) {
    if (null == pattern) {
      set(name, null);
    } else {
      set(name, pattern.pattern());
    }
  }

  /**
   * A class that represents a set of positive integer ranges. It parses strings of the form: "2-3,5,7-" where ranges
   * are separated by comma and the lower/upper bounds are separated by dash. Either the lower or upper bound may be
   * omitted meaning all values up to or over. So the string above means 2, 3, 5, and 7, 8, 9, ...
   */
  public static class IntegerRanges {
    private static class Range {
      int start;
      int end;
    }

    List<Range> ranges = new ArrayList<Range>();

    public IntegerRanges() {
    }

    public IntegerRanges(String newValue) {
      StringTokenizer itr = new StringTokenizer(newValue, ",");
      while (itr.hasMoreTokens()) {
        String rng = itr.nextToken().trim();
        String[] parts = rng.split("-", 3);
        if (parts.length < 1 || parts.length > 2) {
          throw new IllegalArgumentException("integer range badly formed: " + rng);
        }
        Range r = new Range();
        r.start = convertToInt(parts[0], 0);
        if (parts.length == 2) {
          r.end = convertToInt(parts[1], Integer.MAX_VALUE);
        } else {
          r.end = r.start;
        }
        if (r.start > r.end) {
          throw new IllegalArgumentException("IntegerRange from " + r.start + " to " + r.end + " is invalid");
        }
        ranges.add(r);
      }
    }

    /**
     * Convert a string to an int treating empty strings as the default value.
     * 
     * @param value the string value
     * @param defaultValue the value for if the string is empty
     * @return the desired integer
     */
    private static int convertToInt(String value, int defaultValue) {
      String trim = value.trim();
      if (trim.length() == 0) {
        return defaultValue;
      }
      return Integer.parseInt(trim);
    }

    /**
     * Is the given value in the set of ranges
     * 
     * @param value the value to check
     * @return is the value in the ranges?
     */
    public boolean isIncluded(int value) {
      for (Range r : ranges) {
        if (r.start <= value && value <= r.end) {
          return true;
        }
      }
      return false;
    }

    @Override
    public String toString() {
      StringBuilder result = new StringBuilder();
      boolean first = true;
      for (Range r : ranges) {
        if (first) {
          first = false;
        } else {
          result.append(',');
        }
        result.append(r.start);
        result.append('-');
        result.append(r.end);
      }
      return result.toString();
    }
  }

  /**
   * Parse the given attribute as a set of integer ranges
   * 
   * @param name the attribute name
   * @param defaultValue the default value if it is not set
   * @return a new set of ranges from the configured value
   */
  public IntegerRanges getRange(String name, String defaultValue) {
    return new IntegerRanges(get(name, defaultValue));
  }

  /**
   * Get the comma delimited values of the <code>name</code> property as a collection of <code>String</code>s. If no
   * such property is specified then empty collection is returned.
   * <p>
   * This is an optimized version of {@link #getStrings(String)}
   * 
   * @param name property name.
   * @return property value as a collection of <code>String</code>s.
   */
  public Collection<String> getStringCollection(String name) {
    String valueString = get(name);
    List<String> values = new ArrayList<String>();
    if (valueString == null)
      return values;
    StringTokenizer tokenizer = new StringTokenizer(valueString, ",");
    values = new ArrayList<String>();
    while (tokenizer.hasMoreTokens()) {
      values.add(tokenizer.nextToken());
    }
    return values;
  }

  /**
   * Get the comma delimited values of the <code>name</code> property as an array of <code>String</code>s. If no such
   * property is specified then <code>null</code> is returned.
   * 
   * @param name property name.
   * @return property value as an array of <code>String</code>s, or <code>null</code>.
   */
  public String[] getStrings(String name) {
    Collection<String> values = this.getStringCollection(name);

    return values.toArray(new String[values.size()]);
  }

  /**
   * Get the comma delimited values of the <code>name</code> property as an array of <code>String</code>s. If no such
   * property is specified then default value is returned.
   * 
   * @param name property name.
   * @param defaultValue The default value
   * @return property value as an array of <code>String</code>s, or default value.
   */
  public String[] getStrings(String name, String... defaultValue) {
    String valueString = get(name);
    if (valueString == null) {
      return defaultValue;
    } else {
      return getStrings(name);
    }
  }

  /**
   * Get the comma delimited values of the <code>name</code> property as a collection of <code>String</code>s, trimmed
   * of the leading and trailing whitespace. If no such property is specified then empty <code>Collection</code> is
   * returned.
   * 
   * @param name property name.
   * @return property value as a collection of <code>String</code>s, or empty <code>Collection</code>
   */
  public Collection<String> getTrimmedStringCollection(String name) {
    String valueString = get(name);
    if (null == valueString) {
      Collection<String> empty = Collections.emptyList();
      return empty;
    }
    return Arrays.asList(getTrimmedStrings(valueString));
  }

  /**
   * Get the comma delimited values of the <code>name</code> property as an array of <code>String</code>s, trimmed of
   * the leading and trailing whitespace. If no such property is specified then an empty array is returned.
   * 
   * @param name property name.
   * @return property value as an array of trimmed <code>String</code>s, or empty array.
   */
  public String[] getTrimmedStrings(String name) {
    String valueString = get(name);
    if (null == valueString || "".equals(valueString.trim())) {
      return emptyStringArray;
    }
    return valueString.trim().split("\\s*,\\s*");
  }

  final public static String[] emptyStringArray = {};

  /**
   * Get the comma delimited values of the <code>name</code> property as an array of <code>String</code>s, trimmed of
   * the leading and trailing whitespace. If no such property is specified then default value is returned.
   * 
   * @param name property name.
   * @param defaultValue The default value
   * @return property value as an array of trimmed <code>String</code>s, or default value.
   */
  public String[] getTrimmedStrings(String name, String... defaultValue) {
    String valueString = get(name);
    if (null == valueString) {
      return defaultValue;
    } else {
      return getTrimmedStrings(name);
    }
  }

  /**
   * Set the array of string values for the <code>name</code> property as as comma delimited values.
   * 
   * @param name property name.
   * @param values The values
   */
  public void setStrings(String name, String... values) {
    set(name, arrayToString(values));
  }

  public String arrayToString(String[] strs) {
    if (strs.length == 0) {
      return "";
    }
    StringBuilder sbuf = new StringBuilder();
    sbuf.append(strs[0]);
    for (int idx = 1; idx < strs.length; idx++) {
      sbuf.append(",");
      sbuf.append(strs[idx]);
    }
    return sbuf.toString();
  }

  /**
   * Load a class by name.
   * 
   * @param name the class name.
   * @return the class object.
   * @throws ClassNotFoundException if the class is not found.
   */
  public Class<?> getClassByName(String name) throws ClassNotFoundException {
    Map<String, Class<?>> map;

    synchronized (CACHE_CLASSES) {
      map = CACHE_CLASSES.get(classLoader);
      if (map == null) {
        map = Collections.synchronizedMap(new WeakHashMap<String, Class<?>>());
        CACHE_CLASSES.put(classLoader, map);
      }
    }

    Class<?> clazz = map.get(name);
    if (clazz == null) {
      clazz = Class.forName(name, true, classLoader);
      if (clazz != null) {
        // two putters can race here, but they'll put the same class
        map.put(name, clazz);
      }
    }

    return clazz;
  }

  /**
   * Get the value of the <code>name</code> property as an array of <code>Class</code>. The value of the property
   * specifies a list of comma separated class names. If no such property is specified, then <code>defaultValue</code>
   * is returned.
   * 
   * @param name the property name.
   * @param defaultValue default value.
   * @return property value as a <code>Class[]</code>, or <code>defaultValue</code>.
   */
  public Class<?>[] getClasses(String name, Class<?>... defaultValue) {
    String[] classnames = getStrings(name);
    if (classnames == null)
      return defaultValue;
    try {
      Class<?>[] classes = new Class<?>[classnames.length];
      for (int i = 0; i < classnames.length; i++) {
        classes[i] = getClassByName(classnames[i]);
      }
      return classes;
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get the value of the <code>name</code> property as a <code>Class</code>. If no such property is specified, then
   * <code>defaultValue</code> is returned.
   * 
   * @param name the class name.
   * @param defaultValue default value.
   * @return property value as a <code>Class</code>, or <code>defaultValue</code>.
   */
  public Class<?> getClass(String name, Class<?> defaultValue) {
    String valueString = get(name);
    if (valueString == null)
      return defaultValue;
    try {
      return getClassByName(valueString);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get the value of the <code>name</code> property as a <code>Class</code> implementing the interface specified by
   * <code>xface</code>.
   * 
   * If no such property is specified, then <code>defaultValue</code> is returned.
   * 
   * An exception is thrown if the returned class does not implement the named interface.
   * 
   * @param name the class name.
   * @param defaultValue default value.
   * @param xface the interface implemented by the named class.
   * @return property value as a <code>Class</code>, or <code>defaultValue</code>.
   */
  public <U> Class<? extends U> getClass(String name, Class<? extends U> defaultValue, Class<U> xface) {
    try {
      Class<?> theClass = getClass(name, defaultValue);
      if (theClass != null && !xface.isAssignableFrom(theClass))
        throw new RuntimeException(theClass + " not " + xface.getName());
      else if (theClass != null)
        return theClass.asSubclass(xface);
      else
        return null;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // /**
  // * Get the value of the <code>name</code> property as a <code>List</code>
  // * of objects implementing the interface specified by <code>xface</code>.
  // *
  // * An exception is thrown if any of the classes does not exist, or if it does
  // * not implement the named interface.
  // *
  // * @param name the property name.
  // * @param xface the interface implemented by the classes named by
  // * <code>name</code>.
  // * @return a <code>List</code> of objects implementing <code>xface</code>.
  // */
  // @SuppressWarnings("unchecked")
  // public <U> List<U> getInstances(String name, Class<U> xface) {
  // List<U> ret = new ArrayList<U>();
  // Class<?>[] classes = getClasses(name);
  // for (Class<?> cl: classes) {
  // if (!xface.isAssignableFrom(cl)) {
  // throw new RuntimeException(cl + " does not implement " + xface);
  // }
  // ret.add((U)ReflectionUtils.newInstance(cl, this));
  // }
  // return ret;
  // }
  //
  /**
   * Set the value of the <code>name</code> property to the name of a <code>theClass</code> implementing the given
   * interface <code>xface</code>.
   * 
   * An exception is thrown if <code>theClass</code> does not implement the interface <code>xface</code>.
   * 
   * @param name property name.
   * @param theClass property value.
   * @param xface the interface implemented by the named class.
   */
  public void setClass(String name, Class<?> theClass, Class<?> xface) {
    if (!xface.isAssignableFrom(theClass))
      throw new RuntimeException(theClass + " not " + xface.getName());
    set(name, theClass.getName());
  }

  // /**
  // * Get a local file under a directory named by <i>dirsProp</i> with
  // * the given <i>path</i>. If <i>dirsProp</i> contains multiple directories,
  // * then one is chosen based on <i>path</i>'s hash code. If the selected
  // * directory does not exist, an attempt is made to create it.
  // *
  // * @param dirsProp directory in which to locate the file.
  // * @param path file-path.
  // * @return local file under the directory with the given path.
  // */
  // public Path getLocalPath(String dirsProp, String path)
  // throws IOException {
  // String[] dirs = getStrings(dirsProp);
  // int hashCode = path.hashCode();
  // FileSystem fs = FileSystem.getLocal(this);
  // for (int i = 0; i < dirs.length; i++) { // try each local dir
  // int index = (hashCode+i & Integer.MAX_VALUE) % dirs.length;
  // Path file = new Path(dirs[index], path);
  // Path dir = file.getParent();
  // if (fs.mkdirs(dir) || fs.exists(dir)) {
  // return file;
  // }
  // }
  // LOG.warn("Could not make " + path +
  // " in local directories from " + dirsProp);
  // for(int i=0; i < dirs.length; i++) {
  // int index = (hashCode+i & Integer.MAX_VALUE) % dirs.length;
  // LOG.warn(dirsProp + "[" + index + "]=" + dirs[index]);
  // }
  // throw new IOException("No valid local directories in property: "+dirsProp);
  // }

  /**
   * Get a local file name under a directory named in <i>dirsProp</i> with the given <i>path</i>. If <i>dirsProp</i>
   * contains multiple directories, then one is chosen based on <i>path</i>'s hash code. If the selected directory does
   * not exist, an attempt is made to create it.
   * 
   * @param dirsProp directory in which to locate the file.
   * @param path file-path.
   * @return local file under the directory with the given path.
   */
  public File getFile(String dirsProp, String path) throws IOException {
    String[] dirs = getStrings(dirsProp);
    int hashCode = path.hashCode();
    for (int i = 0; i < dirs.length; i++) { // try each local dir
      int index = (hashCode + i & Integer.MAX_VALUE) % dirs.length;
      File file = new File(dirs[index], path);
      File dir = file.getParentFile();
      if (dir.exists() || dir.mkdirs()) {
        return file;
      }
    }
    throw new IOException("No valid local directories in property: " + dirsProp);
  }

  /**
   * Get the {@link URL} for the named resource.
   * 
   * @param name resource name.
   * @return the url for the named resource.
   */
  public URL getResource(String name) {
    return classLoader.getResource(name);
  }

  /**
   * Get an input stream attached to the configuration resource with the given <code>name</code>.
   * 
   * @param name configuration resource name.
   * @return an input stream attached to the resource.
   */
  public InputStream getConfResourceAsInputStream(String name) {
    try {
      URL url = getResource(name);

      if (url == null) {
        LOG.info(name + " not found");
        return null;
      } else {
        LOG.info("found resource " + name + " at " + url);
      }

      return url.openStream();
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Get a {@link Reader} attached to the configuration resource with the given <code>name</code>.
   * 
   * @param name configuration resource name.
   * @return a reader attached to the resource.
   */
  public Reader getConfResourceAsReader(String name) {
    try {
      URL url = getResource(name);

      if (url == null) {
        LOG.info(name + " not found");
        return null;
      } else {
        LOG.info("found resource " + name + " at " + url);
      }

      return new InputStreamReader(url.openStream());
    } catch (Exception e) {
      return null;
    }
  }

  protected synchronized Properties getProps() {
    if (properties == null) {
      properties = new Properties();
      loadResources(properties, resources, quietmode);
      if (overlay != null) {
        properties.putAll(overlay);
        for (Map.Entry<Object, Object> item : overlay.entrySet()) {
          updatingResource.put((String) item.getKey(), UNKNOWN_RESOURCE);
        }
      }
    }
    return properties;
  }

  /**
   * Return the number of keys in the configuration.
   * 
   * @return number of keys in the configuration.
   */
  public int size() {
    return getProps().size();
  }

  /**
   * Clears all keys from the configuration.
   */
  public void clear() {
    getProps().clear();
    getOverlay().clear();
  }

  /**
   * Get an {@link Iterator} to go through the list of <code>String</code> key-value pairs in the configuration.
   * 
   * @return an iterator over the entries.
   */
  public Iterator<Map.Entry<String, String>> iterator() {
    // Get a copy of just the string to string pairs. After the old object
    // methods that allow non-strings to be put into configurations are removed,
    // we could replace properties with a Map<String,String> and get rid of this
    // code.
    Map<String, String> result = new HashMap<String, String>();
    for (Map.Entry<Object, Object> item : getProps().entrySet()) {
      if (item.getKey() instanceof String && item.getValue() instanceof String) {
        result.put((String) item.getKey(), (String) item.getValue());
      }
    }
    return result.entrySet().iterator();
  }

  private void loadResources(Properties properties, ArrayList<Object> resources, boolean quiet) {
    if (loadDefaults) {
      for (String resource : defaultResources) {
        loadResource(properties, resource, quiet);
      }
    }

    for (Object resource : resources) {
      loadResource(properties, resource, quiet);
    }
  }

  private void loadResource(Properties properties, Object name, boolean quiet) {
    try {
      DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
      // ignore all comments inside the xml file
      docBuilderFactory.setIgnoringComments(true);

      // allow includes in the xml file
      docBuilderFactory.setNamespaceAware(true);
      try {
        docBuilderFactory.setXIncludeAware(true);
      } catch (UnsupportedOperationException e) {
        LOG.error("Failed to set setXIncludeAware(true) for parser " + docBuilderFactory + ":" + e, e);
      }
      DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
      Document doc = null;
      Element root = null;

      if (name instanceof URL) { // an URL resource
        URL url = (URL) name;
        if (url != null) {
          if (!quiet) {
            LOG.info("parsing " + url);
          }
          doc = builder.parse(url.toString());
        }
      } else if (name instanceof String) { // a CLASSPATH resource
        URL url = getResource((String) name);
        if (url != null) {
          if (!quiet) {
            LOG.info("parsing " + url);
          }
          doc = builder.parse(url.toString());
        }
      } else if (name instanceof InputStream) {
        try {
          doc = builder.parse((InputStream) name);
        } finally {
          ((InputStream) name).close();
        }
      } else if (name instanceof Element) {
        root = (Element) name;
      }

      if (doc == null && root == null) {
        if (quiet)
          return;
        throw new RuntimeException(name + " not found");
      }

      if (root == null) {
        root = doc.getDocumentElement();
      }
      if (!"configuration".equals(root.getTagName()))
        LOG.error("bad conf file: top-level element not <configuration>");
      NodeList props = root.getChildNodes();
      for (int i = 0; i < props.getLength(); i++) {
        Node propNode = props.item(i);
        if (!(propNode instanceof Element))
          continue;
        Element prop = (Element) propNode;
        if ("configuration".equals(prop.getTagName())) {
          loadResource(properties, prop, quiet);
          continue;
        }
        if (!"property".equals(prop.getTagName()))
          LOG.warn("bad conf file: element not <property>");
        NodeList fields = prop.getChildNodes();
        String attr = null;
        String value = null;
        boolean finalParameter = false;
        for (int j = 0; j < fields.getLength(); j++) {
          Node fieldNode = fields.item(j);
          if (!(fieldNode instanceof Element))
            continue;
          Element field = (Element) fieldNode;
          if ("name".equals(field.getTagName()) && field.hasChildNodes())
            attr = ((Text) field.getFirstChild()).getData().trim();
          if ("value".equals(field.getTagName()) && field.hasChildNodes())
            value = ((Text) field.getFirstChild()).getData();
          if ("final".equals(field.getTagName()) && field.hasChildNodes())
            finalParameter = "true".equals(((Text) field.getFirstChild()).getData());
        }

        // Ignore this parameter if it has already been marked as 'final'
        if (attr != null) {
          // if (deprecatedKeyMap.containsKey(attr)) {
          // DeprecatedKeyInfo keyInfo = deprecatedKeyMap.get(attr);
          // keyInfo.accessed = false;
          // for (String key : keyInfo.newKeys) {
          // update new keys with deprecated key's value
          // loadProperty(properties, name, key, value, finalParameter);
          // }
          // } else {
          loadProperty(properties, name, attr, value, finalParameter);
          // }
        }
      }

    } catch (IOException e) {
      LOG.error("error parsing conf file: " + e);
      throw new RuntimeException(e);
    } catch (DOMException e) {
      LOG.error("error parsing conf file: " + e);
      throw new RuntimeException(e);
    } catch (SAXException e) {
      LOG.error("error parsing conf file: " + e);
      throw new RuntimeException(e);
    } catch (ParserConfigurationException e) {
      LOG.error("error parsing conf file: " + e);
      throw new RuntimeException(e);
    }
  }

  private void loadProperty(Properties properties, Object name, String attr, String value, boolean finalParameter) {
    if (value != null) {
      if (!finalParameters.contains(attr)) {
        properties.setProperty(attr, value);
        updatingResource.put(attr, name.toString());
      } else {
        LOG.warn(name + ":an attempt to override final parameter: " + attr + ";  Ignoring.");
      }
    }
    if (finalParameter) {
      finalParameters.add(attr);
    }
  }

  /**
   * Write out the non-default properties in this configuration to the given {@link OutputStream}.
   * 
   * @param out the output stream to write to.
   */
  public void writeXml(OutputStream out) throws IOException {
    writeXml(new OutputStreamWriter(out));
  }

  /**
   * Write out the non-default properties in this configuration to the given {@link Writer}.
   * 
   * @param out the writer to write to.
   */
  public synchronized void writeXml(Writer out) throws IOException {
    Properties properties = getProps();
    try {
      Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
      Element conf = doc.createElement("configuration");
      doc.appendChild(conf);
      conf.appendChild(doc.createTextNode("\n"));
      for (Enumeration<Object> e = properties.keys(); e.hasMoreElements();) {
        String name = (String) e.nextElement();
        Object object = properties.get(name);
        String value = null;
        if (object instanceof String) {
          value = (String) object;
        } else {
          continue;
        }
        Element propNode = doc.createElement("property");
        conf.appendChild(propNode);

        if (updatingResource != null) {
          Comment commentNode = doc.createComment("Loaded from " + updatingResource.get(name));
          propNode.appendChild(commentNode);
        }
        Element nameNode = doc.createElement("name");
        nameNode.appendChild(doc.createTextNode(name));
        propNode.appendChild(nameNode);

        Element valueNode = doc.createElement("value");
        valueNode.appendChild(doc.createTextNode(value));
        propNode.appendChild(valueNode);

        conf.appendChild(doc.createTextNode("\n"));
      }

      DOMSource source = new DOMSource(doc);
      StreamResult result = new StreamResult(out);
      TransformerFactory transFactory = TransformerFactory.newInstance();
      Transformer transformer = transFactory.newTransformer();
      transformer.transform(source, result);
    } catch (TransformerException te) {
      throw new IOException(te);
    } catch (ParserConfigurationException pe) {
      throw new IOException(pe);
    }
  }

  /**
   * Get the {@link ClassLoader} for this job.
   * 
   * @return the correct class loader.
   */
  public ClassLoader getClassLoader() {
    return classLoader;
  }

  /**
   * Set the class loader that will be used to load the various objects.
   * 
   * @param classLoader the new class loader.
   */
  public void setClassLoader(ClassLoader classLoader) {
    this.classLoader = classLoader;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Configuration: ");
    if (loadDefaults) {
      toString(defaultResources, sb);
      if (resources.size() > 0) {
        sb.append(", ");
      }
    }
    toString(resources, sb);
    return sb.toString();
  }

  private <T> void toString(List<T> resources, StringBuilder sb) {
    ListIterator<T> i = resources.listIterator();
    while (i.hasNext()) {
      if (i.nextIndex() != 0) {
        sb.append(", ");
      }
      sb.append(i.next());
    }
  }

  /**
   * Set the quietness-mode.
   * 
   * In the quiet-mode, error and informational messages might not be logged.
   * 
   * @param quietmode <code>true</code> to set quiet-mode on, <code>false</code> to turn it off.
   */
  public synchronized void setQuietMode(boolean quietmode) {
    this.quietmode = quietmode;
  }

  synchronized boolean getQuietMode() {
    return this.quietmode;
  }

  /** For debugging. List non-default properties to the terminal and exit. */
  public static void main(String[] args) throws Exception {
    new Configuration().writeXml(System.out);
  }

}
