
== Checkstyle Installing ==

Checkstyle is used to enforce good programming style. 

Install Checkstyle version 4.4.3 (newer versions will not work!!!!!!!!!!!!!) by the following
steps:

In Eclipse click Help menu->Install New Software -> Add...
-> Local... -> Choose the com.atlassw.tools.eclipse.checkstyle_4.4.3-updatesite folder
and give it an arbitray name. -> follow the install wizard.

== Checkstyle Setting ==


2. Enable Custom GWT Checkstyle checks:

Copy "settings/code-style/gwt-customchecks.jar" into:
  <eclipse>/plugins/com.atlassw.tools.eclipse.checkstyle_4.4.3/extension-libraries
under linux, it should be 
  ~/.eclipse/plugins/com.atlassw.tools.eclipse.checkstyle_4.4.3/extension-libraries

Restart Eclipse.

-Shengliang
