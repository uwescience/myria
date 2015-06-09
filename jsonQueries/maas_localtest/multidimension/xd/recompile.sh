cd /Users/maas/devlocal/myria
/Users/maas/devlocal/myria/myriadeploy/shutdownlocal.sh
/Users/maas/devlocal/myria/gradlew jar
cd /Users/maas/devlocal/myria/myriadeploy
/Users/maas/devlocal/myria/myriadeploy/update_myria_jar_only.py deployment.cfg.local
/Users/maas/devlocal/myria/myriadeploy/launchlocal.sh
