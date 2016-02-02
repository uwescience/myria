---
layout: default
title: Myria Development
group: "docs"
section: 5
---

# Myria Development

This page explains how to get started with Myria development using GitHub, Git, 
and either Eclipse or IntelliJ.

## 1) GitHub setup

### Create an account
To participate in this project, you'll need a GitHub account. If you don't already have one, proceed to <https://github.com/signup/free>. Then [fork Myria](https://github.com/uwescience/myria/fork) to create a copy of the codebase in your account.

### Create and upload SSH public keys
Life is generally easier if you can use to pull (download) and push (upload) changes from/to GitHub without logging in every time. To do so, you'll want to create a public/private keypair and upload the public key to GitHub.

To create keys, on Mac OS X, use `ssh-keygen`. These instructions use the default path of `~/.ssh/id_rsa`, but you may wish to change this if you already have SSH keys set up for a different purpose. In that case, you can skip key generation and go ahead to uploading your public key to GitHub. Or you can create a new, GitHub-specific public/private keypair by adapting the instructions below.

    $ ssh-keygen
    Generating public/private rsa key pair.
    Enter file in which to save the key (/home/dhalperi/.ssh/id_rsa): [empty will pick the default]
    Enter passphrase (empty for no passphrase): [leave empty]
    Enter same passphrase again: [leave empty]
    Your identification has been saved in /home/dhalperi/.ssh/id_rsa.
    Your public key has been saved in /home/dhalperi/.ssh/id_rsa.pub.
    ...

Then get the public key

    $ cat .ssh/id_rsa.pub
    ssh-rsa AAAAB3NzaC1yc2EAAAABIwAAAQEAsMpYkp2spkMzjJi5iK0TETAkw1DfnDey93tXSs/pn30y
    IHsgs9BXnalUrriUZ48xaBlEF9lEiXQ1oq621LP1psTnhY2B6Uh0JKb1RyX8lGfjjD5shMUu69ceWbhU
    vWFtcHYZQeQAUmU+r1v26eIEvGG2fEtvxUItERrUoLrD7R5jgza3nGNxK2+3aVb+rIMdUpYHalsj8PU2
    ddDVe3VjURTSG1m42gALlbUAWnuUKaBIqVhiNikKaPFzAf1MCoa+LLBxzY5E9BpxUiqN4nxvYI2lb4QY
    0T/bRKsLDbrgR8iGnxyDTUsDmHs5TIe+xOIrRnySaZdeCJNFDMV/gMx7Pw== dhalperi@dsp

And add it as a new public key into GitHub's SSH settings here: <https://github.com/settings/ssh>

## 2) Git setup

The Myria project uses Git for content management. (You can probably tell this, given that you're on GitHub.com after all.) This means that your computer will need to have the `git` utility installed.

### Install the `git` utility
* On Mac OS X, you can obtain `git` by installing the XCode command line tools.
* On many flavors of Linux, you can obtain `git` via your package manager, usually under the package name `git-core`.
  * For instance, on  Ubuntu: `sudo apt-get install git-core`
* On Windows, one easy option is to install [Github for Windows](https://windows.github.com/).

### Configure your name and email address

    git config --global user.email dhalperi@cs.washington.edu
    git config --global user.name "Daniel Halperin"

### Configure Git's default push behavior

What you do here depends on the version of `git` you have.

1. Determine your `git` version with `git --version`

    a. if you have `git` version 1.7.11 or later, do this:

        git config --global push.default simple

    b. otherwise, do this:

        git config --global push.default current

This is a bit complicated to explain, but this command changes the default behavior of `git push` with no arguments to something safe and sane. Making this option not the default was a mistake, which will be rectified when "Git 2.0" is released. In the meantime, this is the workaround. See `git config --help` for more information.

## 3) Clone the code from GitHub

Navigate to your personal copy of the Myria codebase, usually at <https://github.com/uwescience/myria> where you substitute your user name for `uwescience`. On that project page, a prominent box in the top-center of the screen shows the repository address. Generally, it defaults to HTTP, but we want to use SSH so that we do not need to authenticate every time. (See step 1 above). Push the SSH button and then copy the resulting URL.

(The repository SSH URL should be `git@github.com:<your user name>/myria.git`, but go make sure you can find it yourself.)

Then you can clone it using the command line:

    $ git clone git@github.com:<your user name>/myria.git
    Cloning into 'myria'...
    remote: Counting objects: 2208, done.
    remote: Compressing objects: 100% (585/585), done.
    remote: Total 2208 (delta 1125), reused 2127 (delta 1054)
    Receiving objects: 100% (2208/2208), 18.34 MiB | 119 KiB/s, done.
    Resolving deltas: 100% (1125/1125), done.

Now you have the code set up locally!

## 4) Install an IDE

You will code much more happily by using an IDE, though any text editor will do the job.
If you don't already have a favorite IDE, here are setup instructions for two popular ones.

### Eclipse IDE

At the time of writing, Eclipse 4.2 (Juno) is the newest version. You can download it from <http://eclipse.org/downloads/>. Your best bet might be the Eclipse IDE for Java Developers <http://eclipse.org/downloads/packages/eclipse-ide-java-developers/junosr1>, but the Classic (and probably any other version) would work just fine as well.

### IntelliJ IDE

You can download the most recent version of IntelliJ from <https://www.jetbrains.com/idea/download/>.  Choose the free "Community Edition".

## 5) Install IDE Plugins
We use two plugins that help us find bugs and other problems in our code:

* FindBugs, from the PL group at the University of Maryland. 
* Checkstyle is an open source tool that helps us write good code. 

Here are their download links, including instructions on how to install plugins:

### Eclipse IDE
* FindBugs <http://findbugs.cs.umd.edu/eclipse/>
* Checkstyle <http://eclipse-cs.sourceforge.net/downloads.html>

### IntelliJ IDE
* FindBugs <https://plugins.jetbrains.com/plugin/3847>
* Checkstyle <https://plugins.jetbrains.com/plugin/1065>


## 6) Import the Myria project into your IDE

### Eclipse IDE
First, switch into the directory

    $ cd myria

Then use `gradlew` to make the Eclipse `.classpath` file ( `gradlew` is a `gradle` wrapper, use `./gradle.bat` if you are using Windows )

    $ ./gradlew eclipseClasspath
    :cleanEclipseClasspath
    :eclipseClasspath

    BUILD SUCCESSFUL

    Total time: 12.401 secs

Then in Eclipse, click through the menu 
`File > Import > Existing projects into workspace`
to open the Myria project.


### IntelliJ IDE
Goto "import project" in IntelliJ and open the "build.gradle" file in the `myria/` folder.
IntelliJ will recognize the project as a Gradle project and set it up automatically.

There is a Gradle menu at the right-hand side of the screen, 
from which you can run the Gradle build, test and other actions. 

## 7) Troubleshooting

If you are experiencing erratic behaviour or Eclipse with many errors, try the following:

### 1. Refresh the whole project. 

In Eclipse, click on the project `myria` and press `F5`.

### 2. Clean the project

In Eclipse, goto `Project > Clean`

In IntelliJ, goto the Gradle menu and activate the `clean` action.

### 3. Rebuild Eclipse classpath file

Re-type the command `./gradlew eclipseClasspath` as above.
