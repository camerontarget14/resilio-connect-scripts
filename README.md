# connect-scripts
Resilio Connect Scripts is a set of recipes and solutions for commonly done tasks in the Resilio Connect product

# Update sync.conf
This folder contains set of scripts and components necesssary to change your Agent's sync.conf file via Distribution job. Script also restarts agent service if necesary. The script does not care about the folder it runs into.

## Mac Agent Package
This folder contains scripts and files to create OS X package with sync.conf pre-packaged to automatically connect to selected Management Console.

## deploy_agent_mac.sh
This script allows to register agent of Mac as a LaunchDaemon and run under limited user accout.
