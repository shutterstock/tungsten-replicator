/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2010 Continuent Inc.
 * Contact: tungsten@continuent.org
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of version 2 of the GNU General Public License as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA
 *
 * Initial developer(s): Robert Hodges
 * Contributor(s):
 */

package com.continuent.tungsten.enterprise.replicator.shard;

import java.rmi.ConnectException;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.List;

import javax.management.remote.JMXConnector;

import com.continuent.tungsten.commons.cluster.resource.logical.DataShard;
import com.continuent.tungsten.commons.exec.ArgvIterator;
import com.continuent.tungsten.commons.jmx.JmxManager;
import com.continuent.tungsten.commons.jmx.ServerRuntimeException;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.management.OpenReplicatorManager;
import com.continuent.tungsten.replicator.management.OpenReplicatorManagerMBean;

/**
 * Implements a utility similar to trepctl to manage shards.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class ShardManagerCtrl
{
    // Instance variables.
    private boolean                     verbose              = false;
    private boolean                     expectLostConnection = false;
    private ArgvIterator                argvIterator;
    private OpenReplicatorManagerMBean  manager              = null;
    private EnterpriseShardManagerMBean shardMgr             = null;
    private JMXConnector                conn                 = null;

    // JMX connection parameters.
    private String                      rmiHost;
    private int                         rmiPort;
    private String                      service;

    // Commands.
    private static String               CREATE               = "create";
    private static String               DROP                 = "drop";
    private static String               DISABLE              = "disable";
    private static String               ENABLE               = "enable";
    private static String               HELP                 = "help";
    private static String               LIST                 = "list";
    private static String               DETECT               = "detect";

    ShardManagerCtrl(String[] argv)
    {
        argvIterator = new ArgvIterator(argv);
    }

    private void printHelp()
    {
        println("Shard Manager Control Utility");
        println("Syntax:  [java " + ShardManagerCtrl.class.getName() + " \\");
        println("             [global-options] command [command-options]");
        println("Global Options:");
        println("\t-host name       - Host name of replicator [default: localhost]");
        println("\t-port number     - Port number of replicator [default: 10000]");
        println("\t-service name    - Name of replicator service [default: replicator]");
        println("\t-verbose         - Print verbose messages");
        println("Commands:");
        println("\tcreate -id id -enabled true/false - Create a new shard");
        println("\tdisable -id expr - Disable one or more shards");
        println("\tdrop -id expr     - Drop one or more shards");
        println("\tenable -id expr   - Enable one or more shards");
        println("\thelp             - Print help");
        println("\tlist             - List current shards");
        println("\tdetect           - Auto-detect new shards");
    }

    /**
     * Main method to run utility.
     * 
     * @param argv optional command string
     */
    public static void main(String argv[])
    {
        ShardManagerCtrl ctrl = new ShardManagerCtrl(argv);
        ctrl.go();
    }

    /**
     * Process replicator command.
     */
    public void go()
    {
        // Set defaults for properties.
        rmiHost = ReplicatorConf.RMI_DEFAULT_HOST;
        rmiPort = new Integer(System.getProperty(ReplicatorConf.RMI_PORT,
                ReplicatorConf.RMI_DEFAULT_PORT)).intValue();
        service = ReplicatorConf.RMI_DEFAULT_SERVICE_NAME;
        String command = null;

        // Parse global options and command.
        String curArg = null;
        try
        {
            while (argvIterator.hasNext())
            {
                curArg = argvIterator.next();
                if ("-host".equals(curArg))
                    rmiHost = argvIterator.next();
                else if ("-port".equals(curArg))
                    rmiPort = Integer.parseInt(argvIterator.next());
                else if ("-verbose".equals(curArg))
                    verbose = true;
                else if ("-service".equals(curArg))
                    service = argvIterator.next();
                else if (curArg.startsWith("-"))
                {
                    fatal("Unrecognized global option: " + curArg, null);
                }
                else
                {
                    command = curArg;
                    break;
                }
            }
        }
        catch (NumberFormatException e)
        {
            fatal("Bad numeric argument for " + curArg, null);
        }
        catch (ArrayIndexOutOfBoundsException e)
        {
            fatal("Missing value for " + curArg, null);
        }

        // Process help before trying to connect.
        if (command != null && command.equals(HELP))
        {
            printHelp();
            System.exit(0);
        }

        try
        {
            // Connect with appropriate protection against a lost connection.
            try
            {
                // Connect.
                conn = JmxManager.getRMIConnector(rmiHost, rmiPort, service);
                manager = (OpenReplicatorManagerMBean) JmxManager
                        .getMBeanProxy(conn, OpenReplicatorManager.class, false);
                shardMgr = (EnterpriseShardManagerMBean) JmxManager
                        .getMBeanProxy(conn, EnterpriseShardManager.class,
                                false);
            }
            catch (ServerRuntimeException e)
            {
                fatal("Connection failed: " + e, e);
            }

            if (command != null)
            {
                // TODO: is start command needed?
                if (command.equals(CREATE))
                    doCreate();
                else if (command.equals(DROP))
                    doDrop();
                else if (command.equals(DISABLE))
                    doDisable();
                else if (command.equals(ENABLE))
                    doEnable();
                else if (command.equals(LIST))
                    doList();
                else if (command.equals(DETECT))
                    doDetect();
                else
                {
                    println("Unknown command: '" + command + "'");
                    printHelp();
                }
            }

            // Check status assuming we didn't expect to lose the connection.
            if (manager.getPendingError() != null)
            {
                println("Error: " + manager.getPendingError());
                println("Exception Message: "
                        + manager.getPendingExceptionMessage());
            }
        }
        catch (ConnectException e)
        {
            // This occurs if JMX fails to connect via RMI.
            if (expectLostConnection)
                println("RMI connection lost!");
            else
                fatal("RMI connection lost!", e);
        }
        catch (RemoteException e)
        {
            // Occurs if there is an MBean server error, for example because
            // the server exited.
            if (expectLostConnection)
                println("Replicator appears to be stopped");
            else
            {
                fatal("Fatal RMI communication error: " + e.getMessage(), e);
            }
        }
        catch (Exception e)
        {
            // Occurs when there is a server-side application exception.
            println("Operation failed: " + e.getMessage());
            if (verbose)
                e.printStackTrace();
            if (manager.getPendingError() != null)
            {
                println("Error: " + manager.getPendingError());
                println("Exception Message: "
                        + manager.getPendingExceptionMessage());
            }
        }
        catch (Throwable t)
        {
            // Occurs if there is a really bad problem.
            fatal("Fatal error: " + t.getMessage(), t);
        }
    }

    // Handle create operation.
    private void doCreate() throws Exception
    {
        // Checks for params option.
        String id = null;
        boolean enabled = false;
        while (argvIterator.hasNext())
        {
            String curArg = argvIterator.next();
            try
            {
                if ("-id".equals(curArg))
                    id = argvIterator.next();
                else if ("-enabled".equals(curArg))
                    enabled = new Boolean(argvIterator.next());
                else
                    fatal("Unrecognized option: " + curArg, null);
            }
            catch (Exception e)
            {
                fatal("Missing or invalid argument to flag: " + curArg, null);
            }
        }

        // Create the shard.
        shardMgr.create(id, enabled);
    }

    // Handle drop operation.
    private void doDrop() throws Exception
    {
        // Checks for params option.
        String id = null;
        while (argvIterator.hasNext())
        {
            String curArg = argvIterator.next();
            try
            {
                if ("-id".equals(curArg))
                    id = argvIterator.next();
                else
                    fatal("Unrecognized option: " + curArg, null);
            }
            catch (Exception e)
            {
                fatal("Missing or invalid argument to flag: " + curArg, null);
            }
        }

        // Create the shard.
        shardMgr.drop(id);
    }

    // Handle disable operation.
    private void doDisable() throws Exception
    {
        // Checks for params option.
        String id = null;
        while (argvIterator.hasNext())
        {
            String curArg = argvIterator.next();
            try
            {
                if ("-id".equals(curArg))
                    id = argvIterator.next();
                else
                    fatal("Unrecognized option: " + curArg, null);
            }
            catch (Exception e)
            {
                fatal("Missing or invalid argument to flag: " + curArg, null);
            }
        }

        // Disable the shard.
        shardMgr.disable(id);
    }

    // Handle disable operation.
    private void doEnable() throws Exception
    {
        // Checks for params option.
        String id = null;
        while (argvIterator.hasNext())
        {
            String curArg = argvIterator.next();
            try
            {
                if ("-id".equals(curArg))
                    id = argvIterator.next();
                else
                    fatal("Unrecognized option: " + curArg, null);
            }
            catch (Exception e)
            {
                fatal("Missing or invalid argument to flag: " + curArg, null);
            }
        }

        // Enable the shard.
        shardMgr.enable(id);
    }

    // Handle list operation.
    private void doList() throws Exception
    {
        List<HashMap<String, String>> shards = shardMgr.list();
        printShards(shards);
    }

    // Handle detect operation.
    private void doDetect() throws Exception
    {
        // Checks for params option.
        String include = null;
        String exclude = null;
        boolean enabled = false;
        while (argvIterator.hasNext())
        {
            String curArg = argvIterator.next();
            try
            {
                if ("-include".equals(curArg))
                    include = argvIterator.next();
                else if ("-include".equals(curArg))
                    exclude = argvIterator.next();
                else if ("-enabled".equals(curArg))
                    enabled = new Boolean(argvIterator.next());
                else
                    fatal("Unrecognized option: " + curArg, null);
            }
            catch (Exception e)
            {
                fatal("Missing or invalid argument to flag: " + curArg, null);
            }
        }

        // Look for shards. 
        List<HashMap<String, String>> newShards = shardMgr.detect(include, exclude, enabled);
        printShards(newShards);
    }

    // Print a list of shards nicely. 
    private static void printShards(List<HashMap<String, String>> shards)
    {
        printf("%-30s  %-10s\n", "Shard ID", "State");
        printf("%-30s  %-10s\n", "--------", "-----");
        for (HashMap<String, String> shardInfo : shards)
        {
            String id = shardInfo.get(DataShard.NAME);
            String state = shardInfo.get(DataShard.STATE);
            printf("%-30s  %-10s\n", id, state);
        }
    }

    // Print a message to stdout.
    private static void println(String msg)
    {
        System.out.println(msg);
    }

    // Print a formatted message to stdout.
    private static void printf(String format, Object... args)
    {
        System.out.printf(format, args);
    }

    // Abort following a fatal error.
    private void fatal(String msg, Throwable t)
    {
        System.out.println(msg);
        if (verbose && t != null)
            t.printStackTrace();
        System.exit(1);
    }
}