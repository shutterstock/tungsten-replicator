/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2011 Continuent Inc.
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
 * Initial developer(s): Seppo Jaakola
 * Contributor(s): Robert Hodges, Linas Virbalas
 */

package com.continuent.tungsten.replicator.management.script;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.NotificationBroadcasterSupport;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.cluster.resource.physical.ReplicatorCapabilities;
import com.continuent.tungsten.commons.config.TungstenProperties;
import com.continuent.tungsten.commons.exec.ProcessExecutor;
import com.continuent.tungsten.replicator.InSequenceNotification;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.backup.RestoreCompletionNotification;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime;
import com.continuent.tungsten.replicator.management.OpenReplicatorContext;
import com.continuent.tungsten.replicator.management.OpenReplicatorPlugin;
import com.continuent.tungsten.replicator.management.events.GoOfflineEvent;
import com.continuent.tungsten.replicator.management.events.OfflineNotification;

/**
 * This class defines an OpenReplicatorPlugin that invokes an external program
 * to control replication.
 *
 * @author <a href="mailto:seppo.jaakola@continuent.com">Seppo Jaakola</a>
 * @version 1.0
 */
public class ScriptPlugin extends NotificationBroadcasterSupport
        implements
            OpenReplicatorPlugin
{
    private static final String   CMD_OPERATION        = "--operation";

    private static final String   CMD_ONLINE           = "online";
    private static final String   CMD_OFFLINE          = "offline";
    private static final String   CMD_OFFLINE_DEFERRED = "offline-deferred";
    private static final String   CMD_PROVISION        = "provision";
    private static final String   CMD_STATUS           = "status";
    private static final String   CMD_FLUSH            = "flush";
    private static final String   CMD_WAITEVENT        = "waitevent";
    private static final String   CMD_SETROLE          = "setrole";
    private static final String   CMD_CAPABILITIES     = "capabilities";

    private static final String   ARG_CONF_FILE        = "--config";
    private static final String   IN_PARAMS            = "--in-params";
    private static final String   OUT_PARAM_FILE       = "--out-params";

    private static final String   ARG_EVENT            = "event";
    private static final String   ARG_TIMEOUT          = "timeout";
    private static final String   ARG_ROLE             = "role";
    public static final String    ARG_URI              = "uri";

    private static Logger         logger               = Logger.getLogger(ScriptPlugin.class);

    private TungstenProperties    properties           = null;

    private ProcessExecutor       shell                = null;
    private OpenReplicatorContext context              = null;

    public ScriptPlugin()
    {
        shell = new ProcessExecutor();
    }

    private TungstenProperties runScript(String command,
            Map<String, String> argumentList) throws ReplicatorException
    {
        String conf = ARG_CONF_FILE
                + properties.getProperty(ReplicatorConf.SCRIPT_ROOT_DIR)
                + File.separator
                + properties.getProperty(ReplicatorConf.SCRIPT_CONF_FILE);
        String script = properties.getProperty(ReplicatorConf.SCRIPT_ROOT_DIR)
                + File.separator
                + properties.getProperty(ReplicatorConf.SCRIPT_PROCESSOR);
        File outputParams = null;
        FileInputStream paramsInputStream = null;

        String args = null;
        if (argumentList != null)
        {
            StringBuffer sb = new StringBuffer();
            for (String arg : argumentList.keySet())
            {
                String value = argumentList.get(arg);
                if (sb.length() > 0)
                {
                    sb.append(";");
                }
                sb.append(arg + "=" + value);
            }
            args = sb.toString();
        }

        String[] cmdBuffer = null;
        try
        {
            // Generate file to which script may write output values in Java
            // properties file format.
            outputParams = File.createTempFile("script-plugin-", ".properties");

            logger.debug("running script: " + script + " conf: " + conf
                    + " command: " + command + " in-params:" + args
                    + " out-params: " + outputParams.getAbsolutePath());

            shell.setWorkDirectory(new File(properties
                    .getProperty(ReplicatorConf.SCRIPT_ROOT_DIR)));
            if (args == null)
            {
                // cmdBuffer = new String[]{script, conf, command,
                // OUT_PARAM_FILE,
                // outputParams.getAbsolutePath()};
                cmdBuffer = new String[]{
                        script,
                        ARG_CONF_FILE,
                        properties.getProperty(ReplicatorConf.SCRIPT_ROOT_DIR)
                                + File.separator
                                + properties
                                        .getProperty(ReplicatorConf.SCRIPT_CONF_FILE),
                        CMD_OPERATION, command, OUT_PARAM_FILE,
                        outputParams.getAbsolutePath()};
            }
            else
            {
                cmdBuffer = new String[]{script, conf, command, IN_PARAMS,
                        args, OUT_PARAM_FILE, outputParams.getAbsolutePath()};
                cmdBuffer = new String[]{
                        script,
                        ARG_CONF_FILE,
                        properties.getProperty(ReplicatorConf.SCRIPT_ROOT_DIR)
                                + File.separator
                                + properties
                                        .getProperty(ReplicatorConf.SCRIPT_CONF_FILE),
                        CMD_OPERATION, command, IN_PARAMS, args,
                        OUT_PARAM_FILE, outputParams.getAbsolutePath()};
            }

            shell.setCommands(cmdBuffer);
            shell.setStdOut(logger);
            shell.setStdErr(logger);
            shell.run();

            int exitValue = shell.getExitValue();

            if (logger.isDebugEnabled() || !shell.isSuccessful())
            {
                logger.info("script exit code: " + exitValue);
            }

            // Load properties, if any, from script.
            TungstenProperties params = new TungstenProperties();
            if (outputParams.exists())
            {
                paramsInputStream = new FileInputStream(outputParams);
                params.load(paramsInputStream);
            }

            // Process any error now. Well-behaved scripts can write a
            // human-readable
            // message in the errmsg parameter. Otherwise we generate an error.
            if (!shell.isSuccessful())
            {
                String errMsg = params.getString("errmsg");
                if (errMsg == null)
                    errMsg = "Script plugin command failed: rc=" + exitValue;
                throw new ReplicatorException(errMsg);
            }

            return params;
        }
        catch (ReplicatorException e)
        {
            dumpCommand(cmdBuffer);
            logger.error("script plugin: " + script + " failed", e);
            throw e;
        }
        catch (Exception e)
        {
            dumpCommand(cmdBuffer);
            logger.error("script plugin: " + script + " failed", e);
            throw new ReplicatorException("Script plugin command failed " + e);
        }
        finally
        {
            // Delete properties file.
            if (outputParams != null)
                outputParams.delete();

            if (paramsInputStream != null)
            {
                try
                {
                    paramsInputStream.close();
                }
                catch (IOException e)
                {
                }
            }
        }
    }

    /**
     * Helper method for script calls without command arguments.
     */
    private TungstenProperties runScript(String cmd) throws ReplicatorException
    {
        return runScript(cmd, null);
    }

    /**
     * Dump operating system command to log.
     */
    private void dumpCommand(String[] cmdBuffer)
    {
        if (cmdBuffer != null)
        {
            StringBuffer sb = new StringBuffer(
                    "Operating system command array: \n");
            for (String value : cmdBuffer)
            {
                sb.append("\"").append(value).append("\" ");
            }
            logger.info(sb);
        }
    }

    /*
     * openReplicator plugin API implemented here
     */

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorPlugin#prepare(OpenReplicatorContext)
     */
    public void prepare(OpenReplicatorContext context)
            throws ReplicatorException
    {
        this.context = context;
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorPlugin#release()
     */
    public void release() throws ReplicatorException
    {
        // Nothing to do...
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorPlugin#configure(com.continuent.tungsten.commons.config.TungstenProperties)
     */
    public synchronized void configure(TungstenProperties properties)
            throws ReplicatorException
    {
        try
        {
            this.properties = properties;
        }
        catch (Exception e)
        {
            logger.error("script plugin configure failed: " + e);
            throw new ReplicatorException("script plugin configure failed: "
                    + e);
        }
    }

    /**
     * Puts the replicator into the goingonline state, which turns on
     * replication.
     */
    public void online(TungstenProperties params) throws Exception
    {
        // Params are not currently supported.
        runScript(CMD_ONLINE);

        /*
         * assuming the backend is ready by now, sending insequence notification
         */
        context.getEventDispatcher().handleEvent(new InSequenceNotification());
    }

    /**
     * Puts the replicator into the offline state, which turns off replication.
     */
    public void offline(TungstenProperties params) throws Exception
    {
        runScript(CMD_OFFLINE);
        /*
         * assuming the backend is offline, send notification
         */
        context.getEventDispatcher().handleEvent(new OfflineNotification());
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorPlugin#offlineDeferred(com.continuent.tungsten.commons.config.TungstenProperties)
     */
    public void offlineDeferred(TungstenProperties params) throws Exception
    {
        // Params are not currently supported - mapping to simple OFFLINE call
        // underneath.
        runScript(CMD_OFFLINE_DEFERRED);
        /*
         * assuming the backend is offline, send notification
         */
        context.getEventDispatcher().handleEvent(new GoOfflineEvent());
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorPlugin#heartbeat(com.continuent.tungsten.commons.config.TungstenProperties)
     */
    public boolean heartbeat(TungstenProperties params) throws Exception
    {
        // TODO Auto-generated method stub
        return false;
    }

    /**
     * Implements a flush operation to synchronize the state of the database
     * with the replication log and return a comparable event ID that can be
     * used in a wait operation on a slave.
     *
     * @param timeout Number of seconds to wait. 0 is indefinite.
     * @return The event ID at which the log is synchronized
     */
    public String flush(long timeout) throws Exception
    {
        TungstenProperties status = runScript(CMD_FLUSH);
        String eventID = status.getProperty("appliedLastSeqno");
        return eventID;
    }

    /**
     * Wait for a particular event to be applied on the slave.
     *
     * @param event Event to wait for
     * @param timeout Number of seconds to wait. 0 is indefinite.
     * @return true if requested sequence number or greater applied, else false
     *         if the wait timed out
     * @throws Exception if there is a timeout or we are canceled
     */
    public boolean waitForAppliedEvent(String event, long timeout)
            throws Exception
    {
        HashMap<String, String> args = new HashMap<String, String>();
        args.put(ARG_EVENT, event);
        args.put(ARG_TIMEOUT, new Long(timeout).toString());
        runScript(CMD_WAITEVENT, args);
        return true;
    }

    /**
     * Returns the current replicator status as a set of name-value pairs. The
     * following values are defined for all replicators.
     */
    public HashMap<String, String> status() throws Exception
    {
        TungstenProperties status = runScript(CMD_STATUS);
        return status.hashMap();
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorPlugin#statusList(java.lang.String)
     */
    public List<Map<String, String>> statusList(String name) throws Exception
    {
        throw new ReplicatorException(
                "Detailed status lists are not supported for script plugins");
    }

    /**
     * Calls the provision method on the script. {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorPlugin#provision(java.lang.String)
     */
    public void provision(String uri) throws Exception
    {
        // Make sure URI is valid if specified.
        URI real_uri = null;
        if (uri != null)
            real_uri = new URI(uri);

        // Provision.
        HashMap<String, String> args = new HashMap<String, String>();
        args.put(ARG_URI, uri);
        runScript(CMD_PROVISION, args);

        // Send notification.
        context.getEventDispatcher().handleEvent(
                new RestoreCompletionNotification(real_uri));
    }

    public void consistencyCheck(String method, String schemaName,
            String tableName, int rowOffset, int rowLimit) throws Exception
    {
        throw new ReplicatorException("Consistency check is not supported");
    }

    /**
     * Sets the replicator role. {@inheritDoc}
     *
     * @see com.continuent.tungsten.replicator.management.OpenReplicatorPlugin#setRole(java.lang.String,
     *      java.lang.String)
     */
    public void setRole(String role, String uri) throws ReplicatorException
    {
        HashMap<String, String> args = new HashMap<String, String>();
        args.put(ARG_URI, uri);
        args.put(ARG_ROLE, role);
        runScript(CMD_SETROLE, args);
    }

    /**
     * Return the capabilities of this replicator plugin.
     */
    public ReplicatorCapabilities getCapabilities() throws Exception
    {
        TungstenProperties props = runScript(CMD_CAPABILITIES);
        return new ReplicatorCapabilities(props);
    }

    @Override
    public ReplicatorRuntime getReplicatorRuntime()
    {
        return null;
    }
}
