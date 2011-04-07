/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2009 Continuent Inc.
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
 * Initial developer(s): Edward Archibald
 * Contributor(s):
 */

package com.continuent.tungsten.commons.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;

import jline.ArgumentCompletor;
import jline.Completor;
import jline.ConsoleReader;
import jline.MultiCompletor;
import jline.NullCompletor;
import jline.SimpleCompletor;

import com.continuent.tungsten.commons.cluster.resource.ResourceState;
import com.continuent.tungsten.commons.cluster.resource.notification.ReplicationServiceNotification;
import com.continuent.tungsten.commons.cluster.resource.physical.DataSource;
import com.continuent.tungsten.commons.cluster.resource.physical.Replicator;
import com.continuent.tungsten.commons.config.TungstenProperties;

public class CLUtils implements Serializable
{
    /**
     *
     */
    private static final long   serialVersionUID = 1L;
    private static final String COMMAND_COMMIT   = "commit";
    private static final String COMMAND_QUIT     = "quit";
    private static final String COMMAND_ROLLBACK = "rollback";
    private static final String COMMAND_LIST     = "ls";

    private static final String NEWLINE          = "\n";

    public static String[] getInputTokens(ConsoleReader cr, String prompt,
            BufferedReader in) throws IOException
    {
        String inbuf = null;

        if (cr != null)
        {
            inbuf = cr.readLine(prompt);
        }
        else
        {
            System.out.print(prompt);
            inbuf = in.readLine();
        }
        if (inbuf == null)
        {
            CLUtils.println("\nExiting...");
            System.exit(0);
        }

        Vector<String> noBlanks = new Vector<String>();
        for (String token : inbuf.split("\\b"))
        {
            if (!token.trim().equals(""))
                noBlanks.add(token);
        }

        if (noBlanks.size() > 0)
        {
            return noBlanks.toArray(new String[noBlanks.size()]);
        }
        return null;
    }

    /**
     * Does a generic format of a TungstenProperties instance.
     *
     * @param name - a name to be associated with the properties
     * @param props - the TungstenProperties instance
     * @param header - an optional header that can be pre-pended to each
     *            property
     * @return a String with the aggregate formatted properties.
     */
    public static String formatPropertiesOld(String name,
            TungstenProperties props, String header, boolean wasModified)
    {
        String indent = "  ";
        StringBuilder builder = new StringBuilder();
        builder.append(header);

        builder.append(String.format("%s%s\n", name, modifiedSign(wasModified)));
        builder.append("{\n");
        Map<String, String> propMap = props.hashMap();
        for (String key : propMap.keySet())
        {
            builder.append(String.format("%s%s = %s\n", indent, key,
                    propMap.get(key)));
        }
        builder.append(String.format("}"));

        return builder.toString();
    }

    public static String formatProperties(String name,
            TungstenProperties props, String header, boolean wasModified)
    {
        Map<String, String> propMap = props.hashMap();
        return formatMap(name, propMap, "", header, wasModified);
    }

    /**
     * Does a generic format of a TungstenProperties instance.
     *
     * @param name - a name to be associated with the properties
     * @param props - the TungstenProperties instance
     * @param header - an optional header that can be pre-pended to each
     *            property
     * @return a String with the aggregate formatted properties.
     */
    public static String formatMapOld(String name, Map<String, String> props,
            String header, boolean wasModified)
    {
        String indent = "  ";
        StringBuilder builder = new StringBuilder();
        builder.append(header);

        builder.append(String.format("%s%s\n", name, modifiedSign(wasModified)));
        builder.append("{\n");

        for (String key : props.keySet())
        {
            builder.append(String.format("%s%s = %s\n", indent, key,
                    props.get(key)));
        }
        builder.append(String.format("}"));
        return builder.toString();
    }

    public static String formatMap(String name, Map<String, String> props,
            String header, boolean wasModified)
    {
        return formatMap(name, props, "", header, wasModified);
    }

    public static String formatMap(String name, Map<String, String> props,
            String indent, String header, boolean wasModified)
    {
        StringBuilder builder = new StringBuilder();

        TreeMap<String, String> sorted = new TreeMap<String, String>();
        sorted.putAll(props);
        for (String key : sorted.keySet())
        {
            Object value = props.get(key);
            builder.append(String.format("%s%s:%s\n", indent, key,
                    value.toString()));
        }

        Vector<String[]> results = new Vector<String[]>();
        results.add(new String[]{builder.toString()});

        return ResultFormatter.formatResults(name, null, results,
                ResultFormatter.DEFAULT_WIDTH, true, true);
    }

    public static String formatProperties(String name,
            TungstenProperties props, String header)
    {
        return formatProperties(name, props, header, false);
    }

    /**
     * @param dsProps
     * @param header
     * @param wasModified
     * @param printDetails
     * @param includeStatistics
     * @return a formatted string representing a datasource
     */
    public static String formatDsProps(TungstenProperties dsProps,
            String header, boolean wasModified, boolean printDetails,
            boolean includeStatistics)
    {
        return formatStatus(dsProps, null, null, header, wasModified,
                printDetails, includeStatistics);
    }

    /**
     * @param dsProps - datasource properties to format
     * @param replProps - formatted replicator status for the datasource
     * @param header - header to be inserted on each line
     * @param wasModified - indicates whether or not the datasource has been
     *            modified
     * @param printDetails - print details
     * @param includeStatistics - include statistics
     * @return a formatted string representing a datasource/replicator status
     */
    public static String formatStatus(TungstenProperties dsProps,
            TungstenProperties replProps, String header, boolean wasModified,
            boolean printDetails, boolean includeStatistics)
    {
        return formatStatus(dsProps, replProps, null, header, wasModified,
                printDetails, includeStatistics);
    }

    /**
     * @param dsProps - datasource properties to format
     * @param replProps - formatted replicator status for the datasource
     * @param dbProps - properties that represent the database server state
     * @param header - header to be inserted on each line
     * @param wasModified - indicates whether or not the datasource has been
     *            modified
     * @param printDetails - print details
     * @param includeStatistics - include statistics
     * @return a formatted string representing a datasource/replicator status
     */
    public static String formatStatus(TungstenProperties dsProps,
            TungstenProperties replProps, TungstenProperties dbProps,
            String header, boolean wasModified, boolean printDetails,
            boolean includeStatistics)
    {
        String indent = "  ";
        StringBuilder builder = new StringBuilder();
        builder.append(header);
        String progressInformation = "";
        String additionalInfo = "";

        if (replProps != null)
        {
            progressInformation = String.format("progress=%s",
                    replProps.getString(Replicator.APPLIED_LAST_SEQNO));

            if (dsProps.getString(Replicator.ROLE).equals("slave"))
            {
                additionalInfo = String.format(", %s, latency=%s",
                        progressInformation,
                        replProps.getString(DataSource.APPLIED_LATENCY));

            }
            else
            {
                additionalInfo = String.format(", %s", progressInformation);
            }
        }

        String state = dsProps.get(DataSource.STATE);
        String failureInfo = "";
        if (state.equals(ResourceState.FAILED.toString()))
        {
            failureInfo = String.format("(%s)",
                    dsProps.getString(DataSource.LASTERROR));
        }
        else if (state.equals(ResourceState.SHUNNED.toString()))
        {
            String lastError = dsProps.getString(DataSource.LASTERROR);
            String shunReason = dsProps.getString(DataSource.LASTSHUNREASON);
            if (shunReason == null)
                shunReason = "";

            if (!shunReason.equals("") && !shunReason.equals("NONE"))
            {
                if (lastError != null && !lastError.equals(""))
                    failureInfo = String.format("(%s AFTER %s)", shunReason,
                            lastError);
                else
                    failureInfo = String.format("(%s)", shunReason);
            }
            else
            {
                failureInfo = String.format("(%s)", lastError);
            }
        }

        String dsHeader = String.format("%s%s(%s:%s) %s",
                dsProps.getString(DataSource.NAME), modifiedSign(wasModified),
                dsProps.getString("role"), dsProps.getString("state"),
                failureInfo);

        // String dsHeader = String.format("%s", dsProps
        // .getString(DataSource.NAME));

        if (!printDetails)
        {
            int indentToUse = dsProps.getString(DataSource.NAME).length() + 1;
            builder.append(
                    ResultFormatter.makeSeparator(
                            ResultFormatter.DEFAULT_WIDTH, 1, true)).append(
                    NEWLINE);
            builder.append(ResultFormatter.makeRow((new String[]{dsHeader}),
                    ResultFormatter.DEFAULT_WIDTH, indentToUse, true, true));
            builder.append(
                    ResultFormatter.makeSeparator(
                            ResultFormatter.DEFAULT_WIDTH, 1, true)).append(
                    NEWLINE);
            builder.append(ResultFormatter.makeRow(new String[]{indent
                    + formatReplicatorProps(replProps, header, printDetails)},
                    ResultFormatter.DEFAULT_WIDTH, 0, true, true));

            String dbState = (dbProps != null
                    ? dbProps.getString("state")
                    : "UNKNOWN");
            String dbHeader = String.format("%sDATASERVER(state=%s)\n", indent,
                    dbState);
            builder.append(ResultFormatter.makeRow((new String[]{dbHeader}),
                    ResultFormatter.DEFAULT_WIDTH, indentToUse, true, true));
            builder.append(
                    ResultFormatter.makeSeparator(
                            ResultFormatter.DEFAULT_WIDTH, 1, true)).append(
                    NEWLINE);

            builder.append(NEWLINE);

            return builder.toString();
        }

        // DETAILS:
        builder.append(formatMap(dsHeader, dsProps.map(), "", "", false));

        if (replProps != null)
        {
            String replHeader = String.format(
                    "%s:REPLICATOR(role=%s, state=%s)",
                    replProps.getString("host"), replProps.getString("role"),
                    replProps.getString("state"));

            builder.append(formatMap(replHeader, replProps.map(), "", "  ",
                    false));

        }

        if (dbProps != null)
        {
            String dbHeader = String.format("%s:DATASERVER(state=%s)",
                    dsProps.getString("name"), dbProps.getString("state"));

            builder.append(formatMap(dbHeader, dbProps.map(), "", "  ", false));

        }

        builder.append(NEWLINE);
        return builder.toString();
    }

    @SuppressWarnings("unchecked")
    public static String formatReplicatorProps(TungstenProperties replProps,
            String header, boolean printDetails)
    {

        if (replProps == null)
        {
            return "REPLICATOR(STOPPED or MANAGER DOWN)";
        }
        String indent = "\t";
        StringBuilder builder = new StringBuilder();

        builder.append(String.format("REPLICATOR(host=%s, state=%s)", replProps
                .getString(Replicator.HOST), ReplicationServiceNotification
                .replicatorStateToResourceState(replProps.getString("state"))));

        Map<String, TungstenProperties> serviceProps = (Map<String, TungstenProperties>) replProps
                .getObject("serviceProperties");
        if (serviceProps != null)
        {
            for (String serviceName : serviceProps.keySet())
            {
                TungstenProperties props = serviceProps.get(serviceName);
                builder.append(NEWLINE)
                        .append("      ")
                        .append(String.format(
                                "%s(%s:%s:%s) progress=%d",
                                props.getString(Replicator.SIMPLE_SERVICE_NAME),
                                props.getString(Replicator.ROLE),
                                props.getString(Replicator.SERVICE_TYPE),
                                props.getString(Replicator.STATE),
                                props.getInt(Replicator.APPLIED_LAST_SEQNO)));
            }
        }

        TreeMap<String, String> sortedMap = new TreeMap<String, String>();
        sortedMap.putAll(replProps.map());

        if (!printDetails)
        {
            return builder.toString();
        }

        builder.append("\n").append(header);
        builder.append("{").append("\n");
        builder.append(header);
        builder.append(
                String.format("%shost = %s", indent,
                        replProps.getString("host"))).append("\n");
        builder.append(header);
        builder.append(
                String.format("%sminSeqNo = %s", indent,
                        replProps.getString("minSeqNo"))).append("\n");
        builder.append(header);
        builder.append(
                String.format("%smaxSeqNo = %s", indent,
                        replProps.getString("maxSeqNo"))).append("\n");
        builder.append(header);
        builder.append(
                String.format("%smasterUri = %s", indent,
                        replProps.getString("masterUri"))).append("\n");
        builder.append(header);
        builder.append(
                String.format("%suptimeSeconds = %s", indent,
                        replProps.getString("uptimeSeconds"))).append("\n");
        builder.append(header);
        builder.append(
                String.format("%spendingExceptionMessage = %s", indent,
                        replProps.getString("pendingExceptionMessage")))
                .append("\n");
        builder.append(header);
        builder.append(
                String.format("%spendingErrorCode = %s", indent,
                        replProps.getString("pendingErrorCode"))).append("\n");
        builder.append(header);
        builder.append(
                String.format("%spendingError = %s", indent,
                        replProps.getString("pendingError"))).append("\n");

        builder.append(header);
        builder.append(String.format("}"));
        return builder.toString();
    }

    static public String modifiedSign(boolean wasModified)
    {
        return ((wasModified ? "*" : ""));
    }

    static public void formatStatistics(TungstenProperties dsProps,
            StringBuilder builder, String header, String indent)
    {
        builder.append(
                String.format("%sactiveConnectionCount = %s", indent,
                        dsProps.getObject("activeConnectionCount"))).append(
                "\n");
        builder.append(header);
        builder.append(
                String.format("%sconnectionsCreatedCount = %s", indent,
                        dsProps.getObject("connectionsCreatedCount"))
                        .toString()).append("\n");
        builder.append(header);
        builder.append(
                String.format("%sstatementsCreatedCount = %s", indent,
                        dsProps.getObject("statementsCreatedCount")).toString())
                .append("\n");
        builder.append(header);
        builder.append(
                String.format("%spreparedStatementsCreatedCount = %s", indent,
                        dsProps.getObject("preparedStatementsCreatedCount"))
                        .toString()).append("\n");
        builder.append(header);
        builder.append(
                String.format("%scallableStatementsCreatedCount = %s", indent,
                        dsProps.getObject("callableStatementsCreatedCount"))
                        .toString()).append("\n");
        builder.append(header);

    }

    // Print a message to stdout.
    static public void println(String msg)
    {
        if (msg == null)
            return;

        if (msg.length() > 0)
            System.out.println(msg);
    }

    // Print a formatted message to stdout.
    static public void printf(String fmt, Object... args)
    {
        System.out.printf(fmt, args);
    }

    // Print a message to stdout without a newline
    static public void print(String msg)
    {
        if (msg.length() > 0)
            System.out.print(msg);
    }

    // Print an error.
    static public void error(String msg, Throwable t)
    {
        println("ERROR: " + msg);
        if (t != null)
            t.printStackTrace();
    }

    // Abort following a fatal error.
    static public void fatal(String msg, Throwable t)
    {
        println(msg);
        if (t != null)
            t.printStackTrace();
        System.exit(1);
    }

    public static void printDataService(
            Map<String, TungstenProperties> dataSourceProps, String[] args)
    {
        boolean printDetail = false;

        if (dataSourceProps == null)
        {
            System.out.println("CLUSTER UNAVAILABLE\n");
        }

        if (args.length > 1 && args[1].equals("-l"))
        {
            printDetail = true;
        }

        for (String dsName : dataSourceProps.keySet())
        {
            if (args.length >= 3)
            {
                if (!dsName.equals(args[2]))
                    continue;
            }

            printDataSource(dataSourceProps.get(dsName), "", printDetail);
        }

    }

    public static void printDataSource(TungstenProperties dsProperties,
            String header, boolean printDetails)
    {
        println(formatStatus(dsProperties, null, null, header, false,
                printDetails, printDetails));
    }

    public static String printArgs(String args[])
    {
        return printArgs(args, 0);

    }

    public static String printArgs(String args[], int startElement)
    {

        StringBuffer buf = new StringBuffer();

        for (int i = startElement; i < args.length; i++)
        {
            if (buf.length() > 0)
            {
                buf.append(" ");
            }
            buf.append(args[i]);
        }

        return buf.toString();
    }

    public static TungstenProperties editProperties(TungstenProperties props,
            boolean isNew, BufferedReader in) throws IOException
    {
        boolean wasModified = false;

        ConsoleReader newDSReader = new ConsoleReader();

        List<Completor> comps = new LinkedList<Completor>();

        // Add a choice for each property setting.
        // Complete with currently set value so user can edit it easily
        for (String key : props.hashMap().keySet())
        {
            List<Completor> completor = new LinkedList<Completor>();
            completor.add(new SimpleCompletor(key));
            completor.add(new SimpleCompletor(props.getString(key)));
            completor.add(new NullCompletor());
            comps.add(new ArgumentCompletor(completor));
        }

        // Add commit and rollback keywords
        comps.add(new SimpleCompletor(new String[]{COMMAND_COMMIT,
                COMMAND_ROLLBACK}));

        newDSReader.addCompletor(new MultiCompletor(comps));

        String[] args = null;

        while ((args = getInputTokens(
                newDSReader,
                String.format("edit %s> ", props.getString("name"),
                        CLUtils.modifiedSign(wasModified || isNew)), in)) != null)
        {
            if (COMMAND_QUIT.equals(args[0]))
            {
                if (wasModified)
                {
                    CLUtils.println("Please either commit or rollback changes before quitting");
                    continue;
                }
                break;
            }
            else if (COMMAND_COMMIT.equals(args[0]))
            {
                break;
            }
            else if (COMMAND_ROLLBACK.equals(args[0]))
            {
                props = null;
                break;
            }
            // list current properties
            else if (COMMAND_LIST.equals(args[0]))
            {
                // for (String key : dsProps.keySet())
                println(formatProperties(props.getString("name"), props, "",
                        wasModified));
            }
            // not enough args or key not present in the predefined settings
            else if (args.length != 2 || props.getString(args[0]) == null)
            {
                CLUtils.println("Usage: <attribute> <new value> (example: \"role master\")");
                CLUtils.println("       or use 'rollback' or 'commit' to complete your work");
            }
            // set a property
            else
            {
                props.setString(args[0], args[1]);
                wasModified = true;
                CLUtils.println(CLUtils.formatProperties(
                        props.getString("name"), props, "", wasModified));
            }
        }

        if (!wasModified && !isNew)
            return null;

        return props;
    }

    public static String[] appendArg(String args[], String newArg)
    {
        ArrayList<String> newArgs = new ArrayList<String>();
        for (String arg : args)
            newArgs.add(arg);

        newArgs.add(newArg);

        return newArgs.toArray(new String[newArgs.size()]);
    }

    public static String[] prependArg(String args[], String newArg)
    {
        ArrayList<String> newArgs = new ArrayList<String>();
        newArgs.add(newArg);
        for (String arg : args)
            newArgs.add(arg);

        return newArgs.toArray(new String[newArgs.size()]);
    }

    public static String listToString(List<Object> list)
    {
        StringBuilder builder = new StringBuilder();

        for (Object obj : list)
        {
            builder.append(String.format("%s\n", obj.toString()));
        }

        return builder.toString();
    }

    public static String collectionToString(Collection<Object> collection)
    {
        StringBuilder builder = new StringBuilder();

        for (Object obj : collection)
        {
            builder.append(String.format("%s\n", obj.toString()));
        }

        return builder.toString();
    }
}
