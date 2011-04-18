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
 * Initial developer(s): Stephane Giron
 * Contributor(s):
 */

package com.continuent.tungsten.enterprise.replicator.thl;

import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.config.TungstenProperties;
import com.continuent.tungsten.commons.exec.ArgvIterator;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntimeConf;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSFilteredEvent;
import com.continuent.tungsten.replicator.event.ReplEvent;
import com.continuent.tungsten.replicator.thl.JdbcTHLStorage;
import com.continuent.tungsten.replicator.thl.THLCommands;
import com.continuent.tungsten.replicator.thl.THLEvent;
import com.continuent.tungsten.replicator.thl.THLException;
import com.continuent.tungsten.replicator.thl.THLManagerCtrl;
import com.continuent.tungsten.replicator.thl.THLStorage;

/**
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class EnterpriseTHLManagerCtrl
        extends com.continuent.tungsten.replicator.thl.THLManagerCtrl
{
    private static Logger logger = Logger.getLogger(EnterpriseTHLManagerCtrl.class);

    private THLStorage    storageHandler;
    private String        logDir;
    private int           logFileSize;
    private boolean       useDiskStorage;
    private String        serializer;

    public EnterpriseTHLManagerCtrl(String configFile) throws Exception
    {
        super(configFile);
        TungstenProperties properties = super.readConfig();
        String storage = properties.getString("replicator.store.thl.storage",
                JdbcTHLStorage.class.getName(), true);

        if (storage.equals(DiskTHLStorage.class.getName()))
        {
            useDiskStorage = true;
            storageHandler = (THLStorage) Class.forName(storage).newInstance();
            logDir = properties.getString("replicator.store.thl.log_dir");
            serializer = properties
                    .getString("replicator.store.thl.event_serializer");
        }
        else
        {
            useDiskStorage = false;
        }

    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.thl.THLManagerCtrl#connect(boolean)
     */
    public void connect(boolean readOnly) throws THLException
    {
        println("Connecting to storage");
        if (useDiskStorage)
        {
            DiskTHLStorage storage = (DiskTHLStorage) storageHandler;
            try
            {
                // storage.setUrl(url);
                // storage.setUser(user);
                // storage.setPassword(password);
                storage.setLogDir(logDir);
                storage.setLogFileSize(logFileSize);
                if (serializer != null)
                    storage.setEventSerializer(serializer);
                storage.setReadOnly(readOnly);
                storage.prepare(null);
            }
            catch (ReplicatorException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            catch (InterruptedException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        }
        else
            super.connect();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.thl.THLManagerCtrl#disconnect()
     */
    @Override
    public void disconnect()
    {
        if (useDiskStorage)
        {
            DiskTHLStorage storage = (DiskTHLStorage) storageHandler;
            try
            {
                storage.release();
            }
            catch (ReplicatorException e)
            {
            }
            catch (InterruptedException e)
            {
            }
        }
        else
            super.disconnect();
    }

    /**
     * Queries THL for summary information.
     * 
     * @return Info holder
     * @throws THLException
     */
    public InfoHolder getInfo() throws THLException
    {
        if (useDiskStorage)
        {
            long min = storageHandler.getMinSeqno();
            long max = storageHandler.getMaxSeqno();
            long events = max - min + 1;
            if (max < 0)
                events = 0;
            return new THLManagerCtrl.InfoHolder(min, max, events, -1);
        }
        else
            return super.getInfo();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.thl.THLManagerCtrl#listEvents(java.lang.Long,
     *      java.lang.Long, java.lang.Long, boolean, String)
     */
    @Override
    public void listEvents(Long low, Long high, Long by, boolean pureSQL,
            String charset) throws THLException
    {
        if (useDiskStorage)
        {
            long lowIndex = 0;
            long minSeqno = storageHandler.getMinSeqno();
            if (low != null && low >= minSeqno)
                lowIndex = low;
            else
                lowIndex = minSeqno;

            Long highIndex;
            long maxSeqno = storageHandler.getMaxSeqno();
            if (high != null && high <= maxSeqno)
                highIndex = high;
            else
                highIndex = maxSeqno;

            long i = lowIndex;
            short fragno = 0;
            while (i <= highIndex)
            {
                THLEvent thlEvent = null;
                try
                {
                    thlEvent = storageHandler.find(i, fragno);
                }
                catch (InterruptedException e)
                {
                }
                if (!pureSQL)
                {
                    StringBuilder sb = new StringBuilder();
                    printHeader(sb, thlEvent);
                    print(sb.toString());
                }
                ReplEvent replEvent = thlEvent.getReplEvent();
                if (replEvent instanceof ReplDBMSEvent)
                {
                    ReplDBMSEvent event = (ReplDBMSEvent) replEvent;
                    StringBuilder sb = new StringBuilder();
                    printReplDBMSEvent(sb, event, pureSQL, charset);
                    print(sb.toString());
                    if (replEvent instanceof ReplDBMSFilteredEvent)
                    {
                        i = ((ReplDBMSFilteredEvent) replEvent).getSeqnoEnd();
                    }
                }
                else
                {
                    println("# " + replEvent.getClass().getName()
                            + ": not supported.");
                }
                // We need to list all fragments.
                if (thlEvent.getLastFrag())
                {
                    i++;
                    fragno = 0;
                }
                else
                {
                    fragno++;
                }
            }
        }
        else
            super.listEvents(low, high, by, pureSQL, charset);
    }

    /**
     * Purge THL events in the given seqno interval.
     * 
     * @param low Sequence number specifying the beginning of the range. Leave
     *            null to start from the very beginning of the table.
     * @param high Sequence number specifying the end of the range. Leave null
     *            to end at the very end of the table.
     * @return Number of deleted events
     * @throws THLException
     */
    public int purgeEvents(Long low, Long high, String before)
            throws THLException
    {
        try
        {
            return storageHandler.delete(low, high, before);
        }
        catch (InterruptedException e)
        {
            logger.warn("Delete operation was interrupted!");
        }
        return -1;
    }

    /**
     * Main method to run utility.
     * 
     * @param argv optional command string
     */
    public static void main(String argv[])
    {
        try
        {
            // Command line parameters and options.
            String configFile = null;
            String service = null;
            String command = null;
            String age = null;
            Long seqno = null;
            Long low = null;
            Long high = null;
            Long by = null;
            Boolean pureSQL = null;
            Boolean yesToQuestions = null;
            String fileName = null;
            String charsetName = null;

            // Parse command line arguments.
            argvIterator = new ArgvIterator(argv);
            String curArg = null;
            while (argvIterator.hasNext())
            {
                curArg = argvIterator.next();
                if ("-conf".equals(curArg))
                    configFile = argvIterator.next();
                else if ("-service".equals(curArg))
                    service = argvIterator.next();
                else if ("-seqno".equals(curArg))
                    seqno = Long.parseLong(argvIterator.next());
                else if ("-low".equals(curArg))
                    low = Long.parseLong(argvIterator.next());
                else if ("-high".equals(curArg))
                    high = Long.parseLong(argvIterator.next());
                else if ("-age".equals(curArg))
                    age = argvIterator.next();
                else if ("-by".equals(curArg))
                    by = Long.parseLong(argvIterator.next());
                else if ("-sql".equals(curArg))
                    pureSQL = true;
                else if ("-y".equals(curArg))
                    yesToQuestions = true;
                else if ("-charset".equals(curArg))
                {
                    charsetName = argvIterator.next();
                    if (!Charset.isSupported(charsetName))
                    {
                        println("Unsupported charset " + charsetName
                                + ". Using default.");
                        charsetName = null;
                    }
                }
                else if ("-file".equals(curArg))
                {
                    fileName = argvIterator.next();
                }
                else if (curArg.startsWith("-"))
                    fatal("Unrecognized option: " + curArg, null);
                else
                    command = curArg;
            }

            // Construct actual THLManagerCtrl and call methods based on a
            // parsed user input.
            if (command == null)
            {
                println("Command is missing!");
                printHelp();
                fail();
            }
            else if (THLCommands.HELP.equals(command))
            {
                printHelp();
                succeed();
            }

            // Use default configuration file in case user didn't specify one.
            if (configFile == null)
            {
                if (service == null)
                {
                    fatal("You must specify either a config file or a service name (-conf or -service)",
                            null);
                }
                else
                {
                    ReplicatorRuntimeConf runtimeConf = ReplicatorRuntimeConf
                            .getConfiguration(service);
                    configFile = runtimeConf.getReplicatorProperties()
                            .getAbsolutePath();
                }
            }

            if (THLCommands.INFO.equals(command))
            {
                EnterpriseTHLManagerCtrl thlManager = new EnterpriseTHLManagerCtrl(
                        configFile);
                thlManager.connect(true);

                InfoHolder info = thlManager.getInfo();
                println("min seq# = " + info.getMinSeqNo());
                println("max seq# = " + info.getMaxSeqNo());
                println("events = " + info.getEventCount());
                println("highest known replicated seq# = "
                        + info.getHighestReplicatedEvent());

                thlManager.disconnect();
            }
            else if (THLCommands.LIST.equals(command))
            {
                EnterpriseTHLManagerCtrl thlManager = new EnterpriseTHLManagerCtrl(
                        configFile);
                thlManager.connect(true);

                if (fileName != null)
                {
                    thlManager.listEvents(fileName, getBoolOrFalse(pureSQL),
                            charsetName);
                }
                else if (seqno == null)
                    thlManager.listEvents(low, high, by,
                            getBoolOrFalse(pureSQL), charsetName);
                else
                    thlManager.listEvents(seqno, seqno, by,
                            getBoolOrFalse(pureSQL), charsetName);

                thlManager.disconnect();
            }
            else if (THLCommands.PURGE.equals(command))
            {
                EnterpriseTHLManagerCtrl thlManager = new EnterpriseTHLManagerCtrl(
                        configFile);
                thlManager.connect(false);

                println("WARNING: The purge command will break replication if you delete all events or delete events that have not reached all slaves.");

                boolean confirmed = true;
                if (!getBoolOrFalse(yesToQuestions))
                {
                    confirmed = false;
                    println("Are you sure you wish to delete these events [y/N]?");
                    if (readYes())
                        confirmed = true;
                    else
                        println("Nothing done.");
                }
                if (confirmed)
                {
                    String log = "Deleting events where";
                    int deleted = 0;
                    if (seqno == null)
                    {
                        if (low != null)
                            log += " SEQ# >= " + low;
                        if (low != null && high != null)
                            log += " and";
                        if (high != null)
                            log += " SEQ# <=" + high;
                        if (age != null)
                            log += " age is older than " + age;
                        println(log);
                        deleted = thlManager.purgeEvents(low, high, age);
                    }
                    else
                    {
                        log += " SEQ# = " + seqno;
                        println(log);
                        deleted = thlManager.purgeEvents(seqno, seqno);
                    }
                    println("Deleted events: " + deleted);
                }

                thlManager.disconnect();
            }
            else if (THLCommands.SKIP.equals(command))
            {
                EnterpriseTHLManagerCtrl thlManager = new EnterpriseTHLManagerCtrl(
                        configFile);
                if (thlManager.useDiskStorage)
                {
                    println("SKIP operation is not supported when using disk THL storage.");
                    println("Please check skip options of the ONLINE command instead. ");
                    return;
                }

                thlManager.connect(true);

                println("WARNING: Skipping events may cause data inconsistencies with the master database.");

                boolean confirmed = true;
                if (!getBoolOrFalse(yesToQuestions))
                {
                    confirmed = false;
                    long toBeSkipped = -1;
                    if (seqno == null)
                        toBeSkipped = thlManager.getEventCount(low, high);
                    else
                        toBeSkipped = thlManager.getEventCount(seqno, seqno);
                    if (toBeSkipped == 0)
                        println("Nothing to skip.");
                    else
                    {
                        println("Are you sure you wish to skip " + toBeSkipped
                                + " events [y/N]?");
                        if (readYes())
                            confirmed = true;
                        else
                            println("Nothing done.");
                    }
                }
                if (confirmed)
                {
                    String log = "Skipping events where";
                    long skipped = 0;
                    if (seqno == null)
                    {
                        if (low != null)
                            log += " SEQ# >= " + low;
                        if (low != null && high != null)
                            log += " and";
                        if (high != null)
                            log += " SEQ# <=" + high;
                        println(log);
                        skipped = thlManager.skipEvents(low, high);
                    }
                    else
                    {
                        log += " SEQ# = " + seqno;
                        println(log);
                        skipped = thlManager.skipEvents(seqno, seqno);
                    }
                    println("Marked events as skipped: " + skipped);
                }

                thlManager.disconnect();
            }
            else if (command.equals("index"))
            {
                EnterpriseTHLManagerCtrl thlManager = new EnterpriseTHLManagerCtrl(
                        configFile);
                thlManager.connect(true);

                thlManager.printIndex();

                thlManager.disconnect();
            }
            else
            {
                println("Unknown command: '" + command + "'");
                printHelp();
                fail();
            }
        }
        catch (Throwable t)
        {
            fatal("Fatal error: " + t.getMessage(), t);
        }
    }

    private void printIndex()
    {
        if (!useDiskStorage)
        {
            println("Not implemented for other storage than disk based storage");
            return;
        }
        println(((DiskTHLStorage) storageHandler).getIndex());
    }

    private void listEvents(String fileName, boolean pureSQL, String charset)
            throws ReplicatorException, IOException, InterruptedException
    {
        if (!useDiskStorage)
        {
            println("Not implemented for other storage than disk based storage");
            return;
        }
        DiskTHLStorage storage = (DiskTHLStorage) storageHandler;
        LogFile file = storage.setFile(fileName);
        THLEvent thlEvent = null;
        while ((thlEvent = storage.readNextEvent(file)) != null)
        {
            if (!pureSQL)
            {
                StringBuilder sb = new StringBuilder();
                printHeader(sb, thlEvent);
                print(sb.toString());
            }
            ReplEvent replEvent = thlEvent.getReplEvent();
            if (replEvent instanceof ReplDBMSEvent)
            {
                ReplDBMSEvent event = (ReplDBMSEvent) replEvent;
                StringBuilder sb = new StringBuilder();
                printReplDBMSEvent(sb, event, pureSQL, charset);
                print(sb.toString());
            }
            else
            {
                println("# " + replEvent.getClass().getName()
                        + ": not supported.");
            }
        }
    }

    protected static void printHelp()
    {
        println("Replicator THL Manager");
        println("Syntax: thl [global-options] command [command-options]");
        println("Global options:");
        println("  -conf path                       - Path to replicator.properties. Default:    ");
        println("                                     " + defaultConfigPath);
        println("age_format: Seconds=s, Minutes=m, hours=h, days=d e.g \"2d 4h\" is 2 days + 4 hours");
        println("Commands and corresponding options:");
        println("  list [-low #] [-high #] [-by #]  - Dump THL events from low to high #.        ");
        println("       [-sql]                        Specify -sql to use pure SQL output only.  ");
        println("       [-charset <charset_name>]     Character set used for decoding, when needed.");
        println("                                     (only with row replication with using_bytes_for_string is set to true).");
        println("  list [-seqno #] [-sql]           - Dump the exact event by a given #.         ");
        println("       [-charset <charset_name>]     Character set used for decoding, when needed.");
        println("  list [-file <file_name>] [-sql]  - Dump the content of the given file (for Disk-based storage).");
        println("       [-charset <charset_name>]     Character set used for decoding, when needed.");
        println("  index                            - Display index of Disk-based storage        ");
        println("  purge [-low #] [-high #] [-age <age_format>] [-y]");
        println("                                   - Delete events within the given range earlier than the optional age.");
        println("  purge [-seqno #] [-y]            - Delete the exact event.                    ");
        println("                                     Use -y to answer yes to all questions.     ");
        println("  skip [-low #] [-high #] [-y]     - Mark a range of events to be skipped.      ");
        println("  skip [-seqno #] [-y]               Mark an event to be skipped.               ");
        println("  info                             - Display minimum, maximum sequence number   ");
        println("                                     and other summary.                         ");
        println("  help                             - Print this help information.               ");
    }
}
