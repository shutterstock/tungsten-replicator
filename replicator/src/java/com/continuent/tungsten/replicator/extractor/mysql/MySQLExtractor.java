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
 * Contributor(s): Alex Yurchenko, Teemu Ollakka, Stephane Giron, Robert Hodges
 */

package com.continuent.tungsten.replicator.extractor.mysql;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.commands.FileCommands;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.FailurePolicy;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.DatabaseFactory;
import com.continuent.tungsten.replicator.database.MySQLOperationMatcher;
import com.continuent.tungsten.replicator.database.SqlOperation;
import com.continuent.tungsten.replicator.database.SqlOperationMatcher;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.LoadDataFileDelete;
import com.continuent.tungsten.replicator.dbms.LoadDataFileFragment;
import com.continuent.tungsten.replicator.dbms.LoadDataFileQuery;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.RowIdData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.DBMSEmptyEvent;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplOption;
import com.continuent.tungsten.replicator.event.ReplOptionParams;
import com.continuent.tungsten.replicator.extractor.ExtractorException;
import com.continuent.tungsten.replicator.extractor.RawExtractor;
import com.continuent.tungsten.replicator.extractor.mysql.conversion.LittleEndianConversion;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * This class defines a MySQLExtractor
 * 
 * @author <a href="mailto:seppo.jaakola@continuent.com">Seppo Jaakola</a>
 * @version 1.0
 */
public class MySQLExtractor implements RawExtractor
{
    private static Logger                   logger                  = Logger.getLogger(MySQLExtractor.class);

    private ReplicatorRuntime               runtime                 = null;
    private String                          host                    = "localhost";
    private int                             port                    = 3306;
    private String                          user                    = "root";
    private String                          password                = "";
    private boolean                         strictVersionChecking   = true;
    private boolean                         parseStatements         = true;

    /** Replicate from MySQL master using either binlog or client connection. */
    private static String                   MODE_MASTER             = "master";
    /** Replicate from MySQL relay logs on MySQL slave. */
    private static String                   MODE_SLAVE_RELAY        = "slave-relay";
    private String                          binlogMode              = MODE_MASTER;

    // Location of binlogs and pattern.
    private String                          binlogFilePattern       = "mysql-bin";
    private String                          binlogDir               = "/var/log/mysql";

    private boolean                         useRelayLogs            = false;
    private long                            relayLogWaitTimeout     = 0;
    private int                             relayLogRetention       = 3;
    private String                          relayLogDir             = null;
    private int                             serverId                = 1;

    private String                          url;

    private static long                     binlogPositionMaxLength = 10;
    BinlogPosition                          binlogPosition          = null;

    // Number of milliseconds to wait before checking log index for a missing
    // log-rotate event.
    private static long                     INDEX_CHECK_INTERVAL    = 60000;

    // SQL parser.
    SqlOperationMatcher                     sqlMatcher              = new MySQLOperationMatcher();

    private HashMap<Long, TableMapLogEvent> tableEvents             = new HashMap<Long, TableMapLogEvent>();

    private int                             transactionFragSize     = 0;
    private boolean                         fragmentedTransaction   = false;

    // Built-in task to manage relay logs.
    private RelayLogTask                    relayLogTask            = null;
    private Thread                          relayLogThread          = null;

    // Varchar type fields can be retrieved and stored in THL either using
    // String datatype or bytes arrays. By default, using string datatype.
    private boolean                         useBytesForStrings      = false;

    // If true this means we are taking over for MySQL slave replication and can
    // position from the MySQL slave when starting for the first time.
    private boolean                         nativeSlaveTakeover     = false;

    // Should schema name be prefetched when a Load Data Infile Begin event is
    // extracted ?
    private boolean                         prefetchSchemaNameLDI   = true;

    private HashMap<Integer, String>        loadDataSchemas;

    private String                          jdbcHeader;

    private int                             bufferSize              = 32768;

    public String getHost()
    {
        return host;
    }

    public void setHost(String host)
    {
        this.host = host;
    }

    public int getPort()
    {
        return port;
    }

    public void setPort(int port)
    {
        this.port = port;
    }

    public String getUser()
    {
        return user;
    }

    public void setUser(String user)
    {
        this.user = user;
    }

    public String getPassword()
    {
        return password;
    }

    public void setPassword(String password)
    {
        this.password = password;
    }

    public String getBinlogFilePattern()
    {
        return binlogFilePattern;
    }

    public void setBinlogFilePattern(String binlogFilePattern)
    {
        this.binlogFilePattern = binlogFilePattern;
    }

    public String getBinlogDir()
    {
        return binlogDir;
    }

    public void setBinlogDir(String binlogDir)
    {
        this.binlogDir = binlogDir;
    }

    public boolean isStrictVersionChecking()
    {
        return strictVersionChecking;
    }

    public void setStrictVersionChecking(boolean strictVersionChecking)
    {
        this.strictVersionChecking = strictVersionChecking;
    }

    public boolean isParseStatements()
    {
        return parseStatements;
    }

    public void setParseStatements(boolean parseStatements)
    {
        this.parseStatements = parseStatements;
    }

    public boolean isUsingBytesForString()
    {
        return useBytesForStrings;
    }

    public void setUsingBytesForString(boolean useBytes)
    {
        this.useBytesForStrings = useBytes;
    }

    public boolean isUseRelayLogs()
    {
        return useRelayLogs;
    }

    public void setUseRelayLogs(boolean useRelayDir)
    {
        this.useRelayLogs = useRelayDir;
    }

    public long getRelayLogWaitTimeout()
    {
        return relayLogWaitTimeout;
    }

    public void setRelayLogWaitTimeout(long relayLogWaitTimeout)
    {
        this.relayLogWaitTimeout = relayLogWaitTimeout;
    }

    public int getRelayLogRetention()
    {
        return relayLogRetention;
    }

    public void setRelayLogRetention(int relayLogRetention)
    {
        this.relayLogRetention = relayLogRetention;
    }

    public String getRelayLogDir()
    {
        return relayLogDir;
    }

    public void setRelayLogDir(String relayLogDir)
    {
        this.relayLogDir = relayLogDir;
    }

    public int getServerId()
    {
        return serverId;
    }

    public void setServerId(int serverId)
    {
        this.serverId = serverId;
    }

    public int getTransactionFragSize()
    {
        return transactionFragSize;
    }

    public void setTransactionFragSize(int transactionFragSize)
    {
        this.transactionFragSize = transactionFragSize;
    }

    public String getJdbcHeader()
    {
        return jdbcHeader;
    }

    public void setJdbcHeader(String jdbcHeader)
    {
        this.jdbcHeader = jdbcHeader;
    }

    public String getBinlogMode()
    {
        return binlogMode;
    }

    public void setBinlogMode(String binlogMode)
    {
        this.binlogMode = binlogMode;
    }

    public void setBufferSize(int size)
    {
        bufferSize = size;
    }

    /*
     * Read binlog file header and verify it.
     */
    private void check_header(BinlogPosition position)
            throws ReplicatorException
    {

        byte header[] = new byte[MysqlBinlog.BIN_LOG_HEADER_SIZE];
        byte buf[] = new byte[MysqlBinlog.PROBE_HEADER_LEN];
        long tmp_pos = 0;

        FormatDescriptionLogEvent description_event = new FormatDescriptionLogEvent(
                3);
        try
        {
            if (position.getFileInputStream().read(header) != header.length)
            {
                throw new MySQLExtractException(
                        "Failed reading header;  Probably an empty file");
            }
            tmp_pos = header.length;
        }
        catch (IOException e1)
        {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        if (!Arrays.equals(header, MysqlBinlog.BINLOG_MAGIC))
        {
            throw new MySQLExtractException(
                    "File is not a binary log file - found : "
                            + LogEvent.hexdump(header) + " / expected : "
                            + LogEvent.hexdump(MysqlBinlog.BINLOG_MAGIC));
        }

        /*
         * Imagine we are running with --start-position=1000. We still need to
         * know the binlog format's. So we still need to find, if there is one,
         * the Format_desc event, or to know if this is a 3.23 binlog. So we
         * need to first read the first events of the log, those around offset
         * 4. Even if we are reading a 3.23 binlog from the start (no
         * --start-position): we need to know the header length (which is 13 in
         * 3.23, 19 in 4.x) to be able to successfully print the first event
         * (Start_log_event_v3). So even in this case, we need to "probe" the
         * first bytes of the logbefore we do a real read_log_event(). Because
         * read_log_event() needs to know the header's length to work fine.
         */
        for (;;)
        {
            try
            {
                /* mark a savepoint for further reset call */
                position.getFileInputStream().mark(2048);

                position.getDataInputStream().readFully(buf);
                tmp_pos += buf.length;

                logger.debug("buf[4]=" + buf[4]);
                long start_position = 0;
                /* always test for a Start_v3, even if no --start-position */
                if (buf[4] == MysqlBinlog.START_EVENT_V3)
                {
                    /* This is 3.23 or 4.x */
                    if (LittleEndianConversion.convert4BytesToLong(buf,
                            MysqlBinlog.EVENT_LEN_OFFSET) < (MysqlBinlog.LOG_EVENT_MINIMAL_HEADER_LEN + MysqlBinlog.START_V3_HEADER_LEN))
                    {
                        /* This is 3.23 (format 1) */
                        description_event = new FormatDescriptionLogEvent(1);
                    }
                    break;
                }
                else if (tmp_pos >= start_position)
                    break;
                else if (buf[4] == MysqlBinlog.FORMAT_DESCRIPTION_EVENT)
                {
                    /* This is 5.0 */
                    FormatDescriptionLogEvent new_description_event;
                    position.getFileInputStream().reset(); /*
                                                            * seek back to
                                                            * event's start
                                                            */
                    new_description_event = (FormatDescriptionLogEvent) LogEvent
                            .readLogEvent(runtime, position, description_event,
                                    parseStatements, useBytesForStrings,
                                    prefetchSchemaNameLDI);
                    if (new_description_event == null)
                    /* EOF can't be hit here normally, so it's a real error */
                    {
                        throw new MySQLExtractException(
                                "Could not read a Format_description_log_event event "
                                        + "at offset "
                                        + tmp_pos
                                        + "this could be a log format error or read error");
                    }
                    description_event = new_description_event;
                    logger.debug("Setting description_event");
                }
                else if (buf[4] == MysqlBinlog.ROTATE_EVENT)
                {
                    LogEvent ev;
                    position.getFileInputStream().reset(); /*
                                                            * seek back to
                                                            * event's start
                                                            */
                    ev = LogEvent.readLogEvent(runtime, position,
                            description_event, parseStatements,
                            useBytesForStrings, prefetchSchemaNameLDI);
                    if (ev == null)
                    /* EOF can't be hit here normally, so it's a real error */
                    {
                        throw new MySQLExtractException(
                                "Could not read a Rotate_log_event event "
                                        + "at offset "
                                        + tmp_pos
                                        + " this could be a log format error or "
                                        + "read error");
                    }
                }
                else
                {
                    break;
                }
            }
            catch (EOFException e)
            {
                /*
                 * Otherwise this is just EOF : this log currently contains 0-2
                 * events. Maybe it's going to be filled in the next
                 * milliseconds; then we are going to have a problem if this a
                 * 3.23 log (imagine we are locally reading a 3.23 binlog which
                 * is being written presently): we won't know it in
                 * read_log_event() and will fail(). Similar problems could
                 * happen with hot relay logs if --start-position is used (but a
                 * --start-position which is posterior to the current size of
                 * the log). These are rare problems anyway (reading a hot log +
                 * when we read the first events there are not all there yet +
                 * when we read a bit later there are more events + using a
                 * strange --start-position).
                 */
                break;
            }
            catch (IOException e)
            {
                throw new MySQLExtractException(
                        "Could not read entry at offset " + tmp_pos
                                + " : Error in log format or read error", e);
            }
        }
        try
        {
            position.getFileInputStream().reset();
        }
        catch (IOException e)
        {
            throw new MySQLExtractException("Error while resetting stream", e);
        } /* seek back to event's start */
    }

    private LogEvent processFile(BinlogPosition position)
            throws ReplicatorException, InterruptedException
    {
        try
        {
            if (position.getFileInputStream() == null)
            {
                position.openFile();
            }
            if (logger.isDebugEnabled())
                logger.debug("extracting from pos, file: "
                        + position.getFileName() + " pos: "
                        + position.getPosition());
            if (position.getFileInputStream() != null)
            {
                long indexCheckStart = System.currentTimeMillis();
                // read from the ready stream
                while (position.getDataInputStream().available() == 0)
                {
                    // TREP-301 - If we are waiting at the end of the file we
                    // must check that we are not reading a log file that is
                    // missing a log-rotate record.
                    if (System.currentTimeMillis() - indexCheckStart > INDEX_CHECK_INTERVAL)
                    {
                        BinlogIndex bi = new BinlogIndex(binlogDir,
                                binlogFilePattern, true);
                        File nextBinlog = bi.nextBinlog(position.getFileName());
                        if (nextBinlog != null)
                        {
                            // We are stuck at the tail of one binlog with more
                            // to follow. Generate and return fake log-rotate
                            // event.
                            logger.warn("Current log file appears to be missing log-rotate event: "
                                    + position.getFileName());
                            logger.info("Auto-generating log-rotate event for next binlog file: "
                                    + nextBinlog.getName());
                            return new RotateLogEvent(nextBinlog.getName());
                        }

                        // Ensure relay logs are running.
                        assertRelayLogsEnabled();

                        // Update index check time.
                        indexCheckStart = System.currentTimeMillis();
                    }

                    // Sleep for a while.
                    Thread.sleep(10);
                }
                /* TODO assume version 5.0 log */
                FormatDescriptionLogEvent description_event = new FormatDescriptionLogEvent(
                        4);

                if (position.getPosition() == 0)
                {
                    check_header(position);
                    position.setPosition(0);
                    position.openFile();
                    byte[] buf = new byte[MysqlBinlog.BIN_LOG_HEADER_SIZE];
                    position.getDataInputStream().readFully(buf);
                    position.setPosition(MysqlBinlog.BIN_LOG_HEADER_SIZE);
                }

                LogEvent event = LogEvent.readLogEvent(runtime, position,
                        description_event, parseStatements, useBytesForStrings,
                        prefetchSchemaNameLDI);
                position.setEventID(position.getEventID() + 1);

                return event;

            }
            else
            {
                throw new MySQLExtractException("binlog file channel not open");
            }
        }
        catch (IOException e)
        {
            throw new MySQLExtractException("binlog file read error", e);
        }
    }

    /*
     * Return BinlogPosition in String representation. This serves as EventId
     * for DBMSEvent.
     */
    private static String getDBMSEventId(BinlogPosition binlogPosition,
            long sessionId)
    {
        String fileName = binlogPosition.getFileName();
        String position = getPositionAsString(binlogPosition.getPosition());
        return String.format("%s:%s;%d", fileName, position, sessionId);
    }

    private static String getPositionAsString(long position)
    {
        return String.format("%0" + (binlogPositionMaxLength + 1) + "d",
                new Long(position));
    }

    /*
     * Find current position binlog and return BinlogPosition corresponding to
     * head of the newly opened log. If flush parameter is set, perform log
     * flushing as well
     */
    private BinlogPosition positionBinlogMaster(boolean flush)
            throws ReplicatorException
    {
        Database conn = null;
        Statement st = null;
        ResultSet rs = null;
        try
        {
            logger.info("Positioning from MySQL master current position");
            conn = DatabaseFactory.createDatabase(url, user, password);
            conn.connect();
            st = conn.createStatement();
            if (flush)
            {
                logger.debug("Flushing logs");
                st.executeUpdate("FLUSH LOGS");
            }
            logger.debug("Seeking head position in binlog");
            rs = st.executeQuery("SHOW MASTER STATUS");
            if (!rs.next())
                throw new ExtractorException(
                        "Error getting master status; is the MySQL binlog enabled?");
            String binlogFile = rs.getString(1);
            long binlogOffset = rs.getLong(2);

            // If we are using relay logs make sure that relay logging is
            // functioning here.
            if (useRelayLogs)
            {
                startRelayLogs(binlogFile, binlogOffset);
            }

            logger.info("Starting from master binlog position: " + binlogFile
                    + ":" + binlogOffset);
            return new BinlogPosition(binlogOffset, binlogFile, binlogDir,
                    binlogFilePattern, bufferSize);
        }
        catch (SQLException e)
        {
            logger.info("url: " + url + " user: " + user
                    + " password: ********");
            throw new ExtractorException(e);
        }
        finally
        {
            cleanUpDatabaseResources(conn, st, rs);
        }
    }

    /*
     * Find current position on running MySQL slave, stop the slave, and
     * position on the master binlog where the slave stopped.
     */
    private BinlogPosition positionFromSlaveStatus() throws ReplicatorException
    {
        Database conn = null;
        Statement st = null;
        ResultSet rs = null;
        try
        {
            logger.info("Positioning from MySQL slave current position");
            // Use local database to ensure we get the right slave information.
            String url = runtime.getJdbcUrl(null);
            String user = runtime.getJdbcUser();
            String password = runtime.getJdbcPassword();
            logger.info("Establishing connection to local DBMS to get slave info: url="
                    + url);
            conn = DatabaseFactory.createDatabase(url, user, password);
            conn.connect();
            st = conn.createStatement();
            // Stop the MySQL slave if it is currently running.
            logger.info("Stopping MySQL slave io and SQL threads if they are running");
            st.executeUpdate("STOP SLAVE");

            // Look for current binlog position.
            logger.debug("Seeking position in slave relay logs");
            rs = st.executeQuery("SHOW SLAVE STATUS");
            if (!rs.next())
                throw new ExtractorException(
                        "Error getting slave status; is MySQL replication enabled?");
            String binlogFile = rs.getString("Relay_Master_Log_File");
            long binlogOffset = rs.getLong("Exec_Master_Log_Pos");

            logger.info("Starting from position: " + binlogFile + ":"
                    + binlogOffset);
            return new BinlogPosition(binlogOffset, binlogFile, binlogDir,
                    binlogFilePattern, bufferSize);
        }
        catch (SQLException e)
        {
            logger.info("url: " + url + " user: " + user
                    + " password: ********");
            throw new ExtractorException(e);
        }
        finally
        {
            cleanUpDatabaseResources(conn, st, rs);
        }
    }

    /*
     * Extract single event from binlog. Note that this method assumes that
     * given position does not point in the middle of transaction.
     */
    private DBMSEvent extractEvent(BinlogPosition position)
            throws ReplicatorException, InterruptedException
    {

        boolean inTransaction = fragmentedTransaction;
        fragmentedTransaction = false;
        boolean autocommitMode = true;
        boolean doFileFragment = false;
        Timestamp startTime = null;

        long sessionId = 0;
        ArrayList<DBMSData> dataArray = new ArrayList<DBMSData>();

        boolean foundRowsLogEvent = false;
        LinkedList<ReplOption> savedOptions = new LinkedList<ReplOption>();

        try
        {
            String defaultDb = null;
            RowChangeData rowChangeData = null;
            long fragSize = 0;
            int serverId = -1;
            while (true)
            {
                BinlogPosition previousPosition = position.clone();
                DBMSEvent dbmsEvent = null;
                LogEvent logEvent = processFile(position);
                if (logEvent == null)
                {
                    logger.debug("Unknown binlog field, skipping");
                    continue;
                }

                if (serverId == -1)
                    serverId = logEvent.serverId;

                if (startTime == null)
                    startTime = logEvent.getWhen();

                if (logEvent instanceof RowsLogEvent)
                {
                    fragSize += ((RowsLogEvent) logEvent).getEventSize();
                }
                if (logger.isDebugEnabled())
                    logger.debug("Current fragment size=" + fragSize);

                /*
                 * TRANSACTION AGGREGATION PROBLEM We need this variable to
                 * determine when we commit SQL event - event must contain whole
                 * number of transactions (one of the requirements for parallel
                 * application on slaves), preferably 1. Now MySQL binlog
                 * behaves differently for different table types when in
                 * AUTOCOMMIT mode. Updates to InnoDB tables are always followed
                 * by explicit COMMIT event, updates to MyISAM - not. So we
                 * cannot rely solely on COMMIT events to denote the end of
                 * transaction and have to develop a complex heuristics to
                 * determine whether we wait for COMMIT event or replicate
                 * immediately.
                 */
                boolean doCommit = false;
                boolean doRollback = false;

                boolean unsafeForBlockCommit = false;

                if (logEvent.getClass() == QueryLogEvent.class)
                {
                    QueryLogEvent event = (QueryLogEvent) logEvent;
                    String queryString = event.getQuery();
                    StatementData statement = new StatementData(queryString);

                    // Extract the charset name if it can be found.
                    String charsetName = event.getCharsetName();
                    if (charsetName != null)
                        statement.addOption(ReplOptionParams.JAVA_CHARSET_NAME,
                                event.getCharsetName());
                    if (logger.isDebugEnabled())
                        logger.debug("Query extracted: " + queryString
                                + " charset=" + charsetName);

                    // Parse for SQL metadata and add to the statement.
                    String query;

                    if (!useBytesForStrings)
                        query = queryString;
                    else
                    {
                        // Translate only a few bytes, in order to eventually
                        // detect some keywords.
                        int len = Math.min(event.getQueryAsBytes().length, 200);
                        if (charsetName == null)
                            query = new String(event.getQueryAsBytes(), 0, len);
                        else
                            query = new String(event.getQueryAsBytes(), 0, len,
                                    charsetName);
                    }
                    SqlOperation sqlOperation = sqlMatcher.match(query);
                    statement.setParsingMetadata(sqlOperation);

                    // We must commit on DDLs and the like except for BEGIN or
                    // START TRANSACTION, since they start new transaction at
                    // the same time
                    doCommit = !inTransaction || sqlOperation.isAutoCommit();
                    int operation = sqlOperation.getOperation();
                    if (operation == SqlOperation.BEGIN)
                    {
                        inTransaction = true;
                        doCommit = false;
                        // This a a BEGIN statement : buffer session variables
                        // for following row events if any and skip it

                        /* Adding statement options */
                        savedOptions.add(new ReplOption("autocommit", event
                                .getAutocommitFlag()));
                        savedOptions.add(new ReplOption("sql_auto_is_null",
                                event.getAutoIsNullFlag()));
                        savedOptions.add(new ReplOption("foreign_key_checks",
                                event.getForeignKeyChecksFlag()));
                        savedOptions.add(new ReplOption("unique_checks", event
                                .getUniqueChecksFlag()));
                        savedOptions.add(new ReplOption("sql_mode", event
                                .getSqlMode()));
                        savedOptions.add(new ReplOption("character_set_client",
                                String.valueOf(event.getClientCharsetId())));
                        savedOptions.add(new ReplOption("collation_connection",
                                String.valueOf(event.getClientCollationId())));
                        savedOptions.add(new ReplOption("collation_server",
                                String.valueOf(event.getServerCollationId())));

                        if (event.getAutoIncrementIncrement() >= 0)
                            savedOptions.add(new ReplOption(
                                    "auto_increment_increment",
                                    String.valueOf(event
                                            .getAutoIncrementIncrement())));

                        if (event.getAutoIncrementOffset() >= 0)
                            savedOptions
                                    .add(new ReplOption(
                                            "auto_increment_offset",
                                            String.valueOf(event
                                                    .getAutoIncrementOffset())));

                        continue;
                    }

                    if (operation == SqlOperation.COMMIT)
                    {
                        // This is a COMMIT statement : dropping it for now
                        // Temporary workaround for TREP-243
                        doCommit = true;
                        inTransaction = !autocommitMode;
                    }
                    else if (operation == SqlOperation.ROLLBACK)
                    {
                        doRollback = true;
                        inTransaction = !autocommitMode;

                    }
                    else
                    {
                        // some optimisation: it makes sense to check for
                        // 'CREATE DATABASE' only if we know that it is not
                        // regular DML - this is a fix for TREP-52 - attempt
                        // to use DB which hasn't been created yet.
                        boolean isCreateOrDropDB = sqlOperation.getObjectType() == SqlOperation.SCHEMA;
                        boolean prependUseDb = !(sqlOperation.isAutoCommit() && isCreateOrDropDB);

                        if (defaultDb == null)
                        {
                            // first query in transaction
                            sessionId = event.getSessionId();
                            if (prependUseDb)
                            {
                                defaultDb = event.getDefaultDb();
                                statement.setDefaultSchema(defaultDb);
                            }
                        }
                        else
                        {
                            // check that session ID is the same
                            assert (sessionId == event.getSessionId());
                            // check if default database has changed
                            String newDb = event.getDefaultDb();
                            if (!defaultDb.equals(newDb) && prependUseDb)
                            {
                                defaultDb = newDb;
                                statement.setDefaultSchema(newDb);
                            }
                        }
                        if (isCreateOrDropDB)
                            statement.addOption(
                                    StatementData.CREATE_OR_DROP_DB, "");

                        if (operation == SqlOperation.CREATE
                                || operation == SqlOperation.DROP
                                || operation == SqlOperation.ALTER
                                || operation == SqlOperation.UNRECOGNIZED)
                            unsafeForBlockCommit = true;

                        statement.setTimestamp(event.getWhen().getTime());
                        if (!useBytesForStrings)
                        {
                            statement.setQuery(queryString);
                            fragSize += queryString.length();
                        }
                        else
                        {
                            byte[] bytes = event.getQueryAsBytes();
                            statement.setQuery(bytes);
                            fragSize += bytes.length;
                        }

                        /* Adding statement options */
                        statement.addOption("autocommit",
                                event.getAutocommitFlag());
                        statement.addOption("sql_auto_is_null",
                                event.getAutoIsNullFlag());
                        statement.addOption("foreign_key_checks",
                                event.getForeignKeyChecksFlag());
                        statement.addOption("unique_checks",
                                event.getUniqueChecksFlag());

                        if (event.getAutoIncrementIncrement() >= 0)
                            statement.addOption("auto_increment_increment",
                                    String.valueOf(event
                                            .getAutoIncrementIncrement()));

                        if (event.getAutoIncrementOffset() >= 0)
                            statement.addOption("auto_increment_offset", String
                                    .valueOf(event.getAutoIncrementOffset()));

                        /* Adding statement sql_mode */
                        statement.addOption("sql_mode", event.getSqlMode());

                        /* Adding character set / collation information */
                        statement.addOption("character_set_client",
                                String.valueOf(event.getClientCharsetId()));
                        statement.addOption("collation_connection",
                                String.valueOf(event.getClientCollationId()));
                        statement.addOption("collation_server",
                                String.valueOf(event.getServerCollationId()));
                        statement.setErrorCode(event.getErrorCode());

                        dataArray.add(statement);
                    }
                }
                else if (logEvent.getClass() == UserVarLogEvent.class)
                {
                    logger.debug("USER_VAR_EVENT detected: "
                            + ((UserVarLogEvent) logEvent).getQuery());
                    dataArray.add(new StatementData(
                            ((UserVarLogEvent) logEvent).getQuery()));

                }
                else if (logEvent.getClass() == RandLogEvent.class)
                {
                    logger.debug("RAND_EVENT detected: "
                            + ((RandLogEvent) logEvent).getQuery());
                    dataArray.add(new StatementData(((RandLogEvent) logEvent)
                            .getQuery()));
                }
                else if (logEvent.getClass() == IntvarLogEvent.class)
                {
                    IntvarLogEvent intvarLogEvent = (IntvarLogEvent) logEvent;
                    if (logger.isDebugEnabled())
                        logger.debug("INTVAR_EVENT detected, value: "
                                + intvarLogEvent.getValue() + " / type : "
                                + intvarLogEvent.getType());
                    /*
                     * For MySQL applying, we could have following SET
                     * statement: dataArray.add(new StatementData( "SET
                     * INSERT_ID= " + ((Intvar_log_event)
                     * logEvent).getValue()));
                     */
                    dataArray.add(new RowIdData(intvarLogEvent.getValue(),
                            intvarLogEvent.getType()));
                }
                else if (logEvent.getClass() == XidLogEvent.class)
                {
                    logger.debug("Commit extracted: "
                            + ((XidLogEvent) logEvent).getXid());
                    // If there's nothing to commit, just ignore.
                    // Should happen for InnoDB tables in AUTOCOMMIT mode.
                    if (!dataArray.isEmpty())
                    {
                        doCommit = true;
                    }
                    if (rowChangeData != null)
                    {
                        doCommit = true;
                    }
                    // It seems like there's always explicit COMMIT event if
                    // transaction is implicitely committed,
                    // but does transaction start implicitely?
                    inTransaction = !autocommitMode;

                    if (!doCommit)
                    {
                        logger.debug("Clearing Table Map events");
                        tableEvents.clear();
                        tableEvents = new HashMap<Long, TableMapLogEvent>();
                        return new DBMSEmptyEvent(getDBMSEventId(position,
                                sessionId));
                    }
                }
                else if (logEvent.getClass() == StopLogEvent.class)
                {
                    logger.debug("Stop event extracted: ");

                    // TUC-166. MySQL writes a stop event and closes the log
                    // when the MySQL daemon shuts down cleanly. It does not
                    // always mean the server is stopped now because we could
                    // be reading an old log file. We therefore ignore them
                    // and reread which makes us treat the file like a binlog
                    // with a missing ROTATE_LOG event.
                    String stopEventId = getDBMSEventId(previousPosition,
                            sessionId);
                    logger.info("Skipping over server stop event in log: "
                            + stopEventId);
                }
                else if (logEvent.getClass() == RotateLogEvent.class)
                {
                    String newBinlogFilename = ((RotateLogEvent) logEvent)
                            .getNewBinlogFilename();
                    logger.debug("Rotate log event: new binlog="
                            + newBinlogFilename);

                    // Slave relay logs have master rotate logs that we need
                    // to ignore. We detect these because they don't match the
                    // log file pattern.
                    if (MODE_SLAVE_RELAY.equals(binlogMode)
                            && !newBinlogFilename
                                    .startsWith(this.binlogFilePattern))
                    {
                        logger.info("Ignored superfluous master rotate log event: file="
                                + newBinlogFilename);
                    }
                    else
                    {
                        // It's real so we need to rotate the log.
                        position.reset();
                        position.setFileName(((RotateLogEvent) logEvent)
                                .getNewBinlogFilename());
                        position.openFile();
                        // Kick off an asynchronous scan for old relay logs.
                        if (useRelayLogs)
                            purgeRelayLogs(false);
                    }
                }
                else if (logEvent.getClass() == TableMapLogEvent.class)
                {
                    logger.debug("got table map event");
                    // remember last table map event
                    TableMapLogEvent tableEvent = (TableMapLogEvent) logEvent;
                    tableEvents.put(tableEvent.getTableId(), tableEvent);
                }
                else if (logEvent instanceof RowsLogEvent)
                {
                    if (logger.isDebugEnabled())
                        logger.debug("got rows log event - event size = "
                                + ((RowsLogEvent) logEvent).getEventSize());
                    rowChangeData = new RowChangeData();
                    RowsLogEvent rowsEvent = (RowsLogEvent) logEvent;
                    TableMapLogEvent tableEvent = tableEvents.get(rowsEvent
                            .getTableId());
                    rowsEvent.processExtractedEvent(rowChangeData, tableEvent);
                    dataArray.add(rowChangeData);
                    foundRowsLogEvent = true;
                }
                else if (logEvent instanceof BeginLoadQueryLogEvent)
                {
                    BeginLoadQueryLogEvent event = (BeginLoadQueryLogEvent) logEvent;
                    if (prefetchSchemaNameLDI)
                    {
                        if (loadDataSchemas == null)
                            loadDataSchemas = new HashMap<Integer, String>();
                        loadDataSchemas.put(Integer.valueOf(event.getFileID()),
                                event.getSchemaName());
                    }
                    dataArray.add(new LoadDataFileFragment(event.getFileID(),
                            event.getData(), event.getSchemaName()));
                    doFileFragment = true;
                }
                else if (logEvent instanceof AppendBlockLogEvent)
                {
                    AppendBlockLogEvent event = (AppendBlockLogEvent) logEvent;
                    String schema = null;
                    if (prefetchSchemaNameLDI && loadDataSchemas != null)
                        schema = loadDataSchemas.get(Integer.valueOf(event
                                .getFileID()));
                    dataArray.add(new LoadDataFileFragment(event.getFileID(),
                            event.getData(), schema));
                    doFileFragment = true;
                }
                else if (logEvent instanceof ExecuteLoadQueryLogEvent)
                {
                    ExecuteLoadQueryLogEvent event = (ExecuteLoadQueryLogEvent) logEvent;
                    if (loadDataSchemas != null)
                        loadDataSchemas.remove(Integer.valueOf(event
                                .getFileID()));
                    String queryString = event.getQuery();
                    LoadDataFileQuery statement = new LoadDataFileQuery(
                            queryString, event.getWhen().getTime(),
                            event.getDefaultDb(), event.getFileID(),
                            event.getStartPos(), event.getEndPos());
                    /* Adding statement options */
                    statement
                            .addOption("autocommit", event.getAutocommitFlag());
                    statement.addOption("sql_auto_is_null",
                            event.getAutoIsNullFlag());
                    statement.addOption("foreign_key_checks",
                            event.getForeignKeyChecksFlag());
                    statement.addOption("unique_checks",
                            event.getUniqueChecksFlag());

                    /* Adding statement sql_mode */
                    statement.addOption("sql_mode", event.getSqlMode());

                    /* Adding character set / collation information */
                    statement.addOption("character_set_client",
                            String.valueOf(event.getClientCharsetId()));
                    statement.addOption("collation_connection",
                            String.valueOf(event.getClientCollationId()));
                    statement.addOption("collation_server",
                            String.valueOf(event.getServerCollationId()));

                    // Extract the charset name if it can be found.
                    String charsetName = event.getCharsetName();
                    if (charsetName != null)
                        statement.addOption(ReplOptionParams.JAVA_CHARSET_NAME,
                                event.getCharsetName());

                    if (logger.isDebugEnabled())
                    {
                        logger.debug("statement.getOptions() = "
                                + statement.getOptions());
                    }
                    statement.setErrorCode(event.getErrorCode());
                    dataArray.add(statement);
                    doFileFragment = true;
                }
                else if (logEvent instanceof DeleteFileLogEvent)
                {
                    LoadDataFileDelete delete = new LoadDataFileDelete(
                            ((DeleteFileLogEvent) logEvent).getFileID());
                    dataArray.add(delete);
                }
                else
                {
                    logger.debug("got binlog event: " + logEvent);
                }

                if (doCommit || doRollback)
                {
                    logger.debug("Performing commit processing in extractor");

                    runtime.getMonitor().incrementEvents(dataArray.size());
                    String eventId = getDBMSEventId(position, sessionId);

                    dbmsEvent = new DBMSEvent(eventId, dataArray, startTime);
                    if (foundRowsLogEvent)
                        dbmsEvent.setOptions(savedOptions);

                    // Reset tableEvents hashtable when commit occurs
                    logger.debug("Clearing Table Map events");
                    tableEvents.clear();
                    savedOptions.clear();
                }
                else if (transactionFragSize > 0
                        && fragSize > transactionFragSize)
                {
                    if (logger.isDebugEnabled())
                        logger.debug("Fragmenting -- fragment size = "
                                + fragSize);
                    // Extracted event size reached the fragmentation size =>
                    // fragment event into several pieces.
                    // Please note that consistency check events should not be
                    // fragmented as they are not handled by the following code
                    runtime.getMonitor().incrementEvents(dataArray.size());
                    String eventId = getDBMSEventId(position, sessionId);
                    dbmsEvent = new DBMSEvent(eventId, dataArray, false,
                            startTime);
                    if (foundRowsLogEvent)
                        dbmsEvent.setOptions(savedOptions);

                    this.fragmentedTransaction = true;
                }
                else if (doFileFragment)
                {
                    doFileFragment = false;
                    runtime.getMonitor().incrementEvents(dataArray.size());
                    String eventId = getDBMSEventId(position, sessionId);
                    dbmsEvent = new DBMSEvent(eventId, dataArray, false,
                            startTime);
                    if (foundRowsLogEvent)
                        dbmsEvent.setOptions(savedOptions);

                }
                if (dbmsEvent != null)
                {
                    dbmsEvent.addMetadataOption(ReplOptionParams.SERVER_ID,
                            String.valueOf(serverId));
                    if (doRollback)
                        dbmsEvent.addMetadataOption(ReplOptionParams.ROLLBACK,
                                "");
                    if (unsafeForBlockCommit)
                        dbmsEvent.addMetadataOption(
                                ReplOptionParams.UNSAFE_FOR_BLOCK_COMMIT, "");
                    return dbmsEvent;
                }
            }
        }
        catch (ExtractorException e)
        {
            if (runtime.getExtractorFailurePolicy() == FailurePolicy.STOP)
                throw new ExtractorException("Failed to extract from "
                        + position, e);
            else
                logger.error("Failed to extract from " + position, e);

        }
        catch (InterruptedException e)
        {
            // We just pass this up the stack as we are being cancelled.
            throw e;
        }
        catch (Exception e)
        {
            if (runtime.getExtractorFailurePolicy() == FailurePolicy.STOP)
                throw new ExtractorException(
                        "Unexpected failure while extracting event " + position,
                        e);
            else
                logger.error("Unexpected failure while extracting event "
                        + position, e);

        }
        return null;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.RawExtractor#extract()
     */
    public synchronized DBMSEvent extract() throws InterruptedException,
            ReplicatorException
    {
        // If we are using relay logs make sure that relay logging is
        // functioning.
        assertRelayLogsEnabled();

        // Extract the next event.
        return extractEvent(binlogPosition);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.RawExtractor#extract(java.lang.String)
     */
    public DBMSEvent extract(String id) throws InterruptedException,
            ReplicatorException
    {
        setLastEventId(id);
        return extract();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.RawExtractor#setLastEventId(java.lang.String)
     */
    public void setLastEventId(String eventId) throws ReplicatorException
    {
        if (eventId != null)
        {
            logger.info("Starting from an explicit event ID: " + eventId);
            int colonIndex = eventId.indexOf(':');
            int semicolonIndex = eventId.indexOf(";");

            String binlogFileIndex = eventId.substring(0, colonIndex);

            int binlogOffset;

            if (semicolonIndex != -1)
            {
                binlogOffset = Integer.valueOf(eventId.substring(
                        colonIndex + 1, semicolonIndex));
            }
            else
            {
                binlogOffset = Integer.valueOf(eventId
                        .substring(colonIndex + 1));
            }

            // We tolerate the event ID with or without the binlog prefix.
            String binlogFile;
            if (binlogFileIndex.startsWith(binlogFilePattern))
                binlogFile = binlogFileIndex;
            else
                binlogFile = binlogFilePattern + "." + binlogFileIndex;

            // Set the binlog position.
            binlogPosition = new BinlogPosition(binlogOffset, binlogFile,
                    binlogDir, binlogFilePattern, bufferSize);
        }
        else
        {
            logger.info("Inferring event ID to start extraction");
            if (nativeSlaveTakeover)
            {
                // Try to position on the running slave if native slave takeover
                // is enabled.
                binlogPosition = positionFromSlaveStatus();
            }
            else
            {
                // If native slave takeover is not enabled, use the
                // current master position.
                binlogPosition = positionBinlogMaster(true);
            }
        }

        // If we are using relay logs make sure that relay logging is
        // functioning here and we are up to point required by binlog
        // position.
        startRelayLogs(binlogPosition.getFileName(),
                binlogPosition.getPosition());

        // Now open the binlog file.
        binlogPosition.openFile();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException
    {
        runtime = (ReplicatorRuntime) context;

        // Compute our MySQL dbms URL.
        StringBuffer sb = new StringBuffer();
        if (jdbcHeader == null)
            sb.append("jdbc:mysql://");
        else
            sb.append(jdbcHeader);
        sb.append(host);
        sb.append(":");
        sb.append(port);
        sb.append("/");
        url = sb.toString();

        // See if we are operating in native slave takeover mode.
        nativeSlaveTakeover = context.nativeSlaveTakeover();
        if (nativeSlaveTakeover)
            logger.info("Native slave takeover is enabled");

        // Prepare accordingly based on whether we are replicating from a master
        // or MySQL slave relay logs.
        if (MODE_MASTER.equals(binlogMode))
        {
            logger.info("Reading logs from MySQL master: binlogMode= "
                    + binlogMode);
            // If we have enabled Tungsten relay logging, we must use the relay
            // log directory instead of the normal binlog dir.
            if (this.useRelayLogs)
            {
                if (relayLogDir == null)
                {
                    throw new ReplicatorException(
                            "Relay logging is enabled but relay log directory is not set");
                }
                File relayLogs = new File(relayLogDir);

                if (!relayLogs.exists())
                {
                    logger.info("Relay log does not exist, creating: "
                            + relayLogs.getAbsolutePath());
                    if (!relayLogs.mkdirs())
                    {
                        throw new ExtractorException(
                                "Unable to create relay log directory: "
                                        + relayLogs.getAbsolutePath());
                    }
                }

                if (!relayLogs.canWrite())
                {
                    throw new ReplicatorException(
                            "Relay log directory does not exist or is not writable: "
                                    + relayLogs.getAbsolutePath());
                }
                logger.info("Using relay log directory as source of binlogs: "
                        + relayLogDir);
                binlogDir = relayLogDir;
            }
        }
        else if (MODE_SLAVE_RELAY.equals(binlogMode))
        {
            logger.info("Reading logs from MySQL slave: binlogMode= "
                    + binlogMode);
            // Check for conflicting settings.
            if (this.nativeSlaveTakeover)
                throw new ReplicatorException(
                        "nativeSlaveTakeover may not be true with this "
                                + "binlogMode setting: nativeSlaveTakeover="
                                + nativeSlaveTakeover + " binlogMode="
                                + binlogMode);
            if (this.useRelayLogs)
            {
                logger.warn("useRelayLogs setting is incompatible with "
                        + "binlogMode setting, hence ignored: useRelayLogs="
                        + useRelayLogs + " binlogMode=" + binlogMode);
                useRelayLogs = false;
            }
        }
        else
        {
            throw new ReplicatorException(
                    "Invalid binlogMode setting, must be " + MODE_MASTER
                            + " or " + MODE_SLAVE_RELAY + ": " + binlogMode);
        }
    }

    /**
     * If strictVersionChecking is enabled we ensure this database is a
     * supported version. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException
    {
        // NOTE: We can't check the database by default as unit tests depend
        // on being able to run without the server present. Also, we may in
        // future want to run on mirrored binlogs without the database.
        if (!strictVersionChecking)
        {
            logger.warn("MySQL start-up checks are disabled; binlog "
                    + "extraction may fail for unsupported versions "
                    + "or if InnoDB is not present");
            return;
        }

        // Proceed with database checks.
        Database conn = null;

        try
        {
            conn = DatabaseFactory.createDatabase(url, user, password);
            conn.connect();

            String version = getDatabaseVersion(conn);

            // For now only MySQL 5.0 and 5.1 are certified.
            if (version != null && version.startsWith("5"))
            {
                logger.info("Binlog extraction is supported for this MySQL version: "
                        + version);
            }
            else
            {
                logger.warn("Binlog extraction is not certified for this server version: "
                        + version);
                logger.warn("You may experience replication failures due to binlog incompatibilities");
            }

            getMaxBinlogSize(conn);

            checkInnoDBSupport(conn);
        }
        catch (SQLException e)
        {
            String message = "Unable to connect to MySQL server while preparing extractor; is server available?";
            message += "\n(url: " + url + " user: " + user
                    + " password: *********)";
            throw new ExtractorException(message, e);
        }
        finally
        {
            cleanUpDatabaseResources(conn, null, null);
        }
    }

    // Fetch the database version.
    private String getDatabaseVersion(Database conn) throws ReplicatorException
    {
        String version = null;
        Statement st = null;
        ResultSet rs = null;
        try
        {
            st = conn.createStatement();
            rs = st.executeQuery("SELECT @@VERSION");
            if (rs.next())
            {
                version = rs.getString(1);
            }
        }
        catch (SQLException e)
        {
            String message = "Unable to connect to MySQL server to check version; is server available?";
            message += "\n(url: " + url + " user: " + user
                    + " password: *********)";
            throw new ExtractorException(message, e);
        }
        finally
        {
            cleanUpDatabaseResources(null, st, rs);
        }

        return version;
    }

    // Fetch mysql 'max_binlog_size' setting.
    private void getMaxBinlogSize(Database conn) throws ReplicatorException
    {
        Statement st = null;
        ResultSet rs = null;
        try
        {
            st = conn.createStatement();
            rs = st.executeQuery("show variables like 'max_binlog_size'");
            if (rs.next())
            {
                binlogPositionMaxLength = rs.getString(1).length();
            }
        }
        catch (SQLException e)
        {
            String message = "Unable to connect to MySQL server to get max_binlog_size setting; is server available?";
            message += "\n(url: " + url + " user: " + user
                    + " password: *********)";
            throw new ExtractorException(message, e);
        }
        finally
        {
            cleanUpDatabaseResources(null, st, rs);
        }
    }

    private void checkInnoDBSupport(Database conn) throws ReplicatorException
    {
        Statement st = null;
        ResultSet rs = null;
        try
        {
            st = conn.createStatement();
            rs = st.executeQuery("show variables like 'have_innodb'");
            if (!rs.next()
                    || rs.getString(2).compareToIgnoreCase("disabled") == 0)
            {
                logger.warn("Warning! InnoDB support does not seem to be activated (check mysql have_innodb variable)");
            }
        }
        catch (SQLException e)
        {
            String message = "Unable to connect to MySQL server to check have_innodb setting; is server available?";
            message += "\n(url: " + url + " user: " + user
                    + " password: *********)";
            throw new ExtractorException(message, e);
        }
        finally
        {
            cleanUpDatabaseResources(null, st, rs);
        }
    }

    /** Starts relay logs. */
    private synchronized void startRelayLogs(String fileName, long offset)
            throws ReplicatorException
    {
        // Avoid pointless or harmful work.
        if (!useRelayLogs)
            return;

        String startPosition = fileName + ":" + offset;

        // If the relay task is running, as could be the case when
        // repositioning, stop it now.
        stopRelayLogs();

        // Configure client and connect to the master server. Note that we
        // don't try to start from the requested offset or we would get a
        // partial binlog.
        RelayLogClient relayClient = new RelayLogClient();
        relayClient.setUrl(url);
        relayClient.setLogin(user);
        relayClient.setPassword(password);
        relayClient.setBinlogDir(binlogDir);
        relayClient.setBinlog(fileName);
        relayClient.setBinlogPrefix(binlogFilePattern);
        relayClient.setServerId(serverId);
        relayClient.connect();

        // Start the relay log task.
        relayLogTask = new RelayLogTask(relayClient);
        relayLogThread = new Thread(relayLogTask, "Relay Client " + host + ":"
                + port);
        relayLogThread.start();

        // Delay until the relay log opens the file and reaches the desired
        // position.
        logger.info("Waiting for relay log position to catch up to extraction position: "
                + startPosition);
        long startTime = System.currentTimeMillis();
        long maxEndTime;
        if (relayLogWaitTimeout > 0)
            maxEndTime = startTime + (relayLogWaitTimeout * 1000);
        else
            maxEndTime = Long.MAX_VALUE;

        int loopCount = 0;
        while (System.currentTimeMillis() < maxEndTime)
        {
            // Look for normal completion.
            RelayLogPosition position = relayLogTask.getPosition();
            if (position.hasReached(fileName, offset))
                break;

            // Look for unexpected termination.
            if (relayLogTask.isFinished())
                throw new ExtractorException(
                        "Relay log task failed while waiting for start position: "
                                + startPosition);

            // Failing either of the previous conditions, report back and
            // go to sleep.
            if (loopCount % 10 == 0)
                logger.info("Current relay log position: "
                        + position.toString());
            loopCount++;
            try
            {
                Thread.sleep(1000);
            }
            catch (InterruptedException e)
            {
                // This is not expected but could be harmful if ignored.
                throw new ExtractorException(
                        "Unexpected interruption while positioning binlog");
            }
        }

        // If we have timed out, throw an exception.
        if (System.currentTimeMillis() >= maxEndTime)
        {
            throw new ExtractorException(
                    "Timed out waiting for relay log to reach extraction position: "
                            + fileName + ":" + offset);
        }
    }

    /**
     * Purge old relay logs that have aged out past the number of retained
     * files.
     */
    private void purgeRelayLogs(boolean wait)
    {
        if (relayLogRetention > 1)
        {
            // Note the + 1 so we don't accidentally delete the binlog .index
            // file. That would be confusing though not necessarily bad.
            logger.info("Checking for old relay log files...");
            File logDir = new File(binlogDir);
            File[] filesToPurge = FileCommands.filesOverRetentionAndInactive(
                    logDir, binlogFilePattern, relayLogRetention + 1,
                    this.binlogPosition.getFileName());
            FileCommands.deleteFiles(filesToPurge, wait);
        }
    }

    /** Starts relay logs and ensures they are running. */
    private synchronized void assertRelayLogsEnabled()
            throws ReplicatorException, InterruptedException
    {
        // Start the relay log task if it has not been started.
        if (useRelayLogs)
        {
            if (relayLogTask == null)
            {
                // We must have a binlog position by time this is called.
                startRelayLogs(binlogPosition.getFileName(),
                        binlogPosition.getPosition());
            }
            else if (relayLogTask.isFinished())
                throw new ExtractorException(
                        "Relay log task has unexpectedly terminated; logs may not be accessible");
        }
    }

    /** Stops relay log operation. */
    private synchronized void stopRelayLogs()
    {
        if (relayLogTask == null || relayLogTask.isFinished())
            return;

        // Zap the thread.
        logger.info("Cancelling relay log thread");
        relayLogTask.cancel();
        relayLogThread.interrupt();
        try
        {
            relayLogThread.join(2000);
        }
        catch (InterruptedException e)
        {
            logger.warn("Interrupted while waiting for relay log task to complete");
        }

        // Clean up.
        if (relayLogTask.isFinished())
        {
            relayLogTask = null;
            relayLogThread = null;
        }
        else
            logger.warn("Unable to cancel relay log thread");
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException
    {
        stopRelayLogs();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.extractor.RawExtractor#getCurrentResourceEventId()
     */
    public String getCurrentResourceEventId() throws ReplicatorException,
            InterruptedException
    {
        Database conn = null;
        Statement st = null;
        ResultSet rs = null;
        try
        {
            conn = DatabaseFactory.createDatabase(url, user, password);
            conn.connect();
            st = conn.createStatement();
            logger.debug("Seeking head position in binlog");
            rs = st.executeQuery("SHOW MASTER STATUS");
            if (!rs.next())
                throw new ExtractorException(
                        "Error getting master status; is the MySQL binlog enabled?");
            String binlogFile = rs.getString(1);
            long binlogOffset = rs.getLong(2);

            String eventId = binlogFile + ":"
                    + getPositionAsString(binlogOffset);

            return eventId;
        }
        catch (SQLException e)
        {
            logger.info("url: " + url + " user: " + user
                    + " password: ********");
            throw new ExtractorException(
                    "Unable to run SHOW MASTER STATUS to find log position", e);
        }
        finally
        {
            cleanUpDatabaseResources(conn, st, rs);
        }
    }

    // Utility method to close result, statement, and connection objects.
    private void cleanUpDatabaseResources(Database conn, Statement st,
            ResultSet rs)
    {
        if (rs != null)
        {
            try
            {
                rs.close();
            }
            catch (SQLException ignore)
            {
            }
        }
        if (st != null)
        {
            try
            {
                st.close();
            }
            catch (SQLException ignore)
            {
            }
        }
        if (conn != null)
            conn.close();
    }

    public void setPrefetchSchemaNameLDI(boolean prefetchSchemaNameLDI)
    {
        this.prefetchSchemaNameLDI = prefetchSchemaNameLDI;
    }
}
