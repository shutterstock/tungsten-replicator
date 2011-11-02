/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2011 Continuent Inc.
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
 * Contributor(s): Linas Virbalas
 */

package com.continuent.tungsten.replicator.applier.batch;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.csv.CsvException;
import com.continuent.tungsten.commons.csv.CsvWriter;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.applier.RawApplier;
import com.continuent.tungsten.replicator.consistency.ConsistencyException;
import com.continuent.tungsten.replicator.consistency.ConsistencyTable;
import com.continuent.tungsten.replicator.database.Column;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.DatabaseFactory;
import com.continuent.tungsten.replicator.database.Key;
import com.continuent.tungsten.replicator.database.PostgreSQLDatabase;
import com.continuent.tungsten.replicator.database.Table;
import com.continuent.tungsten.replicator.database.TableMetadataCache;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.LoadDataFileFragment;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.RowChangeData.ActionType;
import com.continuent.tungsten.replicator.dbms.RowIdData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.heartbeat.HeartbeatTable;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.thl.CommitSeqnoTable;

/**
 * Implements an applier that bulk loads data into a SQL database via CSV files.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class BatchApplier implements RawApplier
{
    private static Logger logger          = Logger.getLogger(BatchApplier.class);

    /**
     * Option for direct loading of inserts using COPY/LOAD DATA command and
     * temp tables for deletes.
     */
    public static String  DIRECT          = "direct";

    /**
     * Option for indirect loading of inserts and deletes via staging tables.
     */
    public static String  STAGED          = "staged";

    // Task management information.
    private int           taskId;

    // Properties.
    protected String      driver;
    protected String      url;
    protected String      user;
    protected String      password;
    protected String      stageDirectory;
    protected String      loadMethod;
    protected String      loadBatchTemplate;
    protected String      stageTablePrefix;
    protected String      stageDeleteFromTemplate;
    protected String      stageInsertFromTemplate;
    protected String      stagePkeyColumn;
    protected String      stageRowIdColumn;
    protected boolean     supportsReplace = false;
    protected String      startUpCommand;
    protected boolean     cleanUpFiles    = true;

    // Load file directory for this task.
    private File          stageDir;

    // Enum describing the different load methods.
    enum LoadMethod
    {
        direct, staged
    };

    private LoadMethod method;

    // Enum to distinguish batches of deletes vs inserts.
    enum BatchType
    {
        insert, delete
    };

    // Batch CSV file information. When using staging the
    // stage table metadata field is filled in. Otherwise it is null.
    class CsvInfo
    {
        BatchType type;
        String    schema;
        String    table;
        Table     baseTableMetadata;
        Table     stageTableMetadata;
        File      file;
        CsvWriter writer;
    }

    // Open CVS files in current transaction.
    private Map<String, CsvInfo> openCsvFiles         = new TreeMap<String, CsvInfo>();

    // Cached load commands.
    private Map<String, String>  loadCommands         = new HashMap<String, String>();

    // Latest event.
    private ReplDBMSHeader       latestHeader;

    // Table metadata for base tables.
    private TableMetadataCache   tableMetadataCache;

    // DBMS connection information.
    protected String             metadataSchema       = null;
    protected String             consistencyTable     = null;
    protected String             consistencySelect    = null;
    protected Database           conn                 = null;
    protected Statement          statement            = null;
    protected Pattern            ignoreSessionPattern = null;

    // Catalog tables.
    protected CommitSeqnoTable   commitSeqnoTable     = null;
    protected HeartbeatTable     heartbeatTable       = null;

    // Data formatter.
    SimpleDateFormat             dateFormatter;

    public synchronized void setDriver(String driver)
    {
        this.driver = driver;
    }

    public synchronized void setUrl(String url)
    {
        this.url = url;
    }

    public synchronized void setUser(String user)
    {
        this.user = user;
    }

    public synchronized void setPassword(String password)
    {
        this.password = password;
    }

    public synchronized void setLoadBatchTemplate(String loadBatchTemplate)
    {
        this.loadBatchTemplate = loadBatchTemplate;
    }

    public synchronized void setLoadMethod(String loadMethod)
    {
        this.loadMethod = loadMethod;
    }

    public synchronized void setStageTablePrefix(String stageTablePrefix)
    {
        this.stageTablePrefix = stageTablePrefix;
    }

    public synchronized void setStageDeleteFromTemplate(
            String stageDeleteFromTemplate)
    {
        this.stageDeleteFromTemplate = stageDeleteFromTemplate;
    }

    public synchronized void setStageInsertFromTemplate(
            String stageInsertFromTemplate)
    {
        this.stageInsertFromTemplate = stageInsertFromTemplate;
    }

    public synchronized void setStagePkeyColumn(String stagePkeyColumn)
    {
        this.stagePkeyColumn = stagePkeyColumn;
    }

    public synchronized void setStageRowIdColumn(String rowId)
    {
        this.stageRowIdColumn = rowId;
    }

    public synchronized void setSupportsReplace(boolean supportsReplace)
    {
        this.supportsReplace = supportsReplace;
    }

    public synchronized void setStageDirectory(String stageDirectory)
    {
        this.stageDirectory = stageDirectory;
    }

    public synchronized void setStartUpCommand(String startUpCommand)
    {
        this.startUpCommand = startUpCommand;
    }

    public synchronized void setCleanUpFiles(boolean cleanUpFiles)
    {
        this.cleanUpFiles = cleanUpFiles;
    }

    /**
     * Applies row updates using a batch loading scheme. Statements are
     * discarded. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.RawApplier#apply(com.continuent.tungsten.replicator.event.DBMSEvent,
     *      com.continuent.tungsten.replicator.event.ReplDBMSHeader, boolean,
     *      boolean)
     */
    @Override
    public void apply(DBMSEvent event, ReplDBMSHeader header, boolean doCommit,
            boolean doRollback) throws ReplicatorException,
            ConsistencyException, InterruptedException
    {
        ArrayList<DBMSData> dbmsDataValues = event.getData();

        // Iterate through values inferring the database name.
        for (DBMSData dbmsData : dbmsDataValues)
        {
            if (dbmsData instanceof StatementData)
            {
                if (logger.isDebugEnabled())
                {
                    StatementData stmtData = (StatementData) dbmsData;
                    logger.debug("Ignoring statement: " + stmtData.getQuery());
                }
            }
            else if (dbmsData instanceof RowChangeData)
            {
                RowChangeData rd = (RowChangeData) dbmsData;
                for (OneRowChange orc : rd.getRowChanges())
                {
                    // Get the action as well as the schema & table name.
                    ActionType action = orc.getAction();
                    String schema = orc.getSchemaName();
                    String table = orc.getTableName();
                    if (logger.isDebugEnabled())
                    {
                        logger.debug("Processing row update: action=" + action
                                + " schema=" + schema + " table=" + table);
                    }

                    // Process the action.
                    if (action.equals(ActionType.INSERT))
                    {
                        // Fetch column names and values.
                        List<ColumnSpec> colSpecs = orc.getColumnSpec();
                        ArrayList<ArrayList<ColumnVal>> colValues = orc
                                .getColumnValues();

                        // Get information the table definition.
                        Table tableMetadata = this.getTableMetadata(schema,
                                table, colSpecs, null);

                        // Insert each column into the CSV file.
                        this.writeInsertValues(tableMetadata, colSpecs,
                                colValues);
                    }
                    else if (action.equals(ActionType.UPDATE))
                    {
                        // Fetch column names and values.
                        List<ColumnSpec> keySpecs = orc.getKeySpec();
                        List<ColumnSpec> colSpecs = orc.getColumnSpec();
                        ArrayList<ArrayList<ColumnVal>> keyValues = orc
                                .getKeyValues();
                        ArrayList<ArrayList<ColumnVal>> colValues = orc
                                .getColumnValues();

                        // Get information the table definition.
                        Table tableMetadata = this.getTableMetadata(schema,
                                table, colSpecs, keySpecs);

                        // Write keys for deletion and columns for insert.
                        if (method == LoadMethod.staged || !supportsReplace)
                        {
                            // Direct loads that support replace can skip this.
                            this.writeDeleteValues(tableMetadata, keySpecs,
                                    keyValues);
                        }
                        this.writeInsertValues(tableMetadata, colSpecs,
                                colValues);
                    }
                    else if (action.equals(ActionType.DELETE))
                    {
                        // Fetch column names and values.
                        List<ColumnSpec> keySpecs = orc.getKeySpec();
                        // List<ColumnSpec> colSpecs = orc.getColumnSpec();
                        ArrayList<ArrayList<ColumnVal>> keyValues = orc
                                .getKeyValues();

                        // Get information about the table definition.
                        Table tableMetadata = this.getTableMetadata(schema,
                                table, keySpecs, keySpecs);

                        // Insert each column into the CSV file.
                        this.writeDeleteValues(tableMetadata, keySpecs,
                                keyValues);
                    }
                    else
                    {
                        logger.warn("Unrecognized action type: " + action);
                        return;
                    }
                }
            }
            else if (dbmsData instanceof LoadDataFileFragment)
            {
                if (logger.isDebugEnabled())
                    logger.debug("Ignoring load data file fragment");
            }
            else if (dbmsData instanceof RowIdData)
            {
                if (logger.isDebugEnabled())
                    logger.debug("Ignoring row ID data");
            }
            else
            {
                logger.warn("Unsupported DbmsData class: "
                        + dbmsData.getClass().getName());
            }
        }

        // Mark the current header and commit position if requested.
        this.latestHeader = header;
        if (doCommit)
            commit();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.RawApplier#commit()
     */
    @Override
    public void commit() throws ReplicatorException, InterruptedException
    {
        // If we don't have a last header, there is nothing to be done.
        if (latestHeader == null)
        {
            if (logger.isDebugEnabled())
                logger.debug("Unable to commit; last header is null");
            return;
        }

        // Load and merge each open delete CSV file.
        int loadCount = 0;
        for (CsvInfo info : openCsvFiles.values())
        {
            if (info.type == BatchType.delete)
            {
                if (method == LoadMethod.staged)
                {
                    clearStageTable(info);
                    load(info);
                    mergeStageTableDeletes(info);
                }
                else
                {
                    // Create a temporary staging table on the fly.
                    info.stageTableMetadata = createTempDeleteTable(info);
                    load(info);
                    mergeStageTableDeletes(info);
                }
                loadCount++;
            }
        }

        // Load each open insert CSV file.
        for (CsvInfo info : openCsvFiles.values())
        {
            if (info.type == BatchType.insert)
            {
                if (method == LoadMethod.staged)
                {
                    clearStageTable(info);
                    load(info);
                    mergeStageTableInserts(info);
                }
                else
                {
                    load(info);
                }
                loadCount++;
            }
        }

        // Make sure the insert and delete files match the total files.
        if (loadCount != openCsvFiles.size())
        {
            throw new ReplicatorException(
                    "Load file counts do not match total: insert+delete="
                            + loadCount + " total=" + openCsvFiles.size());
        }

        // Update trep_commit_seqno.
        try
        {
            commitSeqnoTable
                    .updateLastCommitSeqno(taskId, this.latestHeader, 0);
        }
        catch (SQLException e)
        {
            throw new ReplicatorException("Unable to update commit position", e);
        }

        // SQL commit here.
        try
        {
            conn.commit();
            conn.setAutoCommit(false);
        }
        catch (SQLException e)
        {
            throw new ReplicatorException("Unable to commit transaction", e);
        }

        // Clear the CSV file cache.
        openCsvFiles.clear();

        // Clear the load directories.
        if (this.cleanUpFiles)
            purgeDirIfExists(stageDir, false);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.RawApplier#getLastEvent()
     */
    @Override
    public ReplDBMSHeader getLastEvent() throws ReplicatorException,
            InterruptedException
    {
        return latestHeader;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.RawApplier#rollback()
     */
    @Override
    public void rollback() throws InterruptedException
    {
        // Roll back connection.
        try
        {
            rollbackTransaction();
        }
        catch (SQLException e)
        {
            logger.info("Unable to roll back transaction");
            if (logger.isDebugEnabled())
                logger.debug("Transaction rollback error", e);
        }

        // Clear the CSV file cache.
        openCsvFiles.clear();

        // Clear the load directories.
        try
        {
            purgeDirIfExists(stageDir, false);
        }
        catch (ReplicatorException e)
        {
            logger.error(
                    "Unable to purge staging directory; "
                            + stageDir.getAbsolutePath(), e);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.RawApplier#setTaskId(int)
     */
    @Override
    public void setTaskId(int id)
    {
        this.taskId = id;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    @Override
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        // Ensure basic properties are not null.
        assertNotNull(driver, "driver");
        assertNotNull(url, "url");
        assertNotNull(user, "user");
        assertNotNull(password, "password");
        assertNotNull(loadBatchTemplate, "loadBatchTemplate");
        assertNotNull(stageDirectory, "stageDirectory");

        // Get metadata schema.
        metadataSchema = context.getReplicatorSchemaName();
        consistencyTable = metadataSchema + "." + ConsistencyTable.TABLE_NAME;
        consistencySelect = "SELECT * FROM " + consistencyTable + " ";

        // Check properties associated with staged versus direct loading.
        if (DIRECT.equals(loadMethod))
        {
            method = LoadMethod.direct;
        }
        else if (STAGED.equals(loadMethod))
        {
            assertNotNull(stageTablePrefix, "stageTablePrefix");
            assertNotNull(stageDeleteFromTemplate, "stageDeleteFromTemplate");
            assertNotNull(stageInsertFromTemplate, "stageInsertFromTemplate");
            assertNotNull(stagePkeyColumn, "stagePkeyColumn");
            assertNotNull(stageRowIdColumn, "stageRowIdColumn");
            method = LoadMethod.staged;
        }
        else
        {
            throw new ReplicatorException(
                    "The loadMethod property must be set to direct or staged");
        }
    }

    // Ensure value is not null.
    public void assertNotNull(String property, String name)
            throws ReplicatorException
    {
        if (property == null)
        {
            throw new ReplicatorException(String.format(
                    "Property %s may not be null", name));
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    @Override
    public void prepare(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        // Create a formatter for printing dates.
        TimeZone tz = TimeZone.getTimeZone("GMT-0:00");
        dateFormatter = new SimpleDateFormat();
        dateFormatter.setTimeZone(tz);
        dateFormatter.applyPattern("yyyy-MM-dd HH:mm:ss.SSS");

        // Set up the staging directory.
        File staging = new File(stageDirectory);
        createDirIfNotExist(staging);

        // Define and create the load sub-directory.
        stageDir = new File(staging, "staging" + taskId);
        purgeDirIfExists(stageDir, true);
        createDirIfNotExist(stageDir);

        // Initialize table metadata cache.
        tableMetadataCache = new TableMetadataCache(5000);

        // Load JDBC driver and connect.
        try
        {
            // Load driver if provided.
            if (driver != null)
            {
                try
                {
                    Class.forName(driver);
                }
                catch (Exception e)
                {
                    throw new ReplicatorException("Unable to load driver: "
                            + driver, e);
                }
            }

            // Create the database.
            conn = DatabaseFactory.createDatabase(url, user, password);
            conn.connect(false);
            statement = conn.createStatement();

            // Set up heartbeat table.
            heartbeatTable = new HeartbeatTable(
                    context.getReplicatorSchemaName(), "");
            heartbeatTable.initializeHeartbeatTable(conn);
            if (method == LoadMethod.staged)
                createStageTables(heartbeatTable.getTable(), false);

            // Create consistency table
            Table consistency = ConsistencyTable
                    .getConsistencyTableDefinition(metadataSchema);
            conn.createTable(consistency, false);
            if (method == LoadMethod.staged)
                createStageTables(consistency, false);

            // Set up commit seqno table and fetch the last processed event.
            commitSeqnoTable = new CommitSeqnoTable(conn,
                    context.getReplicatorSchemaName(), "", false);
            commitSeqnoTable.prepare(taskId);
            latestHeader = commitSeqnoTable.lastCommitSeqno(taskId);

            // Ensure we are not in auto-commit mode.
            conn.setAutoCommit(false);

            // If a start-up command is present, execute that now.
            if (startUpCommand != null)
            {
                if (logger.isDebugEnabled())
                {
                    logger.debug("Executing startup command: " + startUpCommand);
                }
                conn.execute(startUpCommand);
            }
        }
        catch (SQLException e)
        {
            String message = String.format("Failed using url=%s, user=%s", url,
                    user);
            throw new ReplicatorException(message, e);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    @Override
    public void release(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        // Release staging directory if cleanup is requested.
        if (stageDir != null && cleanUpFiles)
            purgeDirIfExists(stageDir, true);

        // Release table cache.
        if (tableMetadataCache != null)
            tableMetadataCache.invalidateAll();
    }

    /**
     * Creates staging tables for the base table. Staging tables are used by
     * Replicator's BatchLoader if using staged load method.
     * 
     * @param baseTable Base table for which to create the corresponding staging
     *            tables.
     */
    private void createStageTables(Table baseTable, boolean dropExisting)
            throws SQLException
    {
        // Create INSERT table.
        Table stageInsertTable = getInsertStageTable(baseTable, true);

        if (logger.isDebugEnabled())
        {
            logger.debug("Creating test staging table: "
                    + stageInsertTable.getName());
            logger.debug("Table details: " + stageInsertTable);
        }

        conn.createTable(stageInsertTable, dropExisting);

        // Create DELETE table.
        Table stageDeleteTable = getDeleteStageTable(baseTable);

        if (logger.isDebugEnabled())
        {
            logger.debug("Creating test staging table: "
                    + stageDeleteTable.getName());
            logger.debug("Table details: " + stageDeleteTable);
        }

        conn.createTable(stageDeleteTable, dropExisting);
    }

    /**
     * Create stage table definition for insert by prefixing the base name,
     * removing PK constraint (if requested) and adding a stageRowIdColumn
     * column.
     * 
     * @param baseTable Table to create corresponding stage table to.
     * @param removePrimaryKey Remove PK constraint from the PK column?
     * @return Prepared staging table object. DBMS table is *not* created.
     */
    private Table getInsertStageTable(Table baseTable, boolean removePrimaryKey)
    {
        String stageInsertName = stageTablePrefix + "_insert_"
                + baseTable.getName();

        Table stageInsertTable = baseTable.clone();
        stageInsertTable.setTable(stageInsertName);
        if (removePrimaryKey)
        {
            Key primaryKey = stageInsertTable.getPrimaryKey();
            if (primaryKey != null)
            {
                stageInsertTable.getKeys().remove(primaryKey);
            }
        }
        Column rowIdColI = new Column(stageRowIdColumn, Types.INTEGER);
        stageInsertTable.AddColumn(rowIdColI);

        return stageInsertTable;
    }

    /**
     * Create stage table definition for delete by prefixing the base name and
     * adding the primary key and row_id as columns.
     * 
     * @param baseTable Table to create corresponding stage table to.
     * @return Prepared staging table object. DBMS table is *not* created.
     */
    private Table getDeleteStageTable(Table baseTable)
    {
        String stageDeleteName = stageTablePrefix + "_delete_"
                + baseTable.getName();

        Table stageDeleteTable = new Table(baseTable.getSchema(),
                stageDeleteName);

        // Column pkeyCol;
        // List<Key> keys = baseTableMetadata.getKeys();
        // if (keys == null || keys.size() == 0)
        // pkeyCol = new Column(this.stagePkeyColumn, Types.INTEGER);
        // else
        // pkeyCol = keys.get(0).getColumns().get(0);
        Column pkeyCol = new Column(stagePkeyColumn, Types.INTEGER);
        stageDeleteTable.AddColumn(pkeyCol);

        Column rowIdColD = new Column(stageRowIdColumn, Types.INTEGER);
        stageDeleteTable.AddColumn(rowIdColD);

        return stageDeleteTable;
    }

    // Returns an insert CSV file for a given schema and table name.
    private CsvInfo getInsertCsvWriter(Table tableMetadata)
            throws ReplicatorException
    {
        if (method == LoadMethod.direct)
        {
            // For direct loading we go straight into the base table.
            return getCsvWriter(tableMetadata, null, BatchType.insert, "insert");
        }
        else
        {
            Table stageTableMetadata = getInsertStageTable(tableMetadata, false);

            // Get a CVS writer for same.
            return getCsvWriter(tableMetadata, stageTableMetadata,
                    BatchType.insert, "insert");
        }
    }

    // Returns a delete CSV file for a given schema and table name.
    private CsvInfo getDeleteCsvWriter(Table baseTableMetadata)
            throws ReplicatorException
    {
        if (method == LoadMethod.direct)
        {
            return getCsvWriter(baseTableMetadata, null, BatchType.delete,
                    "delete");
        }
        else
        {
            Table stageTableMetadata = getDeleteStageTable(baseTableMetadata);

            // Get a CVS writer for same.
            return getCsvWriter(baseTableMetadata, stageTableMetadata,
                    BatchType.delete, "delete");
        }
    }

    // Returns an open CSV file corresponding to a given schema, table name, and
    // load type.
    private CsvInfo getCsvWriter(Table tableMetadata, Table stageTableMetadata,
            BatchType loadType, String prefix) throws ReplicatorException
    {
        // Create a key.
        String key = prefix + "." + tableMetadata.getSchema() + "."
                + tableMetadata.getName();
        CsvInfo info = this.openCsvFiles.get(key);
        if (info == null)
        {
            // Generate file name.
            File file = new File(this.stageDir, key + ".csv");

            // Pick the right table to use. For staging tables, we
            // need to use the stage metadata instead of going direct
            Table csvMetadata;
            if (stageTableMetadata == null)
                csvMetadata = tableMetadata;
            else
                csvMetadata = stageTableMetadata;

            // Now generate the CSV writer.
            try
            {
                // Generate a CSV writer on the file.
                BufferedWriter output = new BufferedWriter(new FileWriter(file));
                CsvWriter writer = conn.getCsvWriter(output);

                // Add the row ID if we are going via staging tables.
                boolean useRowId = false;
                if (method == LoadMethod.staged
                        && this.stageRowIdColumn != null)
                {
                    useRowId = true;
                    writer.addRowIdName(stageRowIdColumn);
                }

                // Populate columns.
                for (Column col : csvMetadata.getAllColumns())
                {
                    String colName = col.getName();
                    if (!useRowId || !stageRowIdColumn.equals(colName))
                    {
                        writer.addColumnName(col.getName());
                    }
                }

                // Create and cache writer information.
                info = new CsvInfo();
                info.type = loadType;
                info.schema = tableMetadata.getSchema();
                info.table = tableMetadata.getName();
                info.baseTableMetadata = tableMetadata;
                info.stageTableMetadata = stageTableMetadata;
                info.file = file;
                info.writer = writer;
                openCsvFiles.put(key, info);
            }
            catch (CsvException e)
            {
                throw new ReplicatorException("Unable to intialize CSV file: "
                        + e.getMessage(), e);
            }
            catch (IOException e)
            {
                throw new ReplicatorException("Unable to intialize CSV file: "
                        + file.getAbsolutePath());
            }
        }
        return info;
    }

    // Wrapper to load insert values.
    private void writeInsertValues(Table tableMetadata,
            List<ColumnSpec> colSpecs, ArrayList<ArrayList<ColumnVal>> colValues)
            throws ReplicatorException
    {
        // Get the CSV writer.
        CsvInfo info = getInsertCsvWriter(tableMetadata);
        this.writeValues(tableMetadata, colSpecs, colValues, info);
    }

    // Wrapper to load insert values.
    private void writeDeleteValues(Table tableMetadata,
            List<ColumnSpec> colSpecs, ArrayList<ArrayList<ColumnVal>> colValues)
            throws ReplicatorException
    {
        // Get the CSV writer.
        CsvInfo info = getDeleteCsvWriter(tableMetadata);
        this.writeValues(tableMetadata, colSpecs, colValues, info);
    }

    // Write values into a CSV file.
    private void writeValues(Table tableMetadata, List<ColumnSpec> colSpecs,
            ArrayList<ArrayList<ColumnVal>> colValues, CsvInfo info)
            throws ReplicatorException
    {
        // Get the CSV writer.
        CsvWriter csv = info.writer;

        // Insert each row's columns into the CSV file.
        Iterator<ArrayList<ColumnVal>> colIterator = colValues.iterator();
        while (colIterator.hasNext())
        {
            ArrayList<ColumnVal> row = colIterator.next();
            try
            {
                for (int i = 0; i < row.size(); i++)
                {
                    ColumnVal columnVal = row.get(i);
                    ColumnSpec columnSpec = colSpecs.get(i);
                    String value = getCsvString(columnVal, columnSpec);
                    csv.put(i + 1, value);
                }
                csv.write();
            }
            catch (CsvException e)
            {
                throw new ReplicatorException("Invalid write to CSV file: "
                        + info.file.getAbsolutePath(), e);
            }
            catch (IOException e)
            {
                throw new ReplicatorException(
                        "Unable to append value to CSV file: "
                                + info.file.getAbsolutePath(), e);
            }
        }
    }

    // Load an open CSV file.
    private void load(CsvInfo info) throws ReplicatorException
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("Loading CSV file: " + info.file.getAbsolutePath());
        }

        // Flush and close the file.
        int rowsToLoad = -1;
        try
        {
            info.writer.flush();
            info.writer.getWriter().close();
            rowsToLoad = info.writer.getRowCount();
        }
        catch (CsvException e)
        {
            throw new ReplicatorException("Unable to close CSV file: "
                    + info.file.getAbsolutePath(), e);
        }
        catch (IOException e)
        {
            throw new ReplicatorException("Unable to close CSV file: "
                    + info.file.getAbsolutePath());
        }

        // Decide whether to use the base table or a staging table.
        String schema;
        String tableName;
        if (info.stageTableMetadata == null)
        {
            schema = info.baseTableMetadata.getSchema();
            tableName = info.baseTableMetadata.getName();
        }
        else
        {
            schema = info.stageTableMetadata.getSchema();
            tableName = info.stageTableMetadata.getName();
        }

        // Generate the load command and execute.
        String loadCommand = getLoadCommand(schema, tableName, false, info.file);
        if (logger.isDebugEnabled())
        {
            logger.debug("Executing load command: " + loadCommand);
        }
        try
        {
            int rowsLoaded = statement.executeUpdate(loadCommand);
            if (rowsLoaded != rowsToLoad)
            {
                ReplicatorException re = new ReplicatorException(
                        "Difference between CSV file size and rows loaded: rowsInFile="
                                + rowsToLoad + " rowsLoaded=" + rowsLoaded);
                re.setExtraData(loadCommand);
                throw re;
            }
            if (logger.isDebugEnabled())
            {
                logger.debug("Rows loaded: " + rowsLoaded);
            }
        }
        catch (SQLException e)
        {
            ReplicatorException re = new ReplicatorException(
                    "Unable to execute load command", e);
            re.setExtraData(loadCommand);
            throw re;
        }

        // Delete the load file if we are done with it.
        if (cleanUpFiles && !info.file.delete())
        {
            logger.warn("Unable to delete load file: "
                    + info.file.getAbsolutePath());
        }
    }

    // Load an open CSV file.
    private void clearStageTable(CsvInfo info) throws ReplicatorException
    {
        Table table = info.stageTableMetadata;
        if (logger.isDebugEnabled())
        {
            logger.debug("Clearing stage table: " + table.fullyQualifiedName());
        }

        // Generate and submit SQL command.
        String delete = "DELETE FROM " + table.fullyQualifiedName();
        if (logger.isDebugEnabled())
        {
            logger.debug("Executing delete command: " + delete);
        }
        try
        {
            int rowsLoaded = statement.executeUpdate(delete);
            if (logger.isDebugEnabled())
            {
                logger.debug("Rows deleted: " + rowsLoaded);
            }
        }
        catch (SQLException e)
        {
            ReplicatorException re = new ReplicatorException(
                    "Unable to delete data from stage table: "
                            + table.fullyQualifiedName(), e);
            re.setExtraData(delete);
            throw re;
        }
    }

    // Load an open CSV file.
    private void mergeStageTableInserts(CsvInfo info)
            throws ReplicatorException
    {
        Table base = info.baseTableMetadata;
        Table stage = info.stageTableMetadata;
        if (logger.isDebugEnabled())
        {
            logger.debug("Merging inserts from stage table: "
                    + stage.fullyQualifiedName());
        }

        // Generate fields required for insert merges.
        String pkey = stagePkeyColumn;
        String basePkey = base.getName() + "." + pkey;
        String stagePkey = stage.getName() + "." + pkey;
        StringBuffer colNames = new StringBuffer();
        for (Column col : base.getAllColumns())
        {
            if (colNames.length() > 0)
                colNames.append(",");
            colNames.append(col.getName());
        }

        // Insert values into SQL template.
        String insert = stageInsertFromTemplate;
        insert = insert.replace("%%BASE_TABLE%%", base.fullyQualifiedName());
        insert = insert.replace("%%BASE_COLUMNS%%", colNames.toString());
        insert = insert.replace("%%STAGE_TABLE%%", stage.fullyQualifiedName());
        insert = insert.replace("%%BASE_PKEY%%", basePkey);
        insert = insert.replace("%%STAGE_PKEY%%", stagePkey);

        // Execute the merge command.
        if (logger.isDebugEnabled())
        {
            logger.debug("Executing insert merge command: " + insert);
        }
        try
        {
            int rows = statement.executeUpdate(insert);
            if (logger.isDebugEnabled())
            {
                logger.debug("Rows inserted: " + rows);
            }
        }
        catch (SQLException e)
        {
            ReplicatorException re = new ReplicatorException(
                    "Unable to merge data from stage table: "
                            + stage.fullyQualifiedName(), e);
            re.setExtraData(insert);
            throw re;
        }
    }

    // Create a temp table for deletes.
    private Table createTempDeleteTable(CsvInfo info)
            throws ReplicatorException
    {
        Table deleteTable = getDeleteTableMetadata(info.schema, info.table);
        try
        {
            conn.createTable(deleteTable, true);
        }
        catch (SQLException e)
        {
            ReplicatorException re = new ReplicatorException(
                    "Unable to create load table: " + deleteTable.getName(), e);
            throw re;
        }
        return deleteTable;
    }

    // Load an open CSV file.
    private void mergeStageTableDeletes(CsvInfo info)
            throws ReplicatorException
    {
        // Temp tables are not used with staging method.
        if (method == LoadMethod.direct)
        {
            // Load data into temp table.
            String loadCommand = getLoadCommand(info.schema,
                    info.baseTableMetadata.getName(),
                    method == LoadMethod.direct, info.file);
            try
            {
                int rows = statement.executeUpdate(loadCommand);
                if (logger.isDebugEnabled())
                {
                    logger.debug("Executed load: loadCommand=" + loadCommand
                            + " rows=" + rows);
                }
            }
            catch (SQLException e)
            {
                ReplicatorException re = new ReplicatorException(
                        "Unable to execute load command", e);
                re.setExtraData(loadCommand);
                throw re;
            }
        } // TODO: is the code below needed for direct? Should it be in "else"
          // clause?

        // Get our tables.
        Table base = info.baseTableMetadata;
        Table stage = info.stageTableMetadata;
        if (logger.isDebugEnabled())
        {
            logger.debug("Merging deletes from stage table: "
                    + stage.fullyQualifiedName());
        }

        // Generate fields required for delete merges.
        String pkey = stagePkeyColumn;
        String basePkey = base.getName() + "." + pkey;
        String stagePkey = stage.getName() + "." + pkey;
        StringBuffer colNames = new StringBuffer();
        for (Column col : base.getAllColumns())
        {
            if (colNames.length() > 0)
                colNames.append(",");
            colNames.append(col.getName());
        }

        // Insert values into SQL template.
        String insert = stageDeleteFromTemplate;
        insert = insert.replace("%%BASE_TABLE%%", base.fullyQualifiedName());
        insert = insert.replace("%%BASE_COLUMNS%%", colNames.toString());
        insert = insert.replace("%%STAGE_TABLE%%", stage.fullyQualifiedName());
        insert = insert.replace("%%BASE_PKEY%%", basePkey);
        insert = insert.replace("%%STAGE_PKEY%%", stagePkey);

        // Execute the merge command.
        if (logger.isDebugEnabled())
        {
            logger.debug("Executing insert merge command: " + insert);
        }
        try
        {
            int rows = statement.executeUpdate(insert);
            if (logger.isDebugEnabled())
            {
                logger.debug("Rows inserted: " + rows);
            }
        }
        catch (SQLException e)
        {
            ReplicatorException re = new ReplicatorException(
                    "Unable to merge data from stage table: "
                            + stage.fullyQualifiedName(), e);
            re.setExtraData(insert);
            throw re;
        }
    }

    /**
     * Returns an open CSV file corresponding to a given schema and table name.
     * 
     * @param temp Is this a temporary table? Temporary tables are treated
     *            differently under PostgreSQL.
     */
    private String getLoadCommand(String schema, String table, boolean temp,
            File csvFile) throws ReplicatorException
    {
        // Temporary tables cannot specify a schema name under PG.
        String qualifiedTable = schema
                + (temp && conn instanceof PostgreSQLDatabase ? "_" : ".")
                + table;
        String loadCommand = this.loadCommands.get(qualifiedTable);
        if (loadCommand == null)
        {
            // Generate load command with file and qualified table name
            // substituted into template. "
            loadCommand = this.loadBatchTemplate.replace("%%TABLE%%",
                    qualifiedTable);
            loadCommand = loadCommand.replace("%%FILE%%",
                    csvFile.getAbsolutePath());
            loadCommands.put(qualifiedTable, loadCommand);
            if (logger.isDebugEnabled())
            {
                logger.debug("Generated load command: table=" + qualifiedTable
                        + " command=" + loadCommand);
            }
        }
        return loadCommand;
    }

    // Get table metadata. Cache for table metadata is populated automatically.
    private Table getTableMetadata(String schema, String name,
            List<ColumnSpec> colSpecs, List<ColumnSpec> keySpecs)
    {
        // In the cache first.
        Table t = tableMetadataCache.retrieve(schema, name);

        // Create if missing and add to cache.
        if (t == null)
        {
            // Create table definition.
            if (logger.isDebugEnabled())
            {
                logger.debug("Adding metadata for table: schema=" + schema
                        + " table=" + name);
            }
            t = new Table(schema, name);

            // Add column definitions.
            for (ColumnSpec colSpec : colSpecs)
            {
                Column col = new Column(colSpec.getName(), colSpec.getType());
                t.AddColumn(col);
            }

            // Store the new definition.
            tableMetadataCache.store(t);
        }

        // If keys are missing and we have them, add them now. This extra
        // step is necessary because insert operations do not have keys,
        // whereas update and delete do. So if we added the insert later,
        // we will need it now.
        if (t.getKeys().size() == 0 && keySpecs != null && keySpecs.size() > 0)
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Adding metadata for table: schema=" + schema
                        + " table=" + name);
            }

            // Fetch the column definition matching each element of the key
            // we receive from replication and construct a key definition.
            Key key = new Key(Key.Primary);
            for (ColumnSpec keySpec : keySpecs)
            {
                for (Column col : t.getAllColumns())
                {
                    String colName = col.getName();
                    if (colName != null && colName.equals(keySpec.getName()))
                    {
                        key.AddColumn(col);
                        break;
                    }
                }
            }

            // Add the key.
            t.AddKey(key);
        }

        // Return the table.
        return t;
    }

    // Get delete table metadata by constructing a Table instance for an
    // existing table.
    private Table getDeleteTableMetadata(String schema, String name)
            throws ReplicatorException
    {
        // Find the base table. This must exist already or something is wrong.
        Table t = tableMetadataCache.retrieve(schema, name);
        if (t == null)
        {
            throw new ReplicatorException(
                    "Table metadata missing for delete table construction: schema="
                            + schema + " table=" + name);
        }

        // Get the keys and construct a companion temp table that just holds
        // keys.
        List<Key> keys = t.getKeys();
        if (keys.size() == 0)
        {
            throw new ReplicatorException(
                    "Table keys missing for delete table construction: schema="
                            + schema + " table=" + name);
        }
        Table deleteTable = new Table(schema, name + "_xxx_delete_xxx");
        deleteTable.setTemporary(true);
        for (Column keyCol : keys.get(0).getColumns())
        {
            deleteTable.AddColumn(keyCol);
        }
        deleteTable.AddKey(keys.get(0));

        // Return the resulting delete table.
        return deleteTable;
    }

    /**
     * Converts a column value to a suitable String for CSV loading. This can be
     * overloaded for particular DBMS types.
     * 
     * @param value Column value
     * @param columnSpec Column metadata
     * @return String for loading
     */
    protected String getCsvString(ColumnVal columnVal, ColumnSpec columnSpec)
    {
        Object value = columnVal.getValue();
        if (value == null)
        {
            if (conn instanceof PostgreSQLDatabase)
            {
                // PG needs to distinguish between NULL and an empty string.
                return null;
            }
            else
                return "";
        }
        else if (value instanceof Timestamp)
        {
            return dateFormatter.format((Timestamp) value);
        }
        else if (value instanceof java.sql.Date)
        {
            return dateFormatter.format((java.sql.Date) value);
        }
        else
            return value.toString();
    }

    // Create a directory if it does not exist.
    private void createDirIfNotExist(File dir) throws ReplicatorException
    {
        if (!dir.exists())
        {
            if (!dir.mkdirs())
            {
                throw new ReplicatorException(
                        "Unable to create staging directory: "
                                + dir.getAbsolutePath());
            }
        }
    }

    // Clear and optionally delete a directory if it exists.
    private void purgeDirIfExists(File dir, boolean delete)
            throws ReplicatorException
    {
        // Return if there's nothing to do.
        if (!dir.exists())
            return;

        // Remove any files.
        for (File child : dir.listFiles())
        {
            if (!child.delete())
            {
                throw new ReplicatorException("Unable to delete staging file: "
                        + child.getAbsolutePath());
            }
        }

        // Remove directory if desired.
        if (delete && !dir.delete())
        {
            if (!dir.delete())
            {
                throw new ReplicatorException(
                        "Unable to delete staging directory: "
                                + dir.getAbsolutePath());
            }
        }
    }

    // Rolls back the current transaction.
    private void rollbackTransaction() throws SQLException
    {
        try
        {
            conn.rollback();
        }
        catch (SQLException e)
        {
            logger.error("Failed to rollback : " + e);
            throw e;
        }
        finally
        {
            // Switch connection back to auto-commit.
            conn.setAutoCommit(true);
        }
    }
}