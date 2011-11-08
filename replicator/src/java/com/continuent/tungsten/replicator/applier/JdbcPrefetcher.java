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
 * Initial developer(s): Stephane Giron
 * Contributor(s):  
 */

package com.continuent.tungsten.replicator.applier;

import java.io.UnsupportedEncodingException;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime;
import com.continuent.tungsten.replicator.database.Column;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.DatabaseFactory;
import com.continuent.tungsten.replicator.database.MySQLOperationMatcher;
import com.continuent.tungsten.replicator.database.SqlOperation;
import com.continuent.tungsten.replicator.database.SqlOperationMatcher;
import com.continuent.tungsten.replicator.database.Table;
import com.continuent.tungsten.replicator.database.TableMetadataCache;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.LoadDataFileDelete;
import com.continuent.tungsten.replicator.dbms.LoadDataFileFragment;
import com.continuent.tungsten.replicator.dbms.LoadDataFileQuery;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.RowIdData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.DBMSEmptyEvent;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSFilteredEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplDBMSHeaderData;
import com.continuent.tungsten.replicator.event.ReplOption;
import com.continuent.tungsten.replicator.event.ReplOptionParams;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.thl.CommitSeqnoTable;
import com.continuent.tungsten.replicator.thl.THLManagerCtrl;

/**
 * Implements a DBMS implementation-independent applier. DBMS-specific features
 * must be subclassed. This applier can be used directly by specifying the DBMS
 * driver and full JDBC URL.
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class JdbcPrefetcher implements RawApplier
{
    static Logger                     logger               = Logger.getLogger(JdbcPrefetcher.class);

    // DELETE [LOW_PRIORITY] [QUICK] [IGNORE] FROM tbl_name
    private Pattern                   delete               = Pattern
                                                                   .compile(
                                                                           "^\\s*delete\\s*(?:low_priority\\s*)?(?:quick\\s*)?(?:ignore\\s*)?(?:from\\s*)(.*)",
                                                                           Pattern.CASE_INSENSITIVE);

    // UPDATE [LOW_PRIORITY] [IGNORE] table_reference
    private Pattern                   update               = Pattern
                                                                   .compile(
                                                                           "^\\s*update\\s*(?:low_priority\\s*)?(?:ignore\\s*)?((?:[`\"]*(?:[a-zA-Z0-9_]+)[`\"]*\\.){0,1}[`\"]*(?:[a-zA-Z0-9_]+)[`\"]*(?:\\s*,\\s*(?:[`\"]*(?:[a-zA-Z0-9_]+)[`\"]*\\.){0,1}[`\"]*(?:[a-zA-Z0-9_]+)[`\"]*)*)\\s+SET\\s+(?:.*)?\\s+(WHERE\\s+.*)",
                                                                           Pattern.CASE_INSENSITIVE);

    protected int                     taskId               = 0;
    protected ReplicatorRuntime       runtime              = null;
    protected String                  driver               = null;
    protected String                  url                  = null;
    protected String                  user                 = "root";
    protected String                  password             = "rootpass";
    protected String                  ignoreSessionVars    = null;

    protected String                  metadataSchema       = null;
    protected Database                conn                 = null;
    protected Statement               statement            = null;
    private PreparedStatement         seqnoStatement       = null;
    protected Pattern                 ignoreSessionPattern = null;

    protected String                  lastSessionId        = "";

    // Values of schema and timestamp which are buffered to avoid
    // unnecessary commands on the SQL connection.
    protected String                  currentSchema        = null;
    protected long                    currentTimestamp     = -1;

    // Statistics.
    protected long                    eventCount           = 0;
    protected long                    commitCount          = 0;

    /**
     * Maximum length of SQL string to log in case of an error. This is needed
     * because some statements may be very large. TODO: make this configurable
     * via replicator.properties
     */
    protected int                     maxSQLLogLength      = 1000;

    private TableMetadataCache        tableMetadataCache;

    private ReplDBMSHeader            lastProcessedEvent   = null;

    protected HashMap<String, String> currentOptions;

    // SQL parser.
    SqlOperationMatcher               sqlMatcher           = new MySQLOperationMatcher();

    private Long                      initTime             = 0L;

    private Map<Long, Timestamp>      appliedTimes;

    private long                      minSeqno             = -1;

    private int                       aheadMaxTime         = 3000;

    private int                       sleepTime            = 500;

    private int                       warmUpEventCount     = 100;

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.RawApplier#setTaskId(int)
     */
    public void setTaskId(int id)
    {
        this.taskId = id;
        if (logger.isDebugEnabled())
            logger.debug("Set task id: id=" + taskId);
    }

    public void setDriver(String driver)
    {
        this.driver = driver;
    }

    public Database getDatabase()
    {
        return conn;
    }

    public void setUrl(String url)
    {
        this.url = url;
    }

    public void setUser(String user)
    {
        this.user = user;
    }

    public void setPassword(String password)
    {
        this.password = password;
    }

    public void setIgnoreSessionVars(String ignoreSessionVars)
    {
        this.ignoreSessionVars = ignoreSessionVars;
    }

    /**
     * Sets the aheadMaxTime value. This is the maximum time that event should be from the last applied event (based on master times
     * 
     * @param aheadMaxTime The aheadMaxTime to set.
     */
    public void setAheadMaxTime(int aheadMaxTime)
    {
        this.aheadMaxTime = aheadMaxTime;
    }

    /**
     * Sets the sleepTime value.
     * 
     * @param sleepTime The sleepTime to set.
     */
    public void setSleepTime(int sleepTime)
    {
        this.sleepTime = sleepTime;
    }

    /**
     * Sets the warmUpEventCount value.
     * 
     * @param warmUpEventCount The warmUpEventCount to set.
     */
    public void setWarmUpEventCount(int warmUpEventCount)
    {
        this.warmUpEventCount = warmUpEventCount;
    }

    enum PrintMode
    {
        ASSIGNMENT, NAMES_ONLY, VALUES_ONLY, PLACE_HOLDER
    }

    /**
     * @param keyValues Is used to identify NULL values, in which case, if the
     *            mode is ASSIGNMENT, "x IS ?" is constructed instead of "x =
     *            ?".
     */
    protected void printColumnSpec(StringBuffer stmt,
            ArrayList<OneRowChange.ColumnSpec> cols,
            ArrayList<OneRowChange.ColumnVal> keyValues, PrintMode mode,
            String separator)
    {
        boolean first = true;
        for (int i = 0; i < cols.size(); i++)
        {
            OneRowChange.ColumnSpec col = cols.get(i);
            if (!first)
                stmt.append(separator);
            else
                first = false;
            if (mode == PrintMode.ASSIGNMENT)
            {
                if (keyValues != null)
                {
                    if (keyValues.get(i).getValue() == null)
                    {
                        // TREP-276: use "IS NULL" vs. "= NULL"
                        // stmt.append(col.getName() + " IS ? ");
                        // Oracle cannot handle "IS ?" and then binding a NULL
                        // value. It needs
                        // an explicit "IS NULL".
                        stmt.append(conn.getDatabaseObjectName(col.getName())
                                + " IS NULL ");
                    }
                    else
                    {
                        stmt.append(conn.getDatabaseObjectName(col.getName())
                                + " = "
                                + conn.getPlaceHolder(col, keyValues.get(i)
                                        .getValue(), col.getTypeDescription()));
                    }
                }
            }
        }
    }

    /**
     * Queries database for column names of a table that OneRowChange is
     * affecting. Fills in column names and key names for the given
     * OneRowChange.
     * 
     * @param data
     * @return Number of columns that a table has. Zero, if no columns were
     *         retrieved (table does not exist or has no columns).
     * @throws SQLException
     */
    protected int fillColumnNames(OneRowChange data) throws SQLException
    {
        Table t = tableMetadataCache.retrieve(data.getSchemaName(),
                data.getTableName());
        if (t == null)
        {
            // Not yet in cache
            t = new Table(data.getSchemaName(), data.getTableName());
            DatabaseMetaData meta = conn.getDatabaseMetaData();
            ResultSet rs = null;

            try
            {
                rs = conn.getColumnsResultSet(meta, data.getSchemaName(),
                        data.getTableName());
                while (rs.next())
                {
                    String columnName = rs.getString("COLUMN_NAME");
                    int columnIdx = rs.getInt("ORDINAL_POSITION");

                    Column column = addColumn(rs, columnName);
                    column.setPosition(columnIdx);
                    t.AddColumn(column);
                }
                tableMetadataCache.store(t);
            }
            finally
            {
                if (rs != null)
                {
                    rs.close();
                }
            }
        }

        // Set column names.
        for (Column column : t.getAllColumns())
        {
            ListIterator<OneRowChange.ColumnSpec> litr = data.getColumnSpec()
                    .listIterator();
            for (; litr.hasNext();)
            {
                OneRowChange.ColumnSpec cv = litr.next();
                if (cv.getIndex() == column.getPosition())
                {
                    cv.setName(column.getName());
                    cv.setSigned(column.isSigned());
                    cv.setTypeDescription(column.getTypeDescription());

                    // Check whether column is real blob on the applier side
                    if (cv.getType() == Types.BLOB)
                        cv.setBlob(column.isBlob());

                    break;
                }
            }

            litr = data.getKeySpec().listIterator();
            for (; litr.hasNext();)
            {
                OneRowChange.ColumnSpec cv = litr.next();
                if (cv.getIndex() == column.getPosition())
                {
                    cv.setName(column.getName());
                    cv.setSigned(column.isSigned());
                    cv.setTypeDescription(column.getTypeDescription());

                    // Check whether column is real blob on the applier side
                    if (cv.getType() == Types.BLOB)
                        cv.setBlob(column.isBlob());

                    break;
                }
            }

        }
        return t.getColumnCount();
    }

    /**
     * Returns a new column definition.
     * 
     * @param rs Metadata resultset
     * @param columnName Name of the column to be added
     * @return the column definition
     * @throws SQLException if an error occurs
     */
    protected Column addColumn(ResultSet rs, String columnName)
            throws SQLException
    {
        return new Column(columnName, rs.getInt("DATA_TYPE"));
    }

    protected int bindValues(PreparedStatement prepStatement,
            ArrayList<OneRowChange.ColumnVal> values, int startBindLoc,
            ArrayList<OneRowChange.ColumnSpec> specs, boolean skipNulls)
            throws SQLException
    {
        int bindLoc = startBindLoc; /*
                                     * prepared stmt variable index starts from
                                     * 1
                                     */

        for (int idx = 0; idx < values.size(); idx++)
        {
            OneRowChange.ColumnVal value = values.get(idx);
            if (value.getValue() == null)
            {
                if (skipNulls)
                    continue;
                if (conn.nullsBoundDifferently(specs.get(idx)))
                    continue;
            }
            setObject(prepStatement, bindLoc, value, specs.get(idx));

            bindLoc += 1;
        }
        return bindLoc;
    }

    protected void setObject(PreparedStatement prepStatement, int bindLoc,
            OneRowChange.ColumnVal value, ColumnSpec columnSpec)
            throws SQLException
    {
        // By default, type is not used. If specific operations have to be done,
        // this should happen in specific classes (e.g. OracleApplier).
        prepStatement.setObject(bindLoc, value.getValue());
    }

    protected void applyRowIdData(RowIdData data) throws ReplicatorException
    {
        logger.warn("No applier for rowid data specified");
    }

    protected void prefetchStatementData(StatementData data)
            throws ReplicatorException
    {
        try
        {
            String schema = data.getDefaultSchema();
            Long timestamp = data.getTimestamp();
            List<ReplOption> options = data.getOptions();

            applyUseSchema(schema);

            applySetTimestamp(timestamp);

            applySessionVariables(options);

            String sqlQuery = null;
            if (data.getQuery() != null)
                sqlQuery = data.getQuery();
            else
            {

                try
                {
                    sqlQuery = new String(data.getQueryAsBytes(),
                            data.getCharset());
                }
                catch (UnsupportedEncodingException e)
                {
                    sqlQuery = new String(data.getQueryAsBytes());
                }
            }

            SqlOperation parsing = (SqlOperation) data.getParsingMetadata();

            if (parsing.getOperation() == SqlOperation.DELETE)
            {
                Matcher m = delete.matcher(sqlQuery);
                if (m.matches())
                {
                    String sqlQueryOld = sqlQuery;
                    sqlQuery = "SELECT * FROM " + m.group(1);
                    if (logger.isDebugEnabled())
                        logger.debug("Transformed " + sqlQueryOld + " into "
                                + sqlQuery);
                }
            }
            else if (parsing.getOperation() == SqlOperation.UPDATE)
            {
                Matcher m = update.matcher(sqlQuery);
                if (m.matches())
                {
                    String sqlQueryOld = sqlQuery;
                    sqlQuery = "SELECT * FROM " + m.group(1) + " " + m.group(2);
                    if (logger.isDebugEnabled())
                        logger.debug("Transformed " + sqlQueryOld + " into "
                                + sqlQuery);
                }
            }
            // else do nothing
            else
            {
                statement.clearBatch();
                return;
            }

            int[] updateCount;
            statement.addBatch(sqlQuery);
            statement.setEscapeProcessing(false);
            try
            {
                updateCount = statement.executeBatch();
            }
            catch (SQLWarning e)
            {
                String msg = "While applying SQL event:\n" + data.toString()
                        + "\nWarning: " + e.getMessage();
                logger.warn(msg);
                updateCount = new int[1];
                updateCount[0] = statement.getUpdateCount();
            }
            catch (SQLException e)
            {
                if (data.getErrorCode() == 0)
                {
                    SQLException sqlException = new SQLException(
                            "Statement failed on slave but succeeded on master");
                    sqlException.initCause(e);
                    throw sqlException;
                }
            }
        }
        catch (SQLException e)
        {
            logFailedStatementSQL(data.getQuery(), e);
            throw new ApplierException(e);
        }
    }

    /**
     * applySetTimestamp adds to the batch the query used to change the server
     * timestamp, if needed and if possible (if the database support such a
     * feature)
     * 
     * @param timestamp the timestamp to be used
     * @throws SQLException if an error occurs
     */
    protected void applySetTimestamp(Long timestamp) throws SQLException
    {
        if (timestamp != null && conn.supportsControlTimestamp())
        {
            if (timestamp.longValue() != currentTimestamp)
            {
                currentTimestamp = timestamp.longValue();
                statement.addBatch(conn.getControlTimestampQuery(timestamp));
            }
        }
    }

    /**
     * applySetUseSchema adds to the batch the query used to change the current
     * schema where queries should be executed, if needed and if possible (if
     * the database support such a feature)
     * 
     * @param schema the schema to be used
     * @throws SQLException if an error occurs
     */
    protected void applyUseSchema(String schema) throws SQLException
    {
        if (schema != null && schema.length() > 0
                && !schema.equals(this.currentSchema))
        {
            currentSchema = schema;
            if (conn.supportsUseDefaultSchema())
                statement.addBatch(conn.getUseSchemaQuery(schema));
        }
    }

    /**
     * applyOptionsToStatement adds to the batch queries used to change the
     * connection options, if needed and if possible (if the database support
     * such a feature)
     * 
     * @param options
     * @return true if any option changed
     * @throws SQLException
     */
    protected boolean applySessionVariables(List<ReplOption> options)
            throws SQLException
    {
        boolean sessionVarChange = false;

        if (options != null && conn.supportsSessionVariables())
        {
            if (currentOptions == null)
                currentOptions = new HashMap<String, String>();

            for (ReplOption statementDataOption : options)
            {
                // if option already exists and have the same value, skip it
                // Otherwise, we need to set it on the current connection
                String optionName = statementDataOption.getOptionName();
                String optionValue = statementDataOption.getOptionValue();

                // Ignore internal Tungsten options.
                if (optionName
                        .startsWith(ReplOptionParams.INTERNAL_OPTIONS_PREFIX))
                    continue;

                // If we are ignoring this option, just continue.
                if (ignoreSessionPattern != null)
                {
                    if (ignoreSessionPattern.matcher(optionName).matches())
                    {
                        if (logger.isDebugEnabled())
                            logger.debug("Ignoring session variable: "
                                    + optionName);
                        continue;
                    }
                }

                if (optionName.equals(StatementData.CREATE_OR_DROP_DB))
                {
                    // Clearing current used schema, so that it will force a new
                    // "use" statement to be issued for the next query
                    currentSchema = null;
                    continue;
                }

                String currentOptionValue = currentOptions.get(optionName);
                if (currentOptionValue == null
                        || !currentOptionValue.equalsIgnoreCase(optionValue))
                {
                    String optionSetStatement = conn.prepareOptionSetStatement(
                            optionName, optionValue);
                    if (optionSetStatement != null)
                    {
                        if (logger.isDebugEnabled())
                            logger.debug("Issuing " + optionSetStatement);
                        statement.addBatch(optionSetStatement);
                    }
                    currentOptions.put(optionName, optionValue);
                    sessionVarChange = true;
                }
            }
        }
        return sessionVarChange;
    }

    /**
     * Logs SQL into error log stream. Trims the message if it exceeds
     * maxSQLLogLength.<br/>
     * In addition, extracts and logs next exception of the SQLException, if
     * available. This extends logging detail that is provided by general
     * exception logging mechanism.
     * 
     * @see #maxSQLLogLength
     * @param sql the sql statement to be logged
     */
    protected void logFailedStatementSQL(String sql, SQLException ex)
    {
        try
        {
            String log = "Statement failed: " + sql;
            if (log.length() > maxSQLLogLength)
                log = log.substring(0, maxSQLLogLength);
            logger.error(log);

            // Sometimes there's more details to extract from the exception.
            if (ex != null && ex.getCause() != null
                    && ex.getCause() instanceof SQLException)
            {
                SQLException nextException = ((SQLException) ex.getCause())
                        .getNextException();
                if (nextException != null)
                {
                    logger.error(nextException.getMessage());
                }
            }
        }
        catch (Exception e)
        {
            if (logger.isDebugEnabled())
                logger.debug("logFailedStatementSQL failed to log, because: "
                        + e.getMessage());
        }
    }

    /**
     * Constructs a SQL statement template later used for prepared statement.
     * 
     * @param action INSERT/UPDATE/DELETE
     * @param schemaName Database name to work on.
     * @param tableName Table name to work on.
     * @param columns Columns to INSERT/UPDATE.
     * @param keys Columns to search on.
     * @param keyValues Column values that are being searched for. Used for
     *            identifying NULL values and constructing "x IS NULL" instead
     *            of "x = NULL". May be null, in which case "x = NULL" is always
     *            used.
     * @return Constructed SQL statement with "?" instead of real values.
     */
    private StringBuffer buildSelectQuery(String schemaName, String tableName,
            ArrayList<OneRowChange.ColumnSpec> keys,
            ArrayList<OneRowChange.ColumnVal> keyValues)
    {
        StringBuffer stmt = new StringBuffer();
        stmt.append("SELECT * FROM ");
        stmt.append(conn.getDatabaseObjectName(schemaName) + "."
                + conn.getDatabaseObjectName(tableName));
        stmt.append(" WHERE ");
        printColumnSpec(stmt, keys, keyValues, PrintMode.ASSIGNMENT, " AND ");
        return stmt;
    }

    /**
     * Compares current key values to the previous key values and determines
     * whether null values changed. Eg. {1, 3, null} vs. {5, 2, null} returns
     * false, but {1, 3, null} vs. {1, null, null} returns true.<br/>
     * Size of both arrays must be the same.
     * 
     * @param currentKeyValues Current key values.
     * @param previousKeyValues Previous key values.
     * @return true, if positions of null values in currentKeyValues changed
     *         compared to previousKeyValues.
     */
    private static boolean didNullKeysChange(
            ArrayList<OneRowChange.ColumnVal> currentKeyValues,
            ArrayList<OneRowChange.ColumnVal> previousKeyValues)
    {
        for (int i = 0; i < currentKeyValues.size(); i++)
        {
            if (previousKeyValues.get(i).getValue() == null
                    || currentKeyValues.get(i).getValue() == null)
                if (!(previousKeyValues.get(i).getValue() == null && currentKeyValues
                        .get(i).getValue() == null))
                    return true;
        }
        return false;
    }

    protected boolean needNewSQLStatement(int row,
            ArrayList<ArrayList<OneRowChange.ColumnVal>> keyValues)
    {
        if (keyValues.size() > row
                && didNullKeysChange(keyValues.get(row), keyValues.get(row - 1)))
            return true;
        return false;
    }

    protected void applyOneRowChangePrepared(OneRowChange oneRowChange)
            throws ReplicatorException
    {
        PreparedStatement prepStatement = null;

        if (oneRowChange.getAction() == RowChangeData.ActionType.INSERT)
            // Nothing to do for now
            return;

        else
        {
            // UPDATE or DELETE
            try
            {
                int colCount = fillColumnNames(oneRowChange);
                if (colCount <= 0)
                {
                    logger.warn("No column information found for table (perhaps table is missing?): "
                            + oneRowChange.getSchemaName()
                            + "."
                            + oneRowChange.getTableName());
                    // While prefetching, it is possible that the table we try
                    // to prefetch does not exist yet. In that case, just
                    // return.
                    return;
                }
            }
            catch (SQLException e1)
            {
                logger.error("column name information could not be retrieved");
                return;
            }

            StringBuffer stmt = null;

            ArrayList<OneRowChange.ColumnSpec> key = oneRowChange.getKeySpec();

            try
            {
                ArrayList<ArrayList<OneRowChange.ColumnVal>> keyValues = oneRowChange
                        .getKeyValues();
                ArrayList<ArrayList<OneRowChange.ColumnVal>> columnValues = oneRowChange
                        .getColumnValues();

                int row = 0;
                for (row = 0; row < columnValues.size()
                        || row < keyValues.size(); row++)
                {
                    if (row == 0 || needNewSQLStatement(row, keyValues))
                    {
                        // Build a new statement only if needed
                        ArrayList<OneRowChange.ColumnVal> keyValuesOfThisRow = null;
                        if (keyValues.size() > 0)
                            keyValuesOfThisRow = keyValues.get(row);

                        stmt = buildSelectQuery(oneRowChange.getSchemaName(),
                                oneRowChange.getTableName(), key,
                                keyValuesOfThisRow);

                        // runtime.getMonitor().incrementEvents(columnValues.size());
                        prepStatement = conn.prepareStatement(stmt.toString());
                    }

                    int bindLoc = 1; /* Start binding at index 1 */

                    /* bind key values */
                    if (keyValues.size() > 0)
                    {
                        bindLoc = bindValues(prepStatement, keyValues.get(row),
                                bindLoc, key, true);
                    }
                    ResultSet rs = null;
                    try
                    {
                        rs = prepStatement.executeQuery();
                    }
                    catch (SQLWarning e)
                    {
                        String msg = "While applying SQL event:\n"
                                + stmt.toString() + "\nWarning: "
                                + e.getMessage();
                        logger.warn(msg);
                    }
                    finally
                    {
                        if (rs != null)
                            rs.close();
                    }
                }

                if (logger.isDebugEnabled())
                {
                    logger.debug("Prefetched event " + " : " + stmt.toString());
                }
            }
            catch (SQLException e)
            {
                ApplierException applierException = new ApplierException(e);
                applierException.setExtraData(logFailedRowChangeSQL(stmt,
                        oneRowChange));
                throw applierException;
            }
            finally
            {
                if (prepStatement != null)
                {
                    try
                    {
                        prepStatement.close();
                    }
                    catch (SQLException ignore)
                    {
                    }
                }
            }
        }
    }

    /**
     * Logs prepared statement and it's arguments into error log stream. Trims
     * the message if it exceeds maxSQLLogLength.
     * 
     * @see #maxSQLLogLength
     * @param stmt SQL template for PreparedStatement
     * @return
     */
    private String logFailedRowChangeSQL(StringBuffer stmt,
            OneRowChange oneRowChange)
    {
        // TODO: use THLManagerCtrl for logging exact failing SQL after
        // branch thl_meta is merged into HEAD. Now this duplicates
        // functionality.
        try
        {
            ArrayList<OneRowChange.ColumnSpec> keys = oneRowChange.getKeySpec();
            ArrayList<OneRowChange.ColumnSpec> columns = oneRowChange
                    .getColumnSpec();
            ArrayList<ArrayList<OneRowChange.ColumnVal>> keyValues = oneRowChange
                    .getKeyValues();
            ArrayList<ArrayList<OneRowChange.ColumnVal>> columnValues = oneRowChange
                    .getColumnValues();
            String log = "Failing statement : " + stmt.toString()
                    + "\nArguments:";
            for (int row = 0; row < columnValues.size()
                    || row < keyValues.size(); row++)
            {
                log += "\n - ROW# = " + row;
                // Print column values.
                for (int c = 0; c < columns.size(); c++)
                {
                    if (columnValues.size() > 0)
                    {
                        OneRowChange.ColumnSpec colSpec = columns.get(c);
                        ArrayList<OneRowChange.ColumnVal> values = columnValues
                                .get(row);
                        OneRowChange.ColumnVal value = values.get(c);
                        log += "\n"
                                + THLManagerCtrl.formatColumn(colSpec, value,
                                        "COL", null);
                    }
                }
                // Print key values.
                for (int k = 0; k < keys.size(); k++)
                {
                    if (keyValues.size() > 0)
                    {
                        OneRowChange.ColumnSpec colSpec = keys.get(k);
                        ArrayList<OneRowChange.ColumnVal> values = keyValues
                                .get(row);
                        OneRowChange.ColumnVal value = values.get(k);
                        log += "\n"
                                + THLManagerCtrl.formatColumn(colSpec, value,
                                        "KEY", null);
                    }
                }
            }
            // Output the error message, truncate it if too large.
            if (log.length() > maxSQLLogLength)
                log = log.substring(0, maxSQLLogLength);

            return log;
        }
        catch (Exception e)
        {
            if (logger.isDebugEnabled())
                logger.debug("logFailedRowChangeSQL failed to log, because: "
                        + e.getMessage());
        }
        return null;
    }

    protected void applyRowChangeData(RowChangeData data,
            List<ReplOption> options) throws ReplicatorException
    {
        if (options != null)
        {
            try
            {
                if (applySessionVariables(options))
                {
                    // Apply session variables to the connection only if
                    // something changed
                    statement.executeBatch();
                    statement.clearBatch();
                }
            }
            catch (SQLException e)
            {
                throw new ApplierException("Failed to apply session variables",
                        e);
            }
        }

        for (OneRowChange row : data.getRowChanges())
        {
            applyOneRowChangePrepared(row);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.RawApplier#apply(com.continuent.tungsten.replicator.event.DBMSEvent,
     *      com.continuent.tungsten.replicator.event.ReplDBMSHeader, boolean,
     *      boolean)
     */
    public void apply(DBMSEvent event, ReplDBMSHeader header, boolean doCommit,
            boolean doRollback)
    {
        long seqno = -1;

        if (appliedTimes == null)
            appliedTimes = new TreeMap<Long, Timestamp>();

        Timestamp sourceTstamp = event.getSourceTstamp();
        appliedTimes.put(header.getSeqno(), sourceTstamp);

        if (header.getSeqno() <= minSeqno + warmUpEventCount)
        {
            if (logger.isDebugEnabled())
                logger.debug("Discarding already applied event "
                        + header.getSeqno());
            return;
        }

        while (true)
        {
            if (initTime == 0)
            {
                initTime = sourceTstamp.getTime();
            }

            // // Check if this is worth prefetching
            ResultSet rs = null;
            try
            {
                rs = seqnoStatement.executeQuery();
                if (rs.next())
                {
                    seqno = rs.getLong("seqno");
                    minSeqno = seqno;
                }
            }
            catch (SQLException e)
            {
                logger.warn(e);
            }
            finally
            {
                if (rs != null)
                    try
                    {
                        rs.close();
                    }
                    catch (SQLException e)
                    {
                    }
            }

            for (Iterator<Entry<Long, Timestamp>> iterator = appliedTimes
                    .entrySet().iterator(); iterator.hasNext();)
            {
                Entry<Long, Timestamp> next = iterator.next();
                if (next.getKey() > seqno)
                {
                    break;
                }

                long time = next.getValue().getTime();
                initTime = time;

                if (next.getKey() < seqno)
                {
                    iterator.remove();
                }
                else
                    break;
            }

            if (header.getSeqno() <= seqno + warmUpEventCount)
            {
                if (logger.isDebugEnabled())
                    logger.debug("Discarding event "
                            + header.getSeqno()
                            + " as it is either already applied or to close to slave position");
                return;
            }

            if (initTime > 0
                    && event.getSourceTstamp().getTime() - initTime > aheadMaxTime)
            {
                if (logger.isDebugEnabled())
                    logger.debug("Event is too far ahead of current slave position... sleeping "
                            + event.getSourceTstamp().getTime()
                            + " "
                            + initTime);
                // this event is too far ahead of the CommitSeqnoTable position
                // : sleep some time and continue
                try
                {
                    Thread.sleep(sleepTime);
                }
                catch (InterruptedException e)
                {
                    return;
                }
                continue;
            }

            // Ensure we are not trying to apply a previously applied event.
            // This case can arise during restart.
            if (lastProcessedEvent != null && lastProcessedEvent.getLastFrag()
                    && lastProcessedEvent.getSeqno() >= header.getSeqno()
                    && !(event instanceof DBMSEmptyEvent))
            {
                logger.info("Skipping over previously applied event: seqno="
                        + header.getSeqno() + " fragno=" + header.getFragno());
                return;
            }
            // if (logger.isDebugEnabled())
            logger.warn("Prefetch for event: seqno=" + header.getSeqno()
                    + " fragno=" + header.getFragno());

            try
            {
                if (event instanceof DBMSEmptyEvent)
                {
                    return;
                }
                else if (header instanceof ReplDBMSFilteredEvent)
                {
                    return;
                }
                else
                {
                    ArrayList<DBMSData> data = event.getData();
                    for (DBMSData dataElem : data)
                    {
                        if (dataElem instanceof RowChangeData)
                        {
                            applyRowChangeData((RowChangeData) dataElem,
                                    event.getOptions());
                        }
                        else if (dataElem instanceof LoadDataFileFragment)
                        {
                            // Don't do anything with prefetch
                        }
                        else if (dataElem instanceof LoadDataFileQuery)
                        {
                            // Don't do anything with prefetch
                        }
                        else if (dataElem instanceof LoadDataFileDelete)
                        {
                            // Don't do anything with prefetch
                        }
                        else if (dataElem instanceof StatementData)
                        {
                            StatementData sdata = (StatementData) dataElem;

                            // Check for table metadata cache invalidation.
                            SqlOperation sqlOperation = (SqlOperation) sdata
                                    .getParsingMetadata();

                            String query = sdata.getQuery();
                            if (sqlOperation == null)
                            {
                                if (query == null)
                                    query = new String(sdata.getQueryAsBytes());
                                sqlOperation = sqlMatcher.match(query);
                                sdata.setParsingMetadata(sqlOperation);
                            }

                            prefetchStatementData(sdata);

                            int invalidated = tableMetadataCache.invalidate(
                                    sqlOperation, sdata.getDefaultSchema());
                            if (invalidated > 0)
                            {
                                if (logger.isDebugEnabled())
                                    logger.debug("Table metadata invalidation: stmt="
                                            + query
                                            + " invalidated="
                                            + invalidated);
                            }

                        }
                        else if (dataElem instanceof RowIdData)
                        {
                            applyRowIdData((RowIdData) dataElem);
                        }
                    }
                }
            }
            catch (ReplicatorException e)
            {
                logger.warn("Failed to prefetch event " + header.getSeqno()
                        + "... Skipping", e);
            }

            // Update the last processed
            lastProcessedEvent = header;

            // Update statistics.
            this.eventCount++;
            if (logger.isDebugEnabled() && eventCount % 20000 == 0)
                logger.debug("Apply statistics: events=" + eventCount
                        + " commits=" + commitCount);

            return;
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.RawApplier#commit()
     */
    public void commit() throws ReplicatorException, InterruptedException
    {
        return;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.RawApplier#rollback()
     */
    public void rollback() throws InterruptedException
    {
        return;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.RawApplier#getLastEvent()
     */
    public ReplDBMSHeader getLastEvent() throws ReplicatorException
    {
        logger.warn("Getting last event");
        if (seqnoStatement != null)
        {
            ResultSet rs = null;
            try
            {
                rs = seqnoStatement.executeQuery();
                if (rs.next())
                    return new ReplDBMSHeaderData(rs.getLong(1),
                            rs.getShort(2), rs.getBoolean(3), rs.getString(4),
                            rs.getLong(5), rs.getString(6), null, null);
            }
            catch (SQLException e)
            {
                logger.warn(e);
            }
            finally
            {
                if (rs != null)
                    try
                    {
                        rs.close();
                    }
                    catch (SQLException e)
                    {
                    }
            }
        }
        return null;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException
    {
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

            seqnoStatement = conn
                    .prepareStatement("select seqno, fragno, last_Frag, source_id, epoch_number, eventid from "
                            + context.getReplicatorSchemaName()
                            + "."
                            + CommitSeqnoTable.TABLE_NAME);

            // Enable binlogs at session level if this is supported and we are
            // either a remote service or slave logging is turned on. This
            // repeats logic in the connect() call but gives a clear log
            // message, which is important for diagnostic purposes.
            if (conn.supportsControlSessionLevelLogging())
            {
                if (runtime.logReplicatorUpdates() || runtime.isRemoteService())
                {
                    if (logger.isDebugEnabled())
                        logger.debug("Slave updates will be logged");
                    conn.controlSessionLevelLogging(false);
                }
                else
                {
                    if (logger.isDebugEnabled())
                        logger.debug("Slave updates will not be logged");
                    conn.controlSessionLevelLogging(true);
                }
            }

            // Set session variable to show we are a slave.
            if (conn.supportsSessionVariables())
            {
                if (logger.isDebugEnabled())
                    logger.debug("Setting TREPSLAVE session variable");
                conn.setSessionVariable("TREPSLAVE", "YES");
            }

            tableMetadataCache = new TableMetadataCache(5000);

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
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException
    {
        runtime = (ReplicatorRuntime) context;
        metadataSchema = context.getReplicatorSchemaName();
        if (ignoreSessionVars != null)
        {
            ignoreSessionPattern = Pattern.compile(ignoreSessionVars);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException
    {
        // if (commitSeqnoTable != null)
        // {
        // commitSeqnoTable.release();
        // commitSeqnoTable = null;
        // }

        currentOptions = null;

        statement = null;
        if (conn != null)
        {
            conn.close();
            conn = null;
        }

        if (tableMetadataCache != null)
        {
            tableMetadataCache.invalidateAll();
            tableMetadataCache = null;
        }
    }
}
