/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2010-2011 Continuent Inc.
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
 * Contributor(s): Linas Virbalas
 */

package com.continuent.tungsten.replicator.filter;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.Column;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.DatabaseFactory;
import com.continuent.tungsten.replicator.database.Key;
import com.continuent.tungsten.replicator.database.MySQLOperationMatcher;
import com.continuent.tungsten.replicator.database.SqlOperation;
import com.continuent.tungsten.replicator.database.SqlOperationMatcher;
import com.continuent.tungsten.replicator.database.Table;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.RowChangeData.ActionType;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.filter.Filter;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * This class defines a PrimaryKeyFilter
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class PrimaryKeyFilter implements Filter
{
    private static Logger                               logger               = Logger.getLogger(PrimaryKeyFilter.class);

    // Metadata cache is a hashtable indexed by the database name and each
    // database uses a hashtable indexed by the table name (This is done in
    // order to be able to drop all table definitions at once if a DROP DATABASE
    // is trapped). Filling metadata cache is done in a lazy way. It will be
    // updated only when a table is used for the first time by a row event.
    private Hashtable<String, Hashtable<String, Table>> metadataCache;

    Database                                            conn                 = null;

    private String                                      user;
    private String                                      url;
    private String                                      password;

    private List<String>                                tables               = null;
    private List<String>                                schemas              = null;
    private String                                      processTablesSchemas = null;
    private boolean                                     addPkeyToInserts     = false;
    private boolean                                     addColumnsToDeletes  = false;

    // SQL parser.
    SqlOperationMatcher                                 sqlMatcher           = new MySQLOperationMatcher();

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException
    {
        if (processTablesSchemas != null)
        {
            if (logger.isDebugEnabled())
                logger.debug("Tables to process: " + processTablesSchemas);

            tables = new ArrayList<String>();
            schemas = new ArrayList<String>();

            String[] list = processTablesSchemas.split(",");
            for (int i = 0; i < list.length; i++)
            {
                String t = list[i].trim().toUpperCase();
                if (t.contains("."))
                    tables.add(t);
                else
                    schemas.add(t);
            }
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException
    {
        metadataCache = new Hashtable<String, Hashtable<String, Table>>();

        // Load defaults for connection
        if (url == null)
            url = context.getJdbcUrl("tungsten_" + context.getServiceName());
        if (user == null)
            user = context.getJdbcUser();
        if (password == null)
            password = context.getJdbcPassword();

        // Connect.
        try
        {
            conn = DatabaseFactory.createDatabase(url, user, password);
            conn.connect();
        }
        catch (SQLException e)
        {
            throw new ReplicatorException(e);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException
    {
        if (metadataCache != null)
        {
            metadataCache.clear();
            metadataCache = null;
        }
        if (conn != null)
        {
            conn.close();
            conn = null;
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.filter.Filter#filter(com.continuent.tungsten.replicator.event.ReplDBMSEvent)
     */
    public ReplDBMSEvent filter(ReplDBMSEvent event)
            throws ReplicatorException, InterruptedException
    {
        ArrayList<DBMSData> data = event.getData();
        if (data == null)
            return event;
        for (DBMSData dataElem : data)
        {
            if (dataElem instanceof RowChangeData)
            {
                RowChangeData rdata = (RowChangeData) dataElem;
                for (OneRowChange orc : rdata.getRowChanges())
                    try
                    {
                        // Check for and add primary key information. This
                        // also adds delete column information if desired.
                        checkForPK(orc);
                    }
                    catch (SQLException e)
                    {
                        throw new ReplicatorException(
                                "Filter failed processing primary key information",
                                e);
                    }
            }
            else if (dataElem instanceof StatementData)
            {
                StatementData sdata = (StatementData) dataElem;
                // Parse statements in order to update table definitions if
                // needed. e.g. DROP DATABASE should drop information about keys
                // which are defined for this database tables, ...
                String query = sdata.getQuery();
                if (query == null)
                    query = new String(sdata.getQueryAsBytes());

                SqlOperation sqlOperation = sqlMatcher.match(query);

                if (sqlOperation.getOperation() == SqlOperation.DROP
                        && sqlOperation.getObjectType() == SqlOperation.SCHEMA)
                {
                    // "drop database" statement detected : remove database
                    // metadata
                    String dbName = sqlOperation.getSchema();
                    if (metadataCache.remove(dbName) != null)
                    {
                        if (logger.isDebugEnabled())
                            logger.debug("DROP DATABASE detected - Removing database metadata for '"
                                    + dbName + "'");
                    }
                    else if (logger.isDebugEnabled())
                        logger.debug("DROP DATABASE detected - no cached database metadata to delete for '"
                                + dbName + "'");
                    continue;
                }
                else if (sqlOperation.getOperation() == SqlOperation.ALTER)
                {
                    // Detected an alter table statement / Dropping table
                    // metadata for the concerned table
                    String name = sqlOperation.getName();
                    String defaultDB = sdata.getDefaultSchema();
                    removeTableMetadata(name, sqlOperation.getSchema(),
                            defaultDB);
                    continue;
                }

            }
        }
        return event;
    }

    private void removeTableMetadata(String tableName, String schemaName,
            String defaultDB)
    {
        if (schemaName != null)
        {
            Hashtable<String, Table> tableCache = metadataCache.get(schemaName);
            if (tableCache != null && tableCache.remove(tableName) != null)
            {
                if (logger.isDebugEnabled())
                    logger.debug("ALTER TABLE detected - Removing table metadata for '"
                            + schemaName + "." + tableName + "'");
            }
            else if (logger.isDebugEnabled())
                logger.debug("ALTER TABLE detected - no cached table metadata to remove for '"
                        + schemaName + "." + tableName + "'");
        }
        else if (defaultDB != null)
        {
            Hashtable<String, Table> tableCache = metadataCache.get(defaultDB);
            if (tableCache != null && tableCache.remove(tableName) != null)
                logger.info("ALTER TABLE detected - Removing table metadata for '"
                        + defaultDB + "." + tableName + "'");
            else
                logger.info("ALTER TABLE detected - no cached table metadata to remove for '"
                        + defaultDB + "." + tableName + "'");
        }
    }

    // Add primary keys to row change data.
    private void checkForPK(OneRowChange orc) throws SQLException
    {
        if (orc.getAction() == ActionType.INSERT && !addPkeyToInserts)
            return;

        String tableName = orc.getTableName();

        if (schemas != null
                && (!schemas.contains(orc.getSchemaName().toUpperCase()) && !tables
                        .contains((orc.getSchemaName() + "." + tableName)
                                .toUpperCase())))
        {
            if (logger.isInfoEnabled())
                logger.info("Table " + orc.getSchemaName() + "." + tableName
                        + " not taken into account by the primary key filter");
            return;
        }

        if (!metadataCache.containsKey(orc.getSchemaName()))
        {
            // Nothing defined yet in this database
            metadataCache.put(orc.getSchemaName(),
                    new Hashtable<String, Table>());
        }

        Hashtable<String, Table> dbCache = metadataCache.get(orc
                .getSchemaName());

        if (!dbCache.containsKey(tableName) || orc.getTableId() == -1
                || dbCache.get(tableName).getTableId() != orc.getTableId())
        {
            // This table was not processed yet or schema changed since it was
            // cached : fetch information about its primary key
            if (dbCache.remove(tableName) != null && logger.isDebugEnabled())
                logger.debug("Detected a schema change for table "
                        + orc.getSchemaName() + "." + tableName
                        + " - Removing table metadata from cache");
            Table newTable = conn.findTable(orc.getSchemaName(),
                    orc.getTableName());
            if (newTable != null)
            {
                newTable.setTableId(orc.getTableId());
                dbCache.put(tableName, newTable);
            }
            else if (logger.isDebugEnabled())
                logger.debug("Table " + tableName + " not found in "
                        + orc.getSchemaName());
        }

        Table table = dbCache.get(tableName);
        if (table == null)
        {
            if (logger.isDebugEnabled())
                logger.debug("Table " + orc.getSchemaName() + "." + tableName
                        + " not found in cache");
            return;
        }
        Key primaryKey = table.getPrimaryKey();
        if (primaryKey == null)
        {
            // No primary key -> just return
            if (logger.isDebugEnabled())
                logger.debug("Table has no primary keys: "
                        + orc.getSchemaName() + "." + tableName);
            return;
        }

        List<Column> keys = primaryKey.getColumns();
        if (keys == null || keys.isEmpty())
        {
            // No primary key -> just return
            if (logger.isDebugEnabled())
                logger.debug("Table has no primary keys: "
                        + orc.getSchemaName() + "." + tableName);
            return;
        }

        // Primary keys identified... let's filter irrelevant key fields
        // from the row event
        ArrayList<ColumnSpec> keySpecs = orc.getKeySpec();
        ArrayList<ArrayList<ColumnVal>> keyValues = orc.getKeyValues();

        for (Iterator<ColumnSpec> iterator = keySpecs.iterator(); iterator
                .hasNext();)
        {
            ColumnSpec keySpec = iterator.next();
            boolean found = false;

            for (Iterator<Column> iterator2 = keys.iterator(); iterator2
                    .hasNext();)
            {
                Column column = iterator2.next();
                if (keySpec.getIndex() == column.getPosition())
                {
                    found = true;
                    break;
                }
            }

            if (!found)
            {
                // This is not a primary key -> remove it from the keys
                // First remove key values
                int idx = keySpecs.indexOf(keySpec);

                for (Iterator<ArrayList<ColumnVal>> iterator2 = keyValues
                        .iterator(); iterator2.hasNext();)
                {
                    if (logger.isDebugEnabled())
                        logger.debug("Removing non primary key column: "
                                + keySpec.getIndex());

                    ArrayList<ColumnVal> values = iterator2.next();
                    values.remove(idx);
                }

                // Then remove the key specs
                iterator.remove();
            }
        }

        // Add the PK information to the INSERTs.
        if (orc.getAction() == ActionType.INSERT)
        {
            if (keySpecs.size() == 0 && keyValues.size() == 0)
            {
                // Add the key columns.
                for (int k = 0; k < keys.size(); k++)
                {
                    Column column = keys.get(k);
                    OneRowChange.ColumnSpec colSpec = orc.new ColumnSpec();
                    colSpec.setIndex(column.getPosition());
                    colSpec.setName(column.getName());
                    colSpec.setType(column.getType());
                    colSpec.setTypeDescription(column.getTypeDescription());
                    keySpecs.add(colSpec);

                    // Add empty dummy column values to the key column.
                    // Without this ProtobufSerializer will fail.
                    ArrayList<ColumnVal> columnValues = new ArrayList<ColumnVal>();
                    keyValues.add(columnValues);
                }
            }
            else
            {
                logger.debug("INSERT already contain keys: " + keySpecs.size());
            }
        }
        else if (orc.getAction() == ActionType.DELETE
                && this.addColumnsToDeletes)
        {
            // Optionally get table columns and add them to metadata for the
            // DELETE.
            List<ColumnSpec> colSpecs = orc.getColumnSpec();
            if (colSpecs.size() == 0)
            {
                List<Column> cols = table.getAllColumns();
                for (int i = 0; i < cols.size(); i++)
                {
                    Column column = cols.get(i);
                    OneRowChange.ColumnSpec colSpec = orc.new ColumnSpec();
                    colSpec.setIndex(column.getPosition());
                    colSpec.setName(column.getName());
                    colSpec.setType(column.getType());
                    colSpec.setTypeDescription(column.getTypeDescription());
                    colSpecs.add(colSpec);
                }
            }
        }
    }

    public void setUser(String user)
    {
        this.user = user;
    }

    public void setUrl(String url)
    {
        this.url = url;
    }

    public void setPassword(String password)
    {
        this.password = password;
    }

    public void setProcessTablesSchemas(String processTablesSchemas)
    {
        this.processTablesSchemas = processTablesSchemas;
    }

    /**
     * If set, primary key ColumnSpec objects will be added to row change events
     * of INSERTs.
     */
    public void setAddPkeyToInserts(boolean addPkeyToInserts)
    {
        this.addPkeyToInserts = addPkeyToInserts;
    }

    /**
     * If set, ColumnSpec objects will be added to deletes so that downstream
     * stages have full column information on the deleted table.
     */
    public synchronized void setAddColumnsToDeletes(boolean addColumnsToDeletes)
    {
        this.addColumnsToDeletes = addColumnsToDeletes;
    }
}
