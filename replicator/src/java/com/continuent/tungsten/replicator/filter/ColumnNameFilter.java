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

package com.continuent.tungsten.replicator.filter;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.database.Column;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.DatabaseFactory;
import com.continuent.tungsten.replicator.database.MySQLOperationMatcher;
import com.continuent.tungsten.replicator.database.SqlOperation;
import com.continuent.tungsten.replicator.database.SqlOperationMatcher;
import com.continuent.tungsten.replicator.database.Table;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * This class defines a ColumnNameFilter. It adds column name information to
 * events on the extractor side.
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class ColumnNameFilter implements Filter
{
    private static Logger                               logger     = Logger.getLogger(ColumnNameFilter.class);

    // Metadata cache is a hashtable indexed by the database name and each
    // database uses a hashtable indexed by the table name (This is done in
    // order to be able to drop all table definitions at once if a DROP DATABASE
    // is trapped). Filling metadata cache is done in a lazy way. It will be
    // updated only when a table is used for the first time by a row event.
    private Hashtable<String, Hashtable<String, Table>> metadataCache;

    Database                                            conn       = null;

    private String                                      user;
    private String                                      url;
    private String                                      password;

    // SQL parser.
    SqlOperationMatcher                                 sqlMatcher = new MySQLOperationMatcher();

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException
    {
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
                {
                    getColumnInformation(orc);
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
        else
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

    // Fetch information about schema.
    private void getColumnInformation(OneRowChange orc)
            throws ReplicatorException
    {
        String tableName = orc.getTableName();

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
            Table newTable = null;
            try
            {
                newTable = conn.findTable(orc.getSchemaName(),
                        orc.getTableName());
            }
            catch (SQLException e)
            {
                throw new ReplicatorException(
                        "Unable to retrieve column metadata: schema="
                                + orc.getSchemaName() + " table="
                                + orc.getTableName());
            }
            if (newTable == null)
            {
                throw new ReplicatorException(
                        "Unable to find column metadata; table may be missing: schema="
                                + orc.getSchemaName() + " table="
                                + orc.getTableName());
            }
            newTable.setTableId(orc.getTableId());
            dbCache.put(tableName, newTable);
        }

        Table table = dbCache.get(tableName);

        ArrayList<Column> columns = table.getAllColumns();
        int index = 0;
        for (Iterator<ColumnSpec> iterator = orc.getColumnSpec().iterator(); iterator
                .hasNext();)
        {
            ColumnSpec type = iterator.next();
            type.setName(columns.get(index).getName());
            index++;
        }

        index = 0;
        for (Iterator<ColumnSpec> iterator = orc.getKeySpec().iterator(); iterator
                .hasNext();)
        {
            ColumnSpec type = iterator.next();
            type.setName(columns.get(index).getName());
            index++;
        }
        // We could retrieve primary keys at this point
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
}
