/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2010 Continuent Inc.
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
 * Initial developer(s): Alex Yurchenko
 * Contributor(s): Stephane Giron
 */

package com.continuent.tungsten.replicator.consistency;

import java.util.ArrayList;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;
import com.continuent.tungsten.replicator.event.ReplOptionParams;
import com.continuent.tungsten.replicator.filter.Filter;
import com.continuent.tungsten.replicator.plugin.PluginContext;

/**
 * This class implements a filter to find consistency checks.
 * 
 * @author <a href="mailto:alexey.yurchenko@continuent.com">Alex Yurchenko</a>
 * @version 1.0
 */
public class ConsistencyCheckFilter implements Filter
{
    static Logger   logger             = Logger
                                               .getLogger(ConsistencyCheckFilter.class);

    String          metadataSchema;
    String          consistencyTable;

    // Selects database from consistency update pattern: "WHERE db= 'db1' AND".
    private Pattern statementDbPattern = Pattern
                                               .compile(
                                                       "WHERE\\s*db\\s*=\\s*(?:\'|\")([a-zA-Z0-9_]+)(?:\'|\")\\s* AND",
                                                       Pattern.CASE_INSENSITIVE);


    /**
     * Stores the consistency check table name. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        metadataSchema = context.getReplicatorSchemaName();
        consistencyTable = metadataSchema + "." + ConsistencyTable.TABLE_NAME;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        // Nothing to do.
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException,
            InterruptedException
    {
        // Nothing to do.
    }

    public ReplDBMSEvent filter(ReplDBMSEvent event)
            throws ReplicatorException, InterruptedException
    {
        ArrayList<DBMSData> data = event.getData();
        try
        {
            if (this.isConsistencyCheck(data))
            {
                if (data.get(data.size() - 1) instanceof StatementData)
                {
                    getWhereFromStatement(event);
                }
                else if (data.get(data.size() - 1) instanceof RowChangeData)
                {
                    getWhereFromRowChange(event);
                }
                else
                {
                    String msg = "Unsupported consistency check log event: "
                            + data.get(data.size() - 1).getClass()
                                    .getCanonicalName();
                    logger.error(msg);
                }
            }
        }
        catch (ConsistencyException e)
        {
            logger.error("Failed to create consistency check metadata: "
                    + e.getMessage());
        }

        return event;
    }

    /**
     * Check quickly to see if we have a consistency check. Later calls will go
     * back in to compute metadata. Meanwhile, this call is meant to be quick.
     */
    private boolean isConsistencyCheck(ArrayList<DBMSData> dataArray)
    {
        boolean consistencyCheck = false;
        if (dataArray.size() >= 2
                && dataArray.get(dataArray.size() - 1) instanceof StatementData)
        {
            StatementData stmt = (StatementData) dataArray
                    .get(dataArray.size() - 1);
            // TODO: make a better detection of update to
            // consistency table.
            if (stmt.getQuery() != null)
                consistencyCheck = stmt.getQuery().contains(consistencyTable);
            else
            {
                // Check the first few bytes in order to detect a consistency
                // check.
                consistencyCheck = new String(stmt.getQueryAsBytes(), 0, Math
                        .min(200, stmt.getQueryAsBytes().length))
                        .contains(consistencyTable);
            }
        }
        else if (dataArray.size() >= 1 && dataArray.size() <= 3
                && dataArray.get(dataArray.size() - 1) instanceof RowChangeData)
        {
            RowChangeData rc = (RowChangeData) dataArray
                    .get(dataArray.size() - 1);
            OneRowChange orc = rc.getRowChanges().get(0);
            consistencyCheck = (orc.getSchemaName()
                    .compareToIgnoreCase(metadataSchema)) == 0
                    && (orc.getTableName().compareToIgnoreCase(
                            ConsistencyTable.TABLE_NAME) == 0)
                    && orc.getAction() == RowChangeData.ActionType.UPDATE;
        }
        return consistencyCheck;
    }

    // Extracts the WHERE clause and database schema from statement.
    private void getWhereFromStatement(ReplDBMSEvent event)
            throws ConsistencyException
    {
        ArrayList<DBMSData> data = event.getData();

        // Sanity checks
        // last query before COMMIT must be UPDATE, before UPDATE there must be
        // INSERT
        final int updateIndex = data.size() - 1;
        final int insertIndex = updateIndex - 1;

        StatementData updateStatement = (StatementData) data.get(updateIndex);
        String update = null;
        if (updateStatement.getQuery() != null)
            update = updateStatement.getQuery().toLowerCase(Locale.ENGLISH);
        else
            update = new String(updateStatement.getQueryAsBytes())
                    .toLowerCase(Locale.ENGLISH);
        if (!update.matches("^update\\s+.+"))
        {
            logger.error("UPDATE:  " + update);
            throw new ConsistencyException(
                    "Invalid consistency check transaction: expected UPDATE before COMMIT, found: "
                            + update);
        }

        StatementData insertStatement = (StatementData) data.get(insertIndex);
        String insert = null;
        if (insertStatement.getQuery() != null)
            insert = insertStatement.getQuery().toLowerCase(Locale.ENGLISH);
        else
            insert = new String(insertStatement.getQueryAsBytes())
                    .toLowerCase(Locale.ENGLISH);
        
        if (!insert.matches("^insert\\s+into\\s+.+"))
        {
            logger.error("INSERT:  " + insert);
            throw new ConsistencyException(
                    "Invalid consistency check transaction: expected INSERT before UPDATE, found: "
                            + insert);
        }

        // Pick off the database name for the consistency check and assign
        // the shard ID. This ensures the consistency check is processed in
        // sequence with other shard transactions.
        Matcher m = this.statementDbPattern.matcher(update);
        if (m.find())
        {
            String dbName = m.group(1);
            event.getDBMSEvent().setMetaDataOption(ReplOptionParams.SHARD_ID,
                    dbName);
        }
        else
        {
            // This is a fall-back if we cannot determine the DBMS. We must
            // serialize or the consistency check may execute out of sequence.
            event.getDBMSEvent().setMetaDataOption(ReplOptionParams.SHARD_ID,
                    ReplOptionParams.SHARD_ID_UNKNOWN);
        }

        // Since we don't have real parser, just look for the whole word
        int whereBegin = update.lastIndexOf(" where ") + 1;

        // Substring() returns a new String, so we must be safe here.
        String where = updateStatement.getQuery().substring(whereBegin);

        // Assign the where clause.
        if (where != null)
            event.getDBMSEvent().addMetadataOption(
                    ReplOptionParams.CONSISTENCY_WHERE, where);
    }

    // Extracts the WHERE clause and database schema from row changes.
    private void getWhereFromRowChange(ReplDBMSEvent event)
            throws ConsistencyException
    {
        ArrayList<DBMSData> data = event.getData();

        RowChangeData rcd = (RowChangeData) data.get(data.size() - 1);
        OneRowChange orc = rcd.getRowChanges().get(0);
        ArrayList<ColumnVal> keyValues = orc.getKeyValues().get(0);
        String dbName = null;

        StringBuffer where = new StringBuffer(256);
        where.append(" WHERE ");
        where.append(ConsistencyTable.dbColumnName);
        where.append(" = '");

        if (keyValues.get(ConsistencyTable.dbColumnIdx).getValue() instanceof String)
        {
            dbName = (String) keyValues.get(ConsistencyTable.dbColumnIdx)
                    .getValue();
            where.append(dbName);
        }
        else
        {
            where.append(new String((byte[]) keyValues.get(
                    ConsistencyTable.dbColumnIdx).getValue()));
        }

        where.append("' AND ");
        where.append(ConsistencyTable.tblColumnName);
        where.append(" = '");

        if (keyValues.get(ConsistencyTable.tblColumnIdx).getValue() instanceof String)
            where.append((String) keyValues.get(ConsistencyTable.tblColumnIdx)
                    .getValue());
        else
            where.append(new String((byte[]) keyValues.get(
                    ConsistencyTable.tblColumnIdx).getValue()));

        where.append("' AND ");
        where.append(ConsistencyTable.idColumnName);
        where.append(" = ");
        where.append((Integer) keyValues.get(ConsistencyTable.idColumnIdx)
                .getValue());

        // Set the db name as the shard ID unless we don't have it, in which
        // case we need to set the default shard ID.
        if (dbName == null)
        {
            event.getDBMSEvent().setMetaDataOption(ReplOptionParams.SHARD_ID,
                    ReplOptionParams.SHARD_ID_UNKNOWN);
        }
        else
        {
            event.getDBMSEvent().setMetaDataOption(ReplOptionParams.SHARD_ID,
                    dbName);
        }

        // Assign the where clause.
        if (where != null)
            event.getDBMSEvent().addMetadataOption(
                    ReplOptionParams.CONSISTENCY_WHERE, where.toString());
    }
}