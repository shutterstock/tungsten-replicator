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
 * Initial developer(s): Teemu Ollakka
 * Contributor(s): Robert Hodges, Stephane Giron
 */

package com.continuent.tungsten.replicator.applier;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.consistency.ConsistencyException;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.LoadDataFileFragment;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.RowIdData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;
import com.continuent.tungsten.replicator.dbms.RowChangeData.ActionType;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSHeader;
import com.continuent.tungsten.replicator.event.ReplDBMSHeaderData;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

/**
 * Implements an applier for MongoDB. The MongoDB applier This class defines a
 * MongoApplier
 * 
 * @author <a href="mailto:jussi-pekka.kurikka@continuent.com">Jussi-Pekka
 *         Kurikka</a>
 * @version 1.0
 */
public class MongoApplier implements RawApplier
{
    private static Logger  logger        = Logger.getLogger(MongoApplier.class);

    // Task management information.
    private int            taskId;
    private String         serviceSchema;

    // Latest event.
    private ReplDBMSHeader latestHeader;

    // Parameters for the extractor.
    private String         connectString = null;

    // Private connection management.
    private Mongo          m;

    /** Set the MongoDB connect string, e.g., "myhost:27071". */
    public void setConnectString(String connectString)
    {
        this.connectString = connectString;
    }

    /**
     * Applies row updates to MongoDB. Statements are discarded. {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.RawApplier#apply(com.continuent.tungsten.replicator.event.DBMSEvent,
     *      com.continuent.tungsten.replicator.event.ReplDBMSHeader, boolean, boolean)
     */
    @Override
    public void apply(DBMSEvent event, ReplDBMSHeader header, boolean doCommit, boolean doRollback)
            throws ApplierException, ConsistencyException, InterruptedException
    {
        ArrayList<DBMSData> dbmsDataValues = event.getData();

        // Iterate through values inferring the database name.
        for (DBMSData dbmsData : dbmsDataValues)
        {
            if (dbmsData instanceof StatementData)
            {
                if (logger.isDebugEnabled())
                    logger.debug("Ignoring statement");
            }
            else if (dbmsData instanceof RowChangeData)
            {
                RowChangeData rd = (RowChangeData) dbmsData;
                for (OneRowChange orc : rd.getRowChanges())
                {
                    ActionType action = orc.getAction();
                    String schema = orc.getSchemaName();
                    String table = orc.getTableName();
                    if (logger.isDebugEnabled())
                    {
                        logger.debug("Processing row update: action=" + action
                                + " schema=" + schema + " table=" + table);
                    }

                    if (action.equals(ActionType.INSERT))
                    {
                        // Connect to the schema and collection.
                        DB db = m.getDB(schema);
                        DBCollection coll = db.getCollection(table);

                        // Fetch column names.
                        List<ColumnSpec> colSpecs = orc.getColumnSpec();

                        // Make a document and insert for each row.
                        Iterator<ArrayList<ColumnVal>> colValues = orc
                                .getColumnValues().iterator();
                        BasicDBObject doc = new BasicDBObject();
                        while (colValues.hasNext())
                        {
                            ArrayList<ColumnVal> row = colValues.next();
                            for (int i = 0; i < row.size(); i++)
                            {
                                String name = colSpecs.get(i).getName();
                                doc.put(name, row.get(i).getValue().toString());
                            }
                        }
                        if (logger.isDebugEnabled())
                            logger.debug("Adding document: doc="
                                    + doc.toString());
                        coll.insert(doc);
                    }
                    else if (action.equals(ActionType.UPDATE))
                    {
                        // Connect to the schema and collection.
                        DB db = m.getDB(schema);
                        DBCollection coll = db.getCollection(table);

                        // Fetch key and column names.
                        List<ColumnSpec> keySpecs = orc.getKeySpec();
                        List<ColumnSpec> colSpecs = orc.getColumnSpec();
                        ArrayList<ArrayList<OneRowChange.ColumnVal>> keyValues = orc
                                .getKeyValues();
                        ArrayList<ArrayList<OneRowChange.ColumnVal>> columnValues = orc
                                .getColumnValues();

                        // Iterate across the rows.
                        for (int row = 0; row < columnValues.size()
                                || row < keyValues.size(); row++)
                        {
                            List<ColumnVal> keyValuesOfRow = keyValues.get(row);
                            List<ColumnVal> colValuesOfRow = columnValues
                                    .get(row);

                            // Prepare key values query to search for rows.
                            DBObject query = new BasicDBObject();
                            for (int i = 0; i < keyValuesOfRow.size(); i++)
                            {
                                String name = keySpecs.get(i).getName();
                                query.put(name, keyValuesOfRow.get(i)
                                        .getValue().toString());
                            }

                            BasicDBObject doc = new BasicDBObject();
                            for (int i = 0; i < colValuesOfRow.size(); i++)
                            {
                                String name = colSpecs.get(i).getName();
                                doc.put(name, colValuesOfRow.get(i).getValue()
                                        .toString());
                            }
                            if (logger.isDebugEnabled())
                            {
                                logger.debug("Updating document: query="
                                        + query + " doc=" + doc);
                            }
                            DBObject updatedRow = coll
                                    .findAndModify(query, doc);
                            if (logger.isDebugEnabled())
                            {
                                if (updatedRow == null)
                                    logger
                                            .debug("Unable to find document for update: query="
                                                    + query);
                                else
                                    logger.debug("Documented updated: doc="
                                            + doc);
                            }
                        }
                    }
                    else if (action.equals(ActionType.DELETE))
                    {
                        // Connect to the schema and collection.
                        DB db = m.getDB(schema);
                        DBCollection coll = db.getCollection(table);

                        // Fetch key and column names.
                        List<ColumnSpec> keySpecs = orc.getKeySpec();
                        ArrayList<ArrayList<OneRowChange.ColumnVal>> keyValues = orc
                                .getKeyValues();
                        ArrayList<ArrayList<OneRowChange.ColumnVal>> columnValues = orc
                                .getColumnValues();

                        // Iterate across the rows.
                        for (int row = 0; row < columnValues.size()
                                || row < keyValues.size(); row++)
                        {
                            List<ColumnVal> keyValuesOfRow = keyValues.get(row);

                            // Prepare key values query to search for rows.
                            DBObject query = new BasicDBObject();
                            for (int i = 0; i < keyValuesOfRow.size(); i++)
                            {
                                String name = keySpecs.get(i).getName();
                                query.put(name, keyValuesOfRow.get(i)
                                        .getValue().toString());
                            }

                            if (logger.isDebugEnabled())
                            {
                                logger.debug("Deleting document: query="
                                        + query);
                            }
                            DBObject deletedRow = coll.findAndRemove(query);
                            if (logger.isDebugEnabled())
                            {
                                if (deletedRow == null)
                                    logger
                                            .debug("Unable to find document for delete");
                                else
                                    logger.debug("Documented deleted: doc="
                                            + deletedRow);
                            }
                        }
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
    public void commit() throws ApplierException, InterruptedException
    {
        // If we don't have a last header, there is nothing to be done.
        if (latestHeader == null)
        {
            if (logger.isDebugEnabled())
                logger.debug("Unable to commit; last header is null");
            return;
        }

        // Connect to the schema and collection.
        DB db = m.getDB(serviceSchema);
        DBCollection trepCommitSeqno = db.getCollection("trep_commit_seqno");

        // Construct query.
        DBObject query = new BasicDBObject();
        query.put("task_id", taskId);

        // Construct update.
        BasicDBObject doc = new BasicDBObject();
        doc.put("task_id", taskId);
        doc.put("seqno", latestHeader.getSeqno());
        // Short seems to cast to Integer in MongoDB.
        doc.put("fragno", latestHeader.getFragno());
        doc.put("last_frag", latestHeader.getLastFrag());
        doc.put("source_id", latestHeader.getSourceId());
        doc.put("epoch_number", latestHeader.getEpochNumber());
        doc.put("event_id", latestHeader.getEventId());

        // Update trep_commit_seqno.
        DBObject updatedDoc = trepCommitSeqno.findAndModify(query, null, null,
                false, doc, true, true);
        if (logger.isDebugEnabled())
        {
            if (updatedDoc == null)
                logger
                        .debug("Unable to update/insert trep_commit_seqno: query="
                                + query + " doc=" + doc);
            else
                logger.debug("Trep_commit_seqno updated: updatedDoc="
                        + updatedDoc);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.RawApplier#getLastEvent()
     */
    @Override
    public ReplDBMSHeader getLastEvent() throws ApplierException,
            InterruptedException
    {
        // Connect to the schema and collection.
        DB db = m.getDB(serviceSchema);
        DBCollection trepCommitSeqno = db.getCollection("trep_commit_seqno");

        // Construct query.
        DBObject query = new BasicDBObject();
        query.put("task_id", taskId);

        // Find matching trep_commit_seqno value.
        DBObject doc = trepCommitSeqno.findOne(query);

        // Return a constructed header or null, depending on whether we found
        // anything.
        if (doc == null)
        {
            if (logger.isDebugEnabled())
                logger.debug("trep_commit_seqno is empty: taskId=" + taskId);
            return null;
        }
        else
        {
            if (logger.isDebugEnabled())
                logger.debug("trep_commit_seqno entry found: doc=" + doc);

            long seqno = (Long) doc.get("seqno");
            // Cast to integer in MongoDB.
            int fragno = (Integer) doc.get("fragno");
            boolean lastFrag = (Boolean) doc.get("last_frag");
            String sourceId = (String) doc.get("source_id");
            long epochNumber = (Long) doc.get("epoch_number");
            String eventId = (String) doc.get("event_id");
            ReplDBMSHeaderData header = new ReplDBMSHeaderData(seqno,
                    (short) fragno, lastFrag, sourceId, epochNumber, eventId);
            return header;
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.RawApplier#rollback()
     */
    @Override
    public void rollback() throws InterruptedException
    {
        // Does nothing for now.
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.applier.RawApplier#setTaskId(int)
     */
    @Override
    public void setTaskId(int id) throws ApplierException
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
        this.serviceSchema = "tungsten_" + context.getServiceName();
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
        // Connect to MongoDB.
        if (logger.isDebugEnabled())
        {
            logger.debug("Connecting to MongoDB: connectString="
                    + connectString);
        }
        m = null;
        try
        {
            if (connectString == null)
                m = new Mongo();
            else
                m = new Mongo(connectString);
        }
        catch (Exception e)
        {
            throw new ReplicatorException(
                    "Unable to connect to MongoDB: connection="
                            + this.connectString, e);
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
        // Close connection to MongoDB.
        if (m != null)
        {
            m.close();
            m = null;
        }
    }
}