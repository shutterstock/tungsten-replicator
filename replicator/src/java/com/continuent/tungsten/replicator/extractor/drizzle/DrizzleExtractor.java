/**
 * Tungsten: An Application Server for uni/cluster.
 * Copyright (C) 2007-2009 Continuent Inc.
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
 * Initial developer(s): Marcus Eriksson
 * INITIAL CODE DONATED UNDER TUNGSTEN CODE CONTRIBUTION AGREEMENT
 */
package com.continuent.tungsten.replicator.extractor.drizzle;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.extractor.ExtractorException;
import com.continuent.tungsten.replicator.extractor.RawExtractor;
import com.continuent.tungsten.replicator.extractor.drizzle.messages.TableMessage;
import com.continuent.tungsten.replicator.extractor.drizzle.messages.TransactionMessage;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.google.protobuf.ByteString;

/**
 * An extractor for the drizzle transaction log.
 * 
 * @author Marcus Eriksson (krummas@gmail.com)
 */
public class DrizzleExtractor implements RawExtractor {
    private static final Logger logger = Logger.getLogger(DrizzleExtractor.class);
    private FileChannel fileChannel;
    private FileInputStream fileInputStream;

    private long lastTransactionId=0;
    private static final long FILE_READ_SLEEP_TIME_MILLIS = 100;

    public DrizzleExtractor() {

    }
    /**
     * Set the value of the last event ID we have processed. The extractor is responsible for returning the next event ID
     * in sequence after this one the next time extract() is called.
     *
     * @param eventId Event ID at which to begin extracting
     * @throws com.continuent.tungsten.replicator.extractor.ExtractorException
     *
     */
    public void setLastEventId(String eventId) throws ExtractorException {
        logger.info("setLastEventId="+eventId);
        if(eventId != null) {
            lastTransactionId = Long.parseLong(eventId);
        } else {
            lastTransactionId = 0;
        }

    }

    /**
     * Extract the next available DBMSEvent from the database log.
     * If there are no transactions in the log, the method blocks.
     *
     * @return next DBMSEvent found in the logs
     */
    public DBMSEvent extract() throws ExtractorException, InterruptedException {
        logger.debug("in extract()");
        TransactionMessage.Transaction transaction;
        try {
            do {
                transaction = getNextTransaction();

                if(logger.isDebugEnabled()) {
                    logger.debug("Found transaction in transaction log: "+transaction);
                }

            } while(transaction.getTransactionContext().getTransactionId() < lastTransactionId 
                    || transactionTouchesTungsten(transaction));

        } catch (IOException e) {
            throw new ExtractorException(e.getMessage(), e);
        }

        lastTransactionId = transaction.getTransactionContext().getTransactionId();
        logger.debug("Got transaction ID = "+lastTransactionId);
        ArrayList<DBMSData> returnList = new ArrayList<DBMSData>(transaction.getStatementCount());

        for(TransactionMessage.Statement statement : transaction.getStatementList()) {
            returnList.add(getDBMSData(statement));
        }
        return new DBMSEvent(Long.toString(lastTransactionId), returnList, new Timestamp(System.currentTimeMillis()));
    }

    /**
     * This filters away transactions that touch the tungsten database, otherwise we get inf. loops.
     *
     * It is assumed that a transaction that touches tungsten only touches that DB.
     *
     * @param transaction the transaction to check.
     * @return true if the txn touches the tungsten database.
     */
    private boolean transactionTouchesTungsten(TransactionMessage.Transaction transaction) {
        for(TransactionMessage.Statement stmt : transaction.getStatementList()) {
            if(touchesTungstenDB(stmt)) {
                logger.debug("Transaction touched tungsten database, not replicating");
                return true;
            }
        }
        return false;
    }

    private boolean touchesTungstenDB(TransactionMessage.Statement statement) {
        switch(statement.getType()) {
            case INSERT:
                return statement.getInsertHeader().getTableMetadata().getSchemaName().toLowerCase().equals("tungsten");
            case DELETE:
                return statement.getDeleteHeader().getTableMetadata().getSchemaName().toLowerCase().equals("tungsten");
            case UPDATE:
                return statement.getUpdateHeader().getTableMetadata().getSchemaName().toLowerCase().equals("tungsten");
            case RAW_SQL:
                return statement.getSql().toLowerCase().contains("tungsten");
        }
        return false;
    }

    /**
     * Builds the dbms data for the statement.
     *
     * TODO: not every statement type that exists is implemented yet, but the ones that are used are.
     * @param statement the transaction log statement.
     * @return a dbms data (either StatementData or RowChangeData)
     */
    private DBMSData getDBMSData(TransactionMessage.Statement statement) {
        switch(statement.getType()) {
            case INSERT:
                return buildInsertData(statement);
            case DELETE:
                return buildDeleteData(statement);
            case UPDATE:
                return buildUpdateData(statement);
            case RAW_SQL:
                return new StatementData(statement.getSql());
            case SET_VARIABLE:
                return new StatementData(createSetStatement(statement.getSetVariableStatement()));
            case DROP_SCHEMA:
                return new StatementData(createDropSchemaStatement(statement.getDropSchemaStatement()));
            case DROP_TABLE:
                return new StatementData(createDropTableStatement(statement.getDropTableStatement()));
            case TRUNCATE_TABLE:
                return new StatementData(createTruncateTableStatement(statement.getTruncateTableStatement()));
        }
        return null;
    }

    /**
     * Create a truncate table statement.
     * @param truncateTableStatement the statement from the txn log.
     * @return a truncate table statement.
     */
    private String createTruncateTableStatement(TransactionMessage.TruncateTableStatement truncateTableStatement) {
        String schemaName = truncateTableStatement.getTableMetadata().getSchemaName();
        String tableName = truncateTableStatement.getTableMetadata().getTableName();
        return "TRUNCATE TABLE `"+schemaName+"`.`"+tableName+"`";
    }

    /**
     * Create a drop table statement.
     * @param dropTableStatement the txn log statement.
     * @return the sql to drop a table.
     */
    private String createDropTableStatement(TransactionMessage.DropTableStatement dropTableStatement) {
        String schemaName = dropTableStatement.getTableMetadata().getSchemaName();
        String tableName = dropTableStatement.getTableMetadata().getTableName();
        return "DROP TABLE `"+schemaName+"`.`"+tableName+"`";
    }

    /**
     * Create a drop schema statement.
     * @param dropSchemaStatement the txn log statement.
     * @return the sql to drop a schema.
     */
    private String createDropSchemaStatement(TransactionMessage.DropSchemaStatement dropSchemaStatement) {
        String schemaName = dropSchemaStatement.getSchemaName();
        return "DROP SCHEMA `"+schemaName+"`";
    }

    /**
     * Create a SET statement.
     * @param setVariableStatement the txn log statement.
     * @return sql to set a variable.
     */
    private String createSetStatement(TransactionMessage.SetVariableStatement setVariableStatement) {
        String variableName = setVariableStatement.getVariableMetadata().getName();
        String variableValue = setVariableStatement.getVariableValue().toStringUtf8();
        StringBuilder builder = new StringBuilder("SET ");
        return builder.append(variableName)
                      .append(" = ")
                      .append("\"")
                      .append(variableValue)
                      .append("\"")
                .toString();
    }

    /**
     * Takes an insert statement and builds the correct replicator RowChangeData:
     *
     * A statement contains an insert header and an insert data. An insert data contains
     * a series of insert records. Each of these records maps against one OneRowChange. The
     * columnSpecs for each OneRowChange are taken from the header, and they all look the
     * same. The columnVals are taken from the records.
     *
     * @param statement a statement from the transaction log.
     * @return a rowchange data, containing row changes.
     */
    private RowChangeData buildInsertData(TransactionMessage.Statement statement) {
        RowChangeData data = new RowChangeData();
        TransactionMessage.InsertHeader insertHeader = statement.getInsertHeader();
        TransactionMessage.InsertData insertData = statement.getInsertData();
        TransactionMessage.TableMetadata tableMetadata = insertHeader.getTableMetadata();
        String schemaName = tableMetadata.getSchemaName();
        String tableName = tableMetadata.getTableName();

        int recordCount = insertData.getRecordCount();
        ArrayList<OneRowChange> rowChanges = new ArrayList<OneRowChange>(recordCount);

        for(TransactionMessage.InsertRecord insertRecord : insertData.getRecordList()) {
            OneRowChange change = new OneRowChange(schemaName, tableName, RowChangeData.ActionType.INSERT);
            int fieldCount = insertRecord.getInsertValueCount();
            ArrayList<OneRowChange.ColumnSpec> columnSpecs = new ArrayList<OneRowChange.ColumnSpec>(fieldCount);
            ArrayList<OneRowChange.ColumnVal> columnValues = new ArrayList<OneRowChange.ColumnVal>(fieldCount);

            for(int i = 0; i < fieldCount; i++) {
                TransactionMessage.FieldMetadata fieldMetaData = insertHeader.getFieldMetadata(i);
                columnSpecs.add(convertFieldMetaData(fieldMetaData));
                columnValues.add(convertSetData(insertRecord.getInsertValue(i)));
            }
            change.setColumnSpec(columnSpecs);
            ArrayList<ArrayList<OneRowChange.ColumnVal>> valHolder = new ArrayList<ArrayList<OneRowChange.ColumnVal>>();
            valHolder.add(columnValues);
            change.setColumnValues(valHolder);
            rowChanges.add(change);

        }
        data.setRowChanges(rowChanges);
        return data;
    }

    /**
     * Builds row change data for a delete statement. Drizzle txn log stores a DeleteHeader which contains
     * the meta data about the fields used in the where statement and a DeleteData which contains the
     * values in the WHERE. If a delete statement matches several rows, several DeleteRecords appear in the
     * DeleteData.
     *
     * @param statement the drizzle transaction statement to convert
     * @return a set of OneRowChanges in a RowChangeData.
     */
    private RowChangeData buildDeleteData(TransactionMessage.Statement statement) {
        RowChangeData data = new RowChangeData();
        TransactionMessage.DeleteHeader deleteHeader = statement.getDeleteHeader();
        TransactionMessage.DeleteData deleteData = statement.getDeleteData();
        TransactionMessage.TableMetadata tableMetadata = deleteHeader.getTableMetadata();
        String schemaName = tableMetadata.getSchemaName();
        String tableName = tableMetadata.getTableName();

        int recordCount = deleteData.getRecordCount();
        ArrayList<OneRowChange> rowChanges = new ArrayList<OneRowChange>(recordCount);

        for(TransactionMessage.DeleteRecord deleteRecord : deleteData.getRecordList()) {
            OneRowChange change = new OneRowChange(schemaName, tableName, RowChangeData.ActionType.DELETE);
            int deleteKeyCount = deleteRecord.getKeyValueCount();

            ArrayList<OneRowChange.ColumnSpec> keyColumnSpecs = new ArrayList<OneRowChange.ColumnSpec>(deleteKeyCount);
            ArrayList<OneRowChange.ColumnVal> keyColumnValues = new ArrayList<OneRowChange.ColumnVal>(deleteKeyCount);

            for(int i = 0; i<deleteKeyCount; i++) {
                TransactionMessage.FieldMetadata keyFieldMetaData = deleteHeader.getKeyFieldMetadata(i);
                keyColumnSpecs.add(convertFieldMetaData(keyFieldMetaData));
                keyColumnValues.add(convertSetData(deleteRecord.getKeyValue(i)));
            }
            change.setKeySpec(keyColumnSpecs);
            ArrayList<ArrayList<OneRowChange.ColumnVal>> keyValHolder = new ArrayList<ArrayList<OneRowChange.ColumnVal>>();
            keyValHolder.add(keyColumnValues);
            change.setKeyValues(keyValHolder);
            rowChanges.add(change);
        }
        data.setRowChanges(rowChanges);
        return data;
    }

    /**
     * Builds a RowChangeData for an UPDATE statement.
     *
     * Drizzle txn log statements for UPDATEs contain an UpdateHeader which describes the
     * key field names and the value field names. It also contains an UpdateData which contains
     * a list of UpdateRecords. The UpdateRecords contain values for the keys from the header
     * and values for the fields.
     *
     * @param statement the statement to convert.
     * @return a RowChangeData containing a set of updates.
     */
    private RowChangeData buildUpdateData(TransactionMessage.Statement statement) {
        RowChangeData data = new RowChangeData();
        TransactionMessage.UpdateHeader updateHeader = statement.getUpdateHeader();
        String schemaName = updateHeader.getTableMetadata().getSchemaName();
        String tableName = updateHeader.getTableMetadata().getTableName();

        TransactionMessage.UpdateData updateData = statement.getUpdateData();

        int recordCount = updateData.getRecordCount();

        ArrayList<OneRowChange> rowChanges = new ArrayList<OneRowChange>(recordCount);

        // for every update record (one per row affected) ...
        for(TransactionMessage.UpdateRecord updateRecord : updateData.getRecordList()) {
            OneRowChange change = new OneRowChange(schemaName, tableName, RowChangeData.ActionType.UPDATE);
            int updateFieldCount = updateRecord.getAfterValueCount();

            ArrayList<OneRowChange.ColumnSpec> keyColumnSpecs = new ArrayList<OneRowChange.ColumnSpec>(updateFieldCount);
            ArrayList<OneRowChange.ColumnSpec> columnSpecs = new ArrayList<OneRowChange.ColumnSpec>(updateFieldCount);
            ArrayList<OneRowChange.ColumnVal> keyColumnValues = new ArrayList<OneRowChange.ColumnVal>(updateFieldCount);
            ArrayList<OneRowChange.ColumnVal> columnValues = new ArrayList<OneRowChange.ColumnVal>(updateFieldCount);

            // ... and for every field in that record
            for(int i=0;i<updateFieldCount; i++) {
                TransactionMessage.FieldMetadata keyFieldMetaData = updateHeader.getKeyFieldMetadata(i);
                TransactionMessage.FieldMetadata fieldMetaData = updateHeader.getSetFieldMetadata(i);
                keyColumnSpecs.add(convertFieldMetaData(keyFieldMetaData));
                columnSpecs.add(convertFieldMetaData(fieldMetaData));
                keyColumnValues.add(convertSetData(updateRecord.getKeyValue(i)));
                columnValues.add(convertSetData(updateRecord.getAfterValue(i)));
            }
            change.setKeySpec(keyColumnSpecs);
            change.setColumnSpec(columnSpecs);
            ArrayList<ArrayList<OneRowChange.ColumnVal>> keyValHolder = new ArrayList<ArrayList<OneRowChange.ColumnVal>>();
            keyValHolder.add(keyColumnValues);
            change.setKeyValues(keyValHolder);
            ArrayList<ArrayList<OneRowChange.ColumnVal>> valHolder = new ArrayList<ArrayList<OneRowChange.ColumnVal>>();
            valHolder.add(columnValues);
            change.setColumnValues(valHolder);
            rowChanges.add(change);
        }
        data.setRowChanges(rowChanges);
        return data;
    }

    /**
     * Creates a ColumnVal from a ByteString. Currently all data is stored as
     * strings in the transaction log.
     * @param afterValue the value to convert.
     * @return the data as a ColumnVal
     */
    private OneRowChange.ColumnVal convertSetData(ByteString afterValue) {
        // Needed since OneRowChange contains a non-static class for ColumnVal
        OneRowChange change = new OneRowChange();
        OneRowChange.ColumnVal columnVal = change.new ColumnVal();
        columnVal.setValue(afterValue.toStringUtf8());
        return columnVal;
    }

    /**
     * Converts a field meta data description from the drizzle transaction log to a ColumnSpec.
     * 
     * @param fieldMetaData the meta data to convert
     * @return a ColumnSpec containing the data.
     */
    private OneRowChange.ColumnSpec convertFieldMetaData(TransactionMessage.FieldMetadata fieldMetaData) {
        // needed since non-static nested classes are used:
        OneRowChange change = new OneRowChange();

        OneRowChange.ColumnSpec colSpec = change.new ColumnSpec();
        colSpec.setNotNull(true); // TODO: FIX when the txn log supports it.
        colSpec.setLength(0); // TODO: FIX
        colSpec.setName(fieldMetaData.getName());
        colSpec.setType(drizzleTypeToSQLType(fieldMetaData.getType()));
        return colSpec;

    }

    /**
     * Convert a drizzle type to a java.sql.Types type.
     * @param type the drizzle type to convert.
     * @return a java.sql.Types type
     *  */

    private int drizzleTypeToSQLType(TableMessage.Table.Field.FieldType type) {
        switch(type) {
            case BIGINT:
                return java.sql.Types.BIGINT;
            case BLOB:
                return java.sql.Types.BLOB;
            case DATE:
                return java.sql.Types.DATE;
            case DATETIME:
                return java.sql.Types.TIMESTAMP;
            case DECIMAL:
                return java.sql.Types.DECIMAL;
            case DOUBLE:
                return java.sql.Types.DOUBLE;
            case ENUM:
                return java.sql.Types.VARCHAR;
            case INTEGER:
                return java.sql.Types.INTEGER;
            case TIME:
                return java.sql.Types.TIME;
            case TIMESTAMP:
                return java.sql.Types.TIMESTAMP;
            case VARCHAR:
                return java.sql.Types.VARCHAR;
            default:
                return java.sql.Types.VARCHAR;                
        }
    }

    private TransactionMessage.Transaction getNextTransaction() throws IOException, InterruptedException {
        int length = readLength();
        while(fileInputStream.available() < length) {
            Thread.sleep(FILE_READ_SLEEP_TIME_MILLIS);
        }
        ByteBuffer bb = ByteBuffer.allocate(length);
        fileChannel.read(bb);
        TransactionMessage.Transaction cmd = TransactionMessage.Transaction.parseFrom(bb.array());
        ByteBuffer crcBuffer = ByteBuffer.allocate(4);
        fileChannel.read(crcBuffer); // ignored for now
        return cmd;
    }

    private int readLength() throws IOException, InterruptedException {
        ByteBuffer bb = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);

        while(fileInputStream.available() < 8) {
            Thread.sleep(FILE_READ_SLEEP_TIME_MILLIS);
        }

        @SuppressWarnings("unused")
        int readLen = fileChannel.read(bb);
        bb.flip();
        @SuppressWarnings("unused")
        int entryType = bb.getInt(); // TODO: entry type is currently ignored

        return bb.getInt();
    }

    /**
     * Extract starting af2ter the event ID provided as an argument. This is equivalent to invoking setLastEventId()
     * followed by extract().
     *
     * @param eventId Event ID at which to begin extracting
     * @return DBMSEvent corresponding to the id
     * @throws com.continuent.tungsten.replicator.extractor.ExtractorException
     *                              Thrown if extractor processing fails
     * @throws InterruptedException Thrown if the applier is interrupted
     */
    public DBMSEvent extract(String eventId) throws ExtractorException, InterruptedException {
        setLastEventId(eventId);
        return extract();
    }

    /**
     * Returns the last event ID committed in the database from which we are extracting. It is used to help synchronize
     * state between the database and the transaction history log.  Values returned from this call must correspond with the
     * last extracted DBMSEvent.eventId as follows: <ol> <li>If the returned value is greater than DBMSEvent.eventId, the
     * database has more recent updates</li> <li>If the returned value is equal to DBMSEvent.eventId, all events have been
     * extracted</li> </ol> It should not be possible to receive a value that is less than the last extracted
     * DBMSEvent.eventId as this implies that the extractor is somehow ahead of the state of the database, which would be
     * inconsistent.
     *
     * @return A current event ID that can be compared with event IDs in DBMSEvent
     * @throws com.continuent.tungsten.replicator.extractor.ExtractorException
     *
     * @throws InterruptedException
     */
    public String getCurrentResourceEventId() throws ExtractorException, InterruptedException {
        // TODO: when INFORMATION_SCHEMA contains max transaction id, use that
        //       until then, assume that when there is data available in the file, there is
        //       at least one more transaction available.
        try {
            if(fileInputStream.available() > 0) {
                return Long.toString(lastTransactionId+1);
            }
        } catch (IOException e) {
            throw new ExtractorException(e.getMessage(), e);
        }
        return Long.toString(lastTransactionId);
    }

    /**
     * Returns a Properties instance containing all data required for a JDBC login. The Properties must have the following
     * data: <table> <theader> <tr> <td>Key</td> <td>Value</td> </tr> </theader> <tbody> <tr> <td>url</td> <td>Valid JDBC
     * URL to connect to tungsten schema</td> </tr> <tr> <td>user</td> <td>DBMS user name</td> </tr> <tr> <td>password</td>
     * <td>DBMS password</td> </tr> </tbody> </table>
     *
     * @return Properties instance or null if this source does not support JDBC
     */
    public Properties getJdbcConnectionProperties() {
        return null; // do not use jdbc yet.
    }

    /**
     * Complete plug-in configuration.  This is called after setters are invoked at the time that the replicator goes
     * through configuration.
     *
     * @throws com.continuent.tungsten.replicator.ReplicatorException
     *          Thrown if configuration is incomplete or fails
     */
    public void configure(PluginContext context) throws ReplicatorException {
        logger.info("in configure()" + context);
    }

    /**
     * Prepare plug-in for use. This method is assumed to allocate all required resources.  It is called before the plug-in
     * performs any operations.
     *
     * @throws com.continuent.tungsten.replicator.ReplicatorException
     *          Thrown if resource allocation fails
     */
    public void prepare(PluginContext context) throws ReplicatorException {
        logger.info("In prepare --- "+ context);
    }

    /**
     * Release all resources used by plug-in.  This is called before the plug-in is deallocated.
     *
     * @throws com.continuent.tungsten.replicator.ReplicatorException
     *          Thrown if resource deallocation fails
     */
    public void release(PluginContext context) throws ReplicatorException {
        //To change body of implemented methods use File | Settings | File Templates.
    }


    /**
     * Sets the path to the transaction log
     *
     * @param log path to the log
     * @throws FileNotFoundException if the file is not found...
     */
    public void setTransactionLog(String log) throws FileNotFoundException {
        fileInputStream = new FileInputStream(log);
        fileChannel = fileInputStream.getChannel();
        logger.info("Using log: "+log);
    }
}
