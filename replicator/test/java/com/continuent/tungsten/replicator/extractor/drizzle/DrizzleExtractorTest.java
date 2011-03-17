/**
 * Tungsten: An Application Server for uni/cluster.
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
 * Initial developer(s): Marcus Eriksson
 * INITIAL CODE DONATED UNDER TUNGSTEN CODE CONTRIBUTION AGREEMENT
 */

package com.continuent.tungsten.replicator.extractor.drizzle;

import static junit.framework.Assert.assertEquals;

import java.io.FileNotFoundException;
import java.util.ArrayList;

import org.apache.log4j.BasicConfigurator;
import org.junit.Test;

import com.continuent.tungsten.commons.config.TungstenProperties;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.applier.DrizzleApplier;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.conf.ReplicatorMonitor;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.StatementData;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.extractor.ExtractorWrapper;
import com.continuent.tungsten.replicator.management.MockOpenReplicatorContext;
import com.continuent.tungsten.replicator.pipeline.Pipeline;
import com.continuent.tungsten.replicator.pipeline.SingleThreadStageTask;

/**
 * Created by IntelliJ IDEA. User: marcuse Date: Oct 22, 2009 Time: 5:23:08 PM
 * To change this template use File | Settings | File Templates.
 */

public class DrizzleExtractorTest
{
    public DrizzleExtractorTest()
    {
        BasicConfigurator.configure();
    }

    @Test
    public void testInsertExtraction() throws FileNotFoundException,
            ReplicatorException, InterruptedException
    {
        DrizzleExtractor extractor = getExtractor("insert.txnlog");

        DBMSEvent event = extractor.extract();
        assertEquals(1, event.getData().size());
        RowChangeData rowData = (RowChangeData) event.getData().get(0);
        assertEquals(2, rowData.getRowChanges().size());
        OneRowChange ch = rowData.getRowChanges().get(0);
        OneRowChange ch2 = rowData.getRowChanges().get(1);
        String query = constructStatement(ch.getAction(), ch.getSchemaName(),
                ch.getTableName(), ch.getColumnSpec(), ch.getKeySpec(), null)
                .toString();
        String query2 = constructStatement(ch2.getAction(),
                ch2.getSchemaName(), ch2.getTableName(), ch2.getColumnSpec(),
                ch2.getKeySpec(), null).toString();
        assertEquals(query, query2);
        assertEquals(
                "INSERT INTO unittests.test1 ( id , test )  VALUES (  ?  ,  ?  ) ",
                query);

        ArrayList<ArrayList<OneRowChange.ColumnVal>> columnValues1 = ch
                .getColumnValues();
        assertEquals(1, columnValues1.size());
        ArrayList<OneRowChange.ColumnVal> vals1 = columnValues1.get(0);
        assertEquals(2, vals1.size());
        OneRowChange.ColumnVal val11 = vals1.get(0);
        OneRowChange.ColumnVal val12 = vals1.get(1);

        assertEquals("1", val11.getValue());
        assertEquals("one", val12.getValue());
        ArrayList<ArrayList<OneRowChange.ColumnVal>> columnValues2 = ch2
                .getColumnValues();
        assertEquals(1, columnValues2.size());
        ArrayList<OneRowChange.ColumnVal> vals2 = columnValues2.get(0);
        assertEquals(2, vals2.size());
        OneRowChange.ColumnVal val21 = vals2.get(0);
        OneRowChange.ColumnVal val22 = vals2.get(1);

        assertEquals("2", val21.getValue());
        assertEquals("two", val22.getValue());
    }

    @Test
    public void testDeleteExtraction() throws FileNotFoundException,
            ReplicatorException, InterruptedException
    {
        DrizzleExtractor extractor = getExtractor("delete.txnlog");

        DBMSEvent event = extractor.extract();
        assertEquals(1, event.getData().size());
        RowChangeData rowData = (RowChangeData) event.getData().get(0);
        assertEquals(1, rowData.getRowChanges().size());
        OneRowChange ch = rowData.getRowChanges().get(0);
        String query = constructStatement(ch.getAction(), ch.getSchemaName(),
                ch.getTableName(), ch.getColumnSpec(), ch.getKeySpec(), null)
                .toString();
        assertEquals("DELETE FROM unittests.test1 WHERE id = ? ", query);

        ArrayList<ArrayList<OneRowChange.ColumnVal>> keyVals = ch
                .getKeyValues();
        assertEquals(1, keyVals.size());
        assertEquals(1, keyVals.get(0).size());
        OneRowChange.ColumnVal keyVal = keyVals.get(0).get(0);
        assertEquals("2", keyVal.getValue());
    }

    @Test
    public void testUpdateExtraction() throws FileNotFoundException,
            ReplicatorException, InterruptedException
    {
        DrizzleExtractor extractor = getExtractor("update.txnlog");
        DBMSEvent event = extractor.extract();
        assertEquals(1, event.getData().size());
        RowChangeData rowData = (RowChangeData) event.getData().get(0);
        assertEquals(1, rowData.getRowChanges().size());
        OneRowChange ch = rowData.getRowChanges().get(0);
        String query = constructStatement(ch.getAction(), ch.getSchemaName(),
                ch.getTableName(), ch.getColumnSpec(), ch.getKeySpec(), null)
                .toString();
        assertEquals("UPDATE unittests.test1 SET test = ?  WHERE id = ? ",
                query);
        ArrayList<ArrayList<OneRowChange.ColumnVal>> keyVals = ch
                .getKeyValues();
        assertEquals(1, keyVals.size());
        assertEquals(1, keyVals.get(0).size());
        OneRowChange.ColumnVal keyVal = keyVals.get(0).get(0);
        assertEquals("2", keyVal.getValue());
        ArrayList<ArrayList<OneRowChange.ColumnVal>> columnValues1 = ch
                .getColumnValues();
        assertEquals(1, columnValues1.size());
        ArrayList<OneRowChange.ColumnVal> vals1 = columnValues1.get(0);
        assertEquals(1, vals1.size());
        OneRowChange.ColumnVal val11 = vals1.get(0);
        assertEquals("updated", val11.getValue());
    }

    @Test
    public void testCreateTableExtraction() throws FileNotFoundException,
            ReplicatorException, InterruptedException
    {
        DrizzleExtractor extractor = getExtractor("create_table.txnlog");
        DBMSEvent event = extractor.extract();
        assertEquals(1, event.getData().size());
        StatementData rowData = (StatementData) event.getData().get(0);
        assertEquals(
                "create table test1 (id int primary key, test varchar(10))",
                rowData.getQuery());
    }

    @Test
    public void testCreateSchemaExtraction() throws FileNotFoundException,
            ReplicatorException, InterruptedException
    {
        DrizzleExtractor extractor = getExtractor("create_schema.txnlog");
        DBMSEvent event = extractor.extract();
        assertEquals(1, event.getData().size());
        StatementData rowData = (StatementData) event.getData().get(0);
        System.out.println(rowData.getQuery());
        assertEquals("create database unittests", rowData.getQuery());
    }

    private DrizzleExtractor getExtractor(String transactionLog)
            throws ReplicatorException
    {
        TungstenProperties conf = new TungstenProperties();
        conf.setString(ReplicatorConf.ROLE, ReplicatorConf.ROLE_MASTER);
        conf.setString(ReplicatorConf.SERVICE_NAME, "test");

        conf.setString(ReplicatorConf.ROLE, ReplicatorConf.ROLE_MASTER);
        conf.setString(ReplicatorConf.PIPELINES, "master");
        conf.setString(ReplicatorConf.PIPELINE_ROOT + ".master", "extract");
        conf.setString(ReplicatorConf.STAGE_ROOT + ".extract",
                SingleThreadStageTask.class.toString());
        conf.setString(ReplicatorConf.STAGE_ROOT + ".extract.extractor",
                "drizzle");
        conf.setString(ReplicatorConf.STAGE_ROOT + ".extract.applier",
                "drizzle");

        conf.setString(ReplicatorConf.APPLIER_ROOT + ".drizzle",
                DrizzleApplier.class.getName());
        conf.setString(ReplicatorConf.EXTRACTOR_ROOT + ".drizzle",
                DrizzleExtractor.class.getName());
        conf.setString(ReplicatorConf.EXTRACTOR_ROOT
                + ".drizzle.transaction_log", transactionLog);

        // Configure runtime with these properties and prepare the
        // extractor for use.
        ReplicatorRuntime runtime = new ReplicatorRuntime(conf,
                new MockOpenReplicatorContext(), ReplicatorMonitor
                        .getInstance());
        runtime.configure();

        Pipeline p = runtime.getPipeline();
        ExtractorWrapper wrapper = (ExtractorWrapper) p.getStages().get(0)
                .getExtractor0();
        return (DrizzleExtractor) wrapper.getExtractor();
    }

    private StringBuffer constructStatement(RowChangeData.ActionType action,
            String schemaName, String tableName,
            ArrayList<OneRowChange.ColumnSpec> columns,
            ArrayList<OneRowChange.ColumnSpec> keys,
            ArrayList<OneRowChange.ColumnVal> keyValues)
    {
        StringBuffer stmt = new StringBuffer();
        if (action == RowChangeData.ActionType.INSERT)
        {
            stmt.append("INSERT INTO ");
            stmt.append(schemaName + "." + tableName);
            stmt.append(" ( ");
            printColumnSpec(stmt, columns, null, PrintMode.NAMES_ONLY, " , ");
            stmt.append(" ) ");
            stmt.append(" VALUES ( ");
            printColumnSpec(stmt, columns, null, PrintMode.PLACE_HOLDER, " , ");
            stmt.append(" ) ");
        }
        else if (action == RowChangeData.ActionType.UPDATE)
        {
            stmt.append("UPDATE ");
            stmt.append(schemaName + "." + tableName);
            stmt.append(" SET ");
            printColumnSpec(stmt, columns, null, PrintMode.ASSIGNMENT, " , ");
            stmt.append(" WHERE ");
            printColumnSpec(stmt, keys, keyValues, PrintMode.ASSIGNMENT,
                    " AND ");
        }
        else if (action == RowChangeData.ActionType.DELETE)
        {
            stmt.append("DELETE FROM ");
            stmt.append(schemaName + "." + tableName);
            stmt.append(" WHERE ");
            printColumnSpec(stmt, keys, keyValues, PrintMode.ASSIGNMENT,
                    " AND ");
        }
        else
        {
            System.out.println("DOH" + action);
        }
        return stmt;
    }

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
                if (keyValues != null && keyValues.get(i).getValue() == null)
                {
                    // TREP-276: use "IS NULL" vs. "= NULL"
                    stmt.append(col.getName() + " IS ? ");
                }
                else
                    stmt.append(col.getName() + " = ? ");
            }
            else if (mode == PrintMode.PLACE_HOLDER)
            {
                stmt.append(" ? ");
            }
            else if (mode == PrintMode.NAMES_ONLY)
            {
                stmt.append(col.getName());
            }
        }
    }

    enum PrintMode
    {
        ASSIGNMENT, NAMES_ONLY, VALUES_ONLY, PLACE_HOLDER
    }
}
