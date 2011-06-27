/**
 * Tungsten: An Application Server for uni/cluster.
 * Copyright (C) 2007-2008 Continuent Inc.
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
 * Initial developer(s): Robert Hodges and Scott Martin
 * Contributor(s): 
 */
package com.continuent.tungsten.replicator.database;


import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.continuent.tungsten.commons.config.TungstenProperties;
import com.continuent.tungsten.replicator.dbms.DBMSData;
import com.continuent.tungsten.replicator.event.DBMSEvent;
import com.continuent.tungsten.replicator.event.ReplDBMSEvent;

/**
 * This class tests the Database interface and associated implementations. 
 * Properties are specified using test.properties.  If test.properties 
 * cannot be found, the test automatically uses Derby database settings. 
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class TestDatabase
{
    private static Logger logger = Logger.getLogger(TestDatabase.class);

    private static String driver;
    private static String url;
    private static String user;
    private static String password;
    private static String schema;

    /**
     * Make sure we have expected test properties.  
     * 
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        // Set test.properties file name. 
        String testPropertiesName = System.getProperty("test.properties");
        if (testPropertiesName == null)
        {
            testPropertiesName = "test.properties";
            logger.info("Setting test.properties file name to default: test.properties");
        }

        // Load properties file. 
        TungstenProperties tp = new TungstenProperties();
        File f = new File(testPropertiesName);
        if (f.canRead())
        {
            logger.info("Reading test.properties file: " + testPropertiesName);
            FileInputStream fis = new FileInputStream(f);
            tp.load(fis);
            fis.close();
        }
        else
            logger.warn("Using default values for test");

        // Set values used for test.  
        driver = tp.getString("database.driver",
                "org.apache.derby.jdbc.EmbeddedDriver", true);
        url = tp.getString("database.url",
                "jdbc:derby:testdb;create=true", true);
        user = tp.getString("database.user");
        password = tp.getString("database.password"); 
        schema = tp.getString("database.schema", "testdb", true);
        
        // Load driver. 
        Class.forName(driver);
    }

    /**
     * TODO: setUp definition.
     * 
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
    }

    /**
     * TODO: tearDown definition.
     * 
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    }

    /**
     * Ensure Database instance can be found and can connect. 
     */
    @Test
    public void testDatabaseConnect() throws Exception
    {
        Database db = DatabaseFactory.createDatabase(url, user, password);
        Assert.assertNotNull(db);
        db.connect();
        db.close();
    }
    
    /**
     * Test calls to support session-level binlogging. 
     */
    @Test
    public void testSessionLoggingSupport() throws Exception
    {
        Database db = DatabaseFactory.createDatabase(url, user, password);
        db.connect();
        if (db.supportsControlSessionLevelLogging())
        {
            db.controlSessionLevelLogging(true);
            db.controlSessionLevelLogging(false);
        }
        db.close();
    }
    
    /**
     * Test database schema management commands. 
     */
    @Test
    public void testSchemaSupport() throws Exception
    {
        Database db = DatabaseFactory.createDatabase(url, user, password);
        db.connect();
        if (db.supportsCreateDropSchema())
        {
            db.createSchema("testSchemaSupport");
            if (db.supportsUseDefaultSchema())
            {
                // Let the database set it directly . 
                db.useDefaultSchema("testSchemaSupport");
                
                // Get the use schema query and run it ourselves. 
                String useQuery = db.getUseSchemaQuery(schema);
                db.execute(useQuery);
            }
            db.dropSchema("testSchemaSupport");
        }
        
        if (db.supportsUseDefaultSchema())
            db.useDefaultSchema(schema);

        db.close();
    }
    
    /**
     * Test timestamp management commands. 
     */
    @Test
    public void testTimestampControl() throws Exception
    {
        Database db = DatabaseFactory.createDatabase(url, user, password);
        db.connect();
        if (db.supportsControlTimestamp())
        {
            String tsQuery = db.getControlTimestampQuery(System.currentTimeMillis());
            db.execute(tsQuery);
        }
        db.close();
    }
    
    /**
     * Verify that we can set and get session variable values. 
     */
    @Test
    public void testSessionVariables() throws Exception
    {
        Database db = DatabaseFactory.createDatabase(url, user, password);
        db.connect();
        if (db.supportsSessionVariables())
        {
            db.setSessionVariable("mytestvar", "testvalue!");
            String value = db.getSessionVariable("mytestvar");
            Assert.assertEquals("Check session variable value", "testvalue!", value);
        }
        db.close();
    }

    /**
     * Ensure that we can create and delete a table containing all table
     * types and with a unique primary key. 
     */
    @Test
    public void testColumnTypesWithKey() throws Exception
    {
        Column myInt = new Column("my_int", Types.INTEGER);
        Column myBigInt = new Column("my_big_int", Types.BIGINT);
        Column myChar = new Column("my_char", Types.CHAR, 10);
        Column myVarChar = new Column("my_var_char", Types.VARCHAR, 10);
        Column myDate = new Column("my_date", Types.DATE);
        Column myTimestamp = new Column("my_timestamp", Types.TIMESTAMP);
        Column myClob = new Column("my_clob", Types.CLOB);
        Column myBlob = new Column("my_blob", Types.BLOB);

        Table testColumnTypes = new Table("tungsten", "test_column_types");
        testColumnTypes.AddColumn(myInt);
        testColumnTypes.AddColumn(myBigInt);
        testColumnTypes.AddColumn(myChar);
        testColumnTypes.AddColumn(myVarChar);
        testColumnTypes.AddColumn(myDate);
        testColumnTypes.AddColumn(myTimestamp);
        testColumnTypes.AddColumn(myClob);
        testColumnTypes.AddColumn(myBlob);
        
        Key primary = new Key(Key.Primary);
        primary.AddColumn(myInt);
        testColumnTypes.AddKey(primary);
        
        // Open database and connect. 
        Database db = DatabaseFactory.createDatabase(url, user, password);
        db.connect();
        if (db.supportsUseDefaultSchema())
            db.useDefaultSchema(schema);

        // Create table. 
        db.createTable(testColumnTypes , true);

        // Add a row. 
        byte byteData[] = "blobs".getBytes("UTF-8");
        myInt.setValue(23);
        myBigInt.setValue(25L);
        myChar.setValue("myChar");
        myVarChar.setValue("myVarChar");
        myDate.setValue(new Date(System.currentTimeMillis()));
        myTimestamp.setValue(new Date(System.currentTimeMillis()));
        myClob.setValue("myClob");
        myBlob.setValue(new ByteArrayInputStream(byteData), byteData.length);
        
        db.insert(testColumnTypes);

        // Update the row we just added. 
        myChar.setValue("myChar2");
        ArrayList<Column> updateColumns = new ArrayList<Column>();
        updateColumns.add(myChar);
        db.update(testColumnTypes, testColumnTypes.getPrimaryKey().getColumns(), 
                updateColumns);
        
        // Drop table. 
        db.dropTable(testColumnTypes);
    }
    
    /**
     * Ensure we can connect and manipulate SQL.  These calls are similar
     * to those used in the THL and appliers. 
     */
    @Test
    public void testTableOperations() throws Exception
    {
        /* History table */
        Column historySeqno     = new Column("seqno",     Types.BIGINT);
        Column historyTstamp    = new Column("tstamp",    Types.VARCHAR, 32);
        Column historyStatement = new Column("statement", Types.BLOB);

        Table history = new Table("tungsten", "history");
        history.AddColumn(historySeqno);
        history.AddColumn(historyTstamp);
        history.AddColumn(historyStatement);

        /* Seqno table */
        Column seqnoSeqno    = new Column("seqno",     Types.BIGINT);
        Column seqnoTrxid    = new Column("trxid",     Types.VARCHAR, 20);

        Key seqnoPrimary = new Key(Key.Primary);
        seqnoPrimary.AddColumn(seqnoSeqno);

        Key seqnoSecondary = new Key(Key.Unique);
        seqnoSecondary.AddColumn(seqnoTrxid);

        Table seqno = new Table("tungsten", "seqno");
        seqno.AddColumn(seqnoSeqno);
        seqno.AddColumn(seqnoTrxid);
        seqno.AddKey(seqnoPrimary);
        seqno.AddKey(seqnoSecondary);

        /* Create a fake SQLEvent to log */
        ArrayList<String> trx = new ArrayList<String>();
        trx.add("INSERT INTO EMP VALUE(1, 2)");
        /* Timestamp fakeTime           = Timestamp.valueOf("2008-01-01 09:00:00"); */
        
        ArrayList<DBMSData> arr = new ArrayList<DBMSData>();
        DBMSEvent dbmsEvent = new DBMSEvent("7", arr, new Timestamp(System.currentTimeMillis()));
        ReplDBMSEvent fake_sqlEvent = new ReplDBMSEvent(7, dbmsEvent);
        ByteArrayOutputStream baob = new ByteArrayOutputStream();
        ObjectOutputStream oob = new ObjectOutputStream(baob);
        oob.writeObject(fake_sqlEvent);
        byte[] barr = baob.toByteArray();
        InputStream is = new ByteArrayInputStream(barr);
        int         fake_SQL_length = barr.length;
        InputStream fake_SQL_is     = is;

        // Open database and connect. 
        Database db = DatabaseFactory.createDatabase(url, user, password);
        db.connect();
        if (db.supportsUseDefaultSchema())
            db.useDefaultSchema(schema);

        // Create history table. 
        db.createTable(history , true);

        // Create seqno table. 
        db.createTable(seqno   , true);

        // Insert a nice row. 
        historySeqno.setValue(10L);
        historyTstamp.setValue("October 3");
        historyStatement.setValue(fake_SQL_is, fake_SQL_length);
        db.insert(history);

        // Update a row. 
        seqnoSeqno.setValue(22L);
        seqnoTrxid.setValue("hello");
        db.update(seqno, seqno.getPrimaryKey().getColumns(), seqno.getNonKeyColumns());

        // Delete row from table seqno based on last value of PK. 
        db.delete(seqno, false);

        // Replace row in seqno with last values of all columns.  
        // In Oracle this should casue a DELETE, INSERT */
        // In MySQL  this should casue a REPLACE INTO */
        db.replace(seqno);

        db.disconnect();
    }
    
    /**
     * Ensure we can get a list of schemas. 
     */
    @Test
    public void testGetSchemas() throws Exception
    {
        // Open database and connect. 
        Database db = DatabaseFactory.createDatabase(url, user, password);
        if(db.getType() == DBMS.DERBY)
        {
            logger.info("Skipping testGetSchemas() on Derby...");
            return;
        }
        db.connect();

        logger.info("getSchemas() returned:");
        ArrayList<String> schemas = db.getSchemas();
        for(String schema : schemas)
        {
            logger.info(schema);
        }
        
        assertTrue("Zero schemas returned", schemas.size() > 0);
        
        db.disconnect();
    }
    
    /**
     * Does time difference function work? 
     */
    @Test
    public void testGetTimeDiff() throws Exception
    {
        // Open database and connect. 
        Database db = DatabaseFactory.createDatabase(url, user, password);
        if(db.getType() == DBMS.DERBY)
        {
            logger.info("Skipping testGetTimeDiff() on Derby...");
            return;
        }
        db.connect();
        
        Timestamp now = new Timestamp(System.currentTimeMillis());
        String sql = null;
        PreparedStatement prepareStatement = null;
        ResultSet rs = null;
        int diff = -1;
        
        // Case A: SQL function vs. SQL function
        sql = "SELECT " + db.getTimeDiff(db.getNowFunction(), db.getNowFunction());
        logger.info("getTimeDiff() prepared SQL: " + sql);
        prepareStatement = db.prepareStatement(sql);
        rs = prepareStatement.executeQuery();
        diff = -1;
        if (rs.next())
        {
            diff = rs.getInt(1);
            logger.info("Time difference: " + diff);
        }
        assertTrue("Timestamp difference should be zero", diff == 0);
        rs.close();   
        
        // Case B: Java object vs. SQL function.        
        sql = "SELECT " + db.getTimeDiff(null, db.getNowFunction());
        logger.info("getTimeDiff() prepared SQL: " + sql);
        prepareStatement = db.prepareStatement(sql);
        prepareStatement.setTimestamp(1, now);
        rs = prepareStatement.executeQuery();
        if (rs.next())
            logger.info("DB host and local host time difference: " + rs.getInt(1));
        rs.close();

        // Case C: Java object vs. Java object.
        sql = "SELECT " + db.getTimeDiff(null, null);
        logger.info("getTimeDiff() prepared SQL: " + sql);
        prepareStatement = db.prepareStatement(sql);
        prepareStatement.setTimestamp(1, now);
        prepareStatement.setTimestamp(2, now);
        rs = prepareStatement.executeQuery();
        diff = -1;
        if (rs.next())
        {
            diff = rs.getInt(1);
            logger.info("Time difference: " + diff);
        }
        assertTrue("Timestamp difference should be zero", diff == 0);
        rs.close();

        db.disconnect();
    }
}
