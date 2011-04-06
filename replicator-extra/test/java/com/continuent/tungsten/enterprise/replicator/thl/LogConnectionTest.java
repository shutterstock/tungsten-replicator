/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2010 Continuent Inc.
 * Contact: tungsten@continuent.com
 *
 * This program is property of Continuent.  All rights reserved. 
 *
 * Initial developer(s): Robert Hodges
 * Contributor(s):
 */

package com.continuent.tungsten.enterprise.replicator.thl;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;

import com.continuent.tungsten.replicator.thl.THLException;

/**
 * Test capabilities of tungsten log files. This test is fully self-contained
 * but creates files on the file system.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class LogConnectionTest extends TestCase
{
    private static Logger logger = Logger.getLogger(LogConnectionTest.class);

    /**
     * Setup.
     * 
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        logger.info("Test starting");
    }

    /**
     * Teardown.
     * 
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    }

    /**
     * Confirm that we can create a log connection manager, then
     * create/get/return a connection, then release the manager. This checks the
     * full log life cycle.
     */
    public void testLifeCycle() throws Exception
    {
        // Create manager and ensure there are no connections.
        LogConnectionManager cm = new LogConnectionManager();
        assertEquals("Empty manager has 0 size", 0, cm.getSize());
        LogConnection conn = cm.getLogConnection(0);
        assertNull("Connection must be null for empty manager", conn);

        // Create new connection.
        LogFile lf = LogHelper.createLogFile("testLifeCycle.dat", 3);
        conn = cm.createAndGetLogConnection(lf, 0L);
        assertEquals("Manager must have 1 connection", 1, cm.getSize());
        assertNotNull("Must get connection", conn);
        assertTrue("Created conn must be loaned", conn.isLoaned());
        assertEquals("Logfile in connection must match input logfile", lf, conn
                .getLogFile());

        // Return the new connection.
        cm.returnLogConnection(conn);
        assertFalse("Returned conn must not be loaned", conn.isLoaned());

        // Fetch connection back.
        LogConnection conn2 = cm.getLogConnection(0);
        assertNotNull("Must get connection", conn2);
        assertTrue("Created conn must be loaned", conn2.isLoaned());
        assertEquals("Logfile in connection must match input logfile", lf,
                conn2.getLogFile());

        // Return the loaner connection.
        cm.returnLogConnection(conn2);
        assertFalse("Returned conn must not be loaned", conn2.isLoaned());

        // Release manager.
        cm.release();
    }

    /**
     * Confirm that non-returned connections throw exceptions if you try to
     * fetch again.
     */
    public void testReturn() throws Exception
    {
        // Create manager and ensure there are no connections.
        LogConnectionManager cm = new LogConnectionManager();

        // Create new connection.
        LogFile lf = LogHelper.createLogFile("testReturn.dat", 3);
        LogConnection conn1 = cm.createAndGetLogConnection(lf, 0L);

        // Get the connection without returning it and show that
        // we get an exception.
        try
        {
            cm.getLogConnection(0L);
            throw new Exception("Returned loaned connection without exception");
        }
        catch (THLException e)
        {
            logger.info("Received expected exception");
        }
        cm.returnLogConnection(conn1);

        // Fetch connection back.
        LogConnection conn2 = cm.getLogConnection(0);
        assertEquals("Logfile in connection must match input logfile", lf,
                conn2.getLogFile());

        // Get the connection without returning it and show that
        // we get an exception.
        try
        {
            cm.getLogConnection(0L);
            throw new Exception("Returned loaned connection without exception");
        }
        catch (THLException e)
        {
            logger.info("Received expected exception");
        }
        cm.returnLogConnection(conn2);

        // Prove we can get the connection back.
        LogConnection conn3 = cm.getLogConnection(0);
        assertEquals("Logfile in connection must match input logfile", lf,
                conn3.getLogFile());

        // Release manager.
        cm.release();
    }

    /**
     * Confirm that connections time out after the timeout value set on the
     * connection manager.
     */
    public void testTimeout() throws Exception
    {
        // Create manager and ensure there are no connections.
        LogConnectionManager cm = new LogConnectionManager();
        cm.setTimeoutMillis(100);

        // Create new connection.
        LogFile lf = LogHelper.createLogFile("testTimeout.dat", 3);
        LogConnection conn1 = cm.createAndGetLogConnection(lf, 0L);
        cm.returnLogConnection(conn1);

        // Fetch connection back. We should get it back.
        LogConnection conn2 = cm.getLogConnection(0);
        assertEquals("Logfile in connection must match input logfile", lf,
                conn2.getLogFile());
        cm.returnLogConnection(conn2);

        // Wait for > 100 ms and retry. Connection should be released and null.
        Thread.sleep(110);

        // Prove returned connection is now null.
        LogConnection conn3 = cm.getLogConnection(0);
        assertNull("timed out connection should be null", conn3);

        // Release manager.
        cm.release();
    }

    /**
     * Confirm we can release connection for current thread.
     */
    public void testThreadRelease() throws Exception
    {
        // Create manager and ensure there are no connections.
        LogConnectionManager cm = new LogConnectionManager();

        // Create new connection.
        LogFile lf = LogHelper.createLogFile("testThreadRelease.dat", 3);
        LogConnection conn1 = cm.createAndGetLogConnection(lf, 0L);
        cm.returnLogConnection(conn1);

        // Release connection for current connection.
        cm.releaseConnection();

        // Prove returned connection is now null.
        LogConnection conn3 = cm.getLogConnection(0);
        assertNull("timed out connection should be null", conn3);
        assertEquals("connections should be released", 0, cm.getSize());

        // Release manager.
        cm.release();
    }

    /**
     * Verify that requesting a lower sequence number causes the connection to
     * release.
     */
    public void testLowSeqno() throws Exception
    {
        // Create manager and ensure there are no connections.
        LogConnectionManager cm = new LogConnectionManager();

        // Create new connection.
        LogFile lf = LogHelper.createLogFile("testThreadRelease.dat", 3);
        LogConnection conn1 = cm.createAndGetLogConnection(lf, 10L);
        cm.returnLogConnection(conn1);

        // Prove returned connection is now null if seqno is lower.
        LogConnection conn3 = cm.getLogConnection(9);
        assertNull("lower connection should be null", conn3);
        assertEquals("connections should be released", 0, cm.getSize());

        // Release manager.
        cm.release();
    }
}