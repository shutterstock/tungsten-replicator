/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2010 Continuent Inc.
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
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.pipeline;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;

/**
 * This class implements simple performance tests of Java operations.  We
 * use the results to benchmark operations like fetching the current time
 * from the VM. 
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class JavaOpsTest extends TestCase
{
    private static Logger logger = Logger.getLogger(JavaOpsTest.class);

    /**
     * Setup.
     * 
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
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
     * Test the time required to read time from the VM.
     */
    public void testReadTime() throws Exception
    {
        long currentTimeMillis;
        long count = 0;
        for (int i = 0; i < 5; i++)
        {
            logger.info("System.currentTimeMillis() invocation count: " + count);
            for (int j = 0; j < 10000000; j++)
            {
                // Make call with usage to prevent it from being optimized out. 
                count++;
                currentTimeMillis = 0;
                currentTimeMillis = System.currentTimeMillis();
                assertTrue(currentTimeMillis > 0);
            }
        }
    }
    
    /** 
     * Test the time to inquire about the health of a thread. 
     */
    public void testThreadIsInterrupted() throws Exception
    {
        long count = 0;
        for (int i = 0; i < 5; i++)
        {
            logger.info("Thread. invocation count: " + count);
            for (int j = 0; j < 1000000; j++)
            {
                // Make call with usage to prevent it from being optimized out. 
                count++;
                boolean isInterrupted = true;
                isInterrupted = Thread.currentThread().isInterrupted();
                assertFalse(isInterrupted);
            }
        }
    }
}