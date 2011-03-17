/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2009 Continuent Inc.
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

package com.continuent.tungsten.replicator.backup;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;

/**
 * This class tests the process helper using dummy commands.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class TestProcessHelper extends TestCase
{
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
     * Tests execution with non-prefixed command.
     */
    public void testNonPrefixedCommand() throws Exception
    {
        ProcessHelper processHelper = new ProcessHelper();
        processHelper.configure();
        processHelper.exec("Running an un-prefixed echo command",
                "echo 'hello!'");
    }

    /**
     * Tests execution with prefixed command.
     */
    public void testPrefixedCommand() throws Exception
    {
        ProcessHelper processHelper = new ProcessHelper();
        processHelper.setCmdPrefix("bash -c");
        processHelper.configure();
        processHelper.exec("Running a prefixed echo command", "echo");
    }

    /**
     * Verify we get an exception if the command fails.
     */
    public void testFailingCommand() throws Exception
    {
        // Try with a bad prefix.
        ProcessHelper processHelper = new ProcessHelper();
        processHelper.setCmdPrefix("bad");
        processHelper.configure();
        try
        {
            processHelper.exec("Running a command with a bad prefix", "echo");
            throw new Exception("Command runs with bad prefix!");
        }
        catch (BackupException e)
        {
        }

        // Try with good prefix and a bad command.
        processHelper.setCmdPrefix("bash -c");
        processHelper.configure();
        try
        {
            processHelper.exec("Running a command with a bad base command",
                    "bad");
            throw new Exception("Command runs with bad prefix!");
        }
        catch (BackupException e)
        {
        }
    }
}