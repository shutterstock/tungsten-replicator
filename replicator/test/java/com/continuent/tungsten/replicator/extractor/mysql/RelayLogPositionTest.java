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

package com.continuent.tungsten.replicator.extractor.mysql;

import java.io.File;

import junit.framework.TestCase;

import org.apache.log4j.Logger;

/**
 * Implements a simple unit test on the RelayLogPosition class.  
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class RelayLogPositionTest extends TestCase
{
    static Logger logger = Logger.getLogger(RelayLogPositionTest.class);

    /**
     * Verify that we correctly identify when a log position has reached
     * a particular file and offset. 
     */
    public void testPositionCheck() throws Exception
    {
        RelayLogPosition rlp = new RelayLogPosition();
        rlp.setPosition(new File("/var/lib/mysql/mysql-bin.000077"), 2333308);

        // Check various combinations of the file is too low. 
        assertFalse("File lower, offset lower", rlp.hasReached("mysql-bin.000078", 0));
        assertFalse("File lower, offset match", rlp.hasReached("mysql-bin.000078", 2333308));
        assertFalse("File lower, offset higher", rlp.hasReached("mysql-bin.000078", 2333309));
        
        // Check when the value matches. 
        assertTrue("File and offset match", rlp.hasReached("mysql-bin.000077", 2333308));
        
        // Check when the file and/or offset are higher. 
        assertTrue("File equal, offset higher", rlp.hasReached("mysql-bin.000077", 2333307));
        assertTrue("File higher, offset lower", rlp.hasReached("mysql-bin.000076", 0));
        assertTrue("File higher, offset higher", rlp.hasReached("mysql-bin.000076", 2333309));
    }
    
    /** Verify that we correctly clone values. */
    public void testClone() throws Exception
    {
        RelayLogPosition rlp = new RelayLogPosition();
        File f = new File("/var/lib/mysql/mysql-bin.000077");
        rlp.setPosition(f, 2333308);
        RelayLogPosition rlp2 = rlp.clone();

        // Check the values. 
        assertEquals("File matches", f, rlp2.getFile());
        assertEquals("offset matches", 2333308, rlp2.getOffset());
        
        // Check when the value matches. 
        assertTrue("File and offset match", rlp2.hasReached("mysql-bin.000077", 2333308));
    }
}