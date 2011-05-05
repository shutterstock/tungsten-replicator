/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2010-11 Continuent Inc.
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

package com.continuent.tungsten.enterprise.replicator.thl;

import java.io.File;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;

/**
 * Tests behavior of write lock class.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class WriteLockTest extends TestCase
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
     * Confirm that we can acquire a write lock, creating the lock file if it
     * does not exist.
     */
    public void testBasicLocking() throws Exception
    {
        // Ensure lock file does not exist.
        File lockFile = new File("testBasicLocking.lock");
        if (lockFile.exists())
            lockFile.delete();
        assertFalse("Lockfile may not exist before test", lockFile.exists());

        // Lock the file and release the file, confirming expected status of
        // lock at each step.
        WriteLock wl = new WriteLock(lockFile);

        for (int i = 0; i < 100; i++)
        {
            boolean locked = wl.acquire();
            assertTrue("Acquire must succeed", locked);
            assertTrue("File must be locked", wl.isLocked());
            assertTrue("File must exist", lockFile.exists());

            wl.release();
            assertFalse("File must not be locked", wl.isLocked());
        }

        // Lock and release again with a new lock. This should succeed on the
        // file, which must now exist.
        WriteLock wl2 = new WriteLock(lockFile);
        boolean locked = wl2.acquire();
        assertTrue("Acquire must succeed", locked);
        assertTrue("File must be locked", wl2.isLocked());

        wl2.release();
        assertFalse("File must not be locked", wl2.isLocked());
    }

    /**
     * Confirm that acquire and release operations are idempotent.
     */
    public void testIdempotency() throws Exception
    {
        // Define lock file.
        File lockFile = new File("testIdempotency.lock");
        WriteLock wl = new WriteLock(lockFile);

        // Acquire multiple times to check that this call is idempotent.
        for (int i = 0; i < 100; i++)
        {
            boolean locked = wl.acquire();
            assertTrue("Acquire must succeed", locked);
            assertTrue("File must be locked", wl.isLocked());
        }

        // Release multiple times to confirm that this call is idempotent.
        for (int i = 0; i < 100; i++)
        {
            wl.release();
            assertFalse("File must not be locked", wl.isLocked());
        }
    }
}
