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

package com.continuent.tungsten.replicator.thl.log;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * This class implements a write locking mechanism by acquiring an exclusive
 * lock on a named operating system file. The lock mechanism is used to prevent
 * multiple processes from writing to disk logs.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class WriteLock
{
    File             lockFile;
    RandomAccessFile raf;
    FileLock         lock;

    /**
     * Instantiates the write lock instance.
     * 
     * @param lockFile
     * @throws ReplicatorException
     */
    public WriteLock(File lockFile) throws ReplicatorException
    {
        this.lockFile = lockFile;
    }

    /**
     * Attempt to acquire write lock. This call is idempotent.
     * 
     * @return true if lock successfully acquired
     */
    public synchronized boolean acquire() throws ReplicatorException
    {
        if (isLocked())
            return true;

        try
        {
            raf = new RandomAccessFile(lockFile, "rw");
            FileChannel channel = raf.getChannel();
            lock = channel.tryLock();
        }
        catch (FileNotFoundException e)
        {
            throw new ReplicatorException(
                    "Unable to find or create lock file: "
                            + lockFile.getAbsolutePath());
        }
        catch (Exception e)
        {
            throw new ReplicatorException(
                    "Error while attempting to acquire file lock: "
                            + lockFile.getAbsolutePath(), e);
        }
        
        finally
        {
            if (lock == null && raf != null)
            {
                close(raf);
            }
        }

        // Clean up and return status.  If we don't get an exclusive lock,
        // we need to clean up so that the next call will get it. 
        if (lock == null)
        {
            if (raf != null)
                close(raf);
            return false;
        }
        else if (lock.isShared())
        {
            release();
            return false;
        }
        else
            return true;
    }

    /**
     * Return true if the write lock is currently acquired exclusively.
     */
    public synchronized boolean isLocked()
    {
        return (lock != null && !lock.isShared());
    }

    /**
     * Release the write lock. This call is idempotent.
     */
    public synchronized void release()
    {
        if (lock != null)
        {
            try
            {
                lock.release();
            }
            catch (IOException e)
            {
            }
            lock = null;

            close(raf);
            raf = null;
        }
    }

    // Close file, suppressing any exception.
    private void close(RandomAccessFile raf)
    {
        try
        {
            raf.close();
        }
        catch (IOException e)
        {
        }
    }
}