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

/**
 * Simple class to track the relay log position using synchronized methods to
 * ensure the file and offset are always updated consistently.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 */
public class RelayLogPosition
{
    protected File curFile;
    protected long curOffset;

    public RelayLogPosition()
    {
    }

    public synchronized void setPosition(File file, long offset)
    {
        this.curFile = file;
        this.curOffset = offset;
    }

    public synchronized void setOffset(int offset)
    {
        this.curOffset = offset;
    }
    
    public synchronized File getFile()
    {
        return curFile;
    }

    public synchronized long getOffset()
    {
        return curOffset;
    }

    /**
     * Return a consistent clone of this position.
     */
    public synchronized RelayLogPosition clone()
    {
        RelayLogPosition clone = new RelayLogPosition();
        clone.setPosition(curFile, curOffset);
        return clone;
    }

    /**
     * Return true if we have reached a desired file:offset position. 
     */
    public synchronized boolean hasReached(String fileName, long offset)
    {
        if (curFile == null)
            return false;
        else if (curFile.getName().compareTo(fileName) < 0)
        {
            // Our file name is greater, position has not been reached. 
            return false;
        }
        else if (curFile.getName().compareTo(fileName) == 0)
        {
            // Our file name is the same, we must compare the offset. 
            if (offset > curOffset)
                return false;
            else 
                return true;
        }
        else 
        {
            // Our file name is less.  We have reached the position. 
            return true;
        }
    }
    
    /**
     * Return a string representation of the position. 
     */
    public synchronized String toString()
    {
        if (curFile == null)
            return "(no position set)";
        else
            return curFile.getName() + ":" + curOffset;
    }
}