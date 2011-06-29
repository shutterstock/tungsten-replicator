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

package com.continuent.tungsten.replicator.thl.log;

import java.io.File;

/**
 * Provides utility methods to open log files quickly.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class LogHelper
{
    /**
     * Create and return a writable file. Any existing file is deleted.
     */
    static LogFile createLogFile(String name, long seqno) throws Exception
    {
        // Create the file and return it if we want to write.
        File logfile = new File(name);
        logfile.delete();
        LogFile tf = new LogFile(logfile);
        tf.create(seqno);
        return tf;
    }

    /**
     * Open an existing file for reading.
     */
    static LogFile openExistingFileForRead(String name) throws Exception
    {
        File logfile = new File(name);
        LogFile tf = new LogFile(logfile);
        tf.openRead();
        return tf;
    }

    /**
     * Open an existing file for writing.
     */
    static LogFile openExistingFileForWrite(String name) throws Exception
    {
        File logfile = new File(name);
        LogFile tf = new LogFile(logfile);
        tf.openWrite();
        return tf;
    }
}
