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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;

/**
 * Implements a dummy backup agent used for unit testing.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class DummyBackupAgent implements BackupAgent
{
    private File    directory = new File(".");
    private boolean fail      = false;

    public DummyBackupAgent()
    {
    }

    public void setDirectory(File directory)
    {
        this.directory = directory;
    }

    public void setFail(boolean fail)
    {
        this.fail = fail;
    }

    public BackupSpecification backup() throws BackupException
    {
        BackupSpecification spec = new BackupSpecification();
        spec.setBackupDate(new Date());

        // Fail if that's what we are supposed to do.
        if (fail)
        {
            throw new BackupException("Backup failing on command!");
        }

        // Produce a dummy file.
        File temp = null;
        FileWriter fw = null;
        try
        {
            temp = File.createTempFile("dummyBackup", "dat", directory);
            fw = new FileWriter(temp);
            fw.write("dummy output");
        }
        catch (IOException e)
        {
            throw new BackupException("Unable to write to temporary file", e);
        }
        finally
        {
            if (fw != null)
            {
                try
                {
                    fw.close();
                }
                catch (IOException e)
                {
                }
            }
        }
        spec.addBackupLocator(new FileBackupLocator(temp, true));
        return spec;
    }

    public void restore(BackupSpecification spec) throws BackupException
    {
        // Fail if that's what we are supposed to do.
        if (fail)
        {
            throw new BackupException("Restore failing on command!");
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.backup.BackupPlugin#configure()
     */
    public void configure() throws BackupException
    {
        if (!directory.isDirectory() || !directory.canWrite())
        {
            throw new BackupException(
                    "Test file directory does not exist or is not writable: "
                            + directory.getAbsolutePath());
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.backup.BackupPlugin#release()
     */
    public void release() throws BackupException
    {
        // Nothing to do!
    }

    /**
     * Returns default capabilities. 
     * {@inheritDoc}
     * @see com.continuent.tungsten.replicator.backup.BackupAgent#capabilities()
     */
    public BackupCapabilities capabilities()
    {
        return new BackupCapabilities();
    }
}
