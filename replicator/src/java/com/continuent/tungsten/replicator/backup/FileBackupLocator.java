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

public class FileBackupLocator implements BackupLocator
{
    private final File    backup;
    private final boolean deleteOnRelease;
    private String        databaseName;

    public FileBackupLocator(File backup, boolean deleteOnRelease)
    {
        this(null, backup, deleteOnRelease);
    }

    public FileBackupLocator(String databaseName, File backup,
            boolean deleteOnRelease)
    {
        this.backup = backup;
        this.deleteOnRelease = deleteOnRelease;
        this.databaseName = databaseName;
    }

    public File getContents()
    {
        return backup;
    }

    public String getDatabaseName()
    {
        return databaseName;
    }

    public void open()
    {
        // Nothing to do.
    }

    public void release()
    {
        if (deleteOnRelease)
        {
            backup.delete();
        }
    }
}
