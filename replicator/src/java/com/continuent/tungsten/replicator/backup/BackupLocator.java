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

/**
 * Maintains references to storage used during backups.  Encapsulates
 * the backup location and logic required (if necessary) to mount 
 * storage for copying backup, e.g., by mounting an LVM snapshot. 
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public interface BackupLocator
{
    /**
     * Makes the storage contents for use.  Must be called before looking
     * at contents. 
     */
    public void open();

    /**
     * Returns a reference to the file containing the backup. 
     */
    public File getContents();

    /**
     * Releases the storage.  This must be called after using the storage
     * to ensure all resources are released. 
     */
    public void release();

    public String getDatabaseName();
}