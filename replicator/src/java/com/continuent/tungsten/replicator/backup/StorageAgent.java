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

import java.net.URI;

/**
 * Denotes a class that implements a storage agent that can store and retrieve
 * files.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public interface StorageAgent extends BackupPlugin
{
    /**
     * Returns the number of backup files that should be retained before
     * deleting old files.
     */
    public int getRetention();

    /**
     * Sets the number of backup files to retain.
     */
    public void setRetention(int numberOfBackups);

    /**
     * Returns the URIs of all backups in storage ordered from oldest to most
     * recent.
     */
    public URI[] list() throws BackupException;

    /**
     * Returns the URI of the most recent backup in storage or null if no
     * backups exist
     */
    public URI last() throws BackupException;

    /**
     * Returns the storage specification of a particular backup or null if no
     * such specification exists.
     */
    public StorageSpecification getSpecification(URI uri)
            throws BackupException;

    /**
     * Stores a backup described by a particular backup specification, returning
     * the URL of the backup.
     */
    public URI store(BackupSpecification specification) throws BackupException;

    /**
     * Retrieves the backup corresponding to a particular URI.
     */
    public BackupSpecification retrieve(URI uri) throws BackupException;

    /**
     * Deletes the indicated backup if it exists.
     * 
     * @return True if backup was found and deleted, otherwise false
     */
    public boolean delete(URI uri) throws BackupException;

    /**
     * Deletes all backups.
     * 
     * @return True if all backups were successfully deleted (also returns true
     *         if there are no backups found)
     */
    public boolean deleteAll() throws BackupException;
}