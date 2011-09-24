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
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.patterns.event.EventDispatcher;
import com.continuent.tungsten.replicator.ErrorNotification;

/**
 * Processes a restore command including retrieving the backup file and loading
 * into the database.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class RestoreTask implements Callable<Boolean>
{
    private static final Logger   logger = Logger.getLogger(RestoreTask.class);
    private final URI             uri;
    private final EventDispatcher eventDispatcher;
    private final BackupAgent     backupAgent;
    private final StorageAgent    storageAgent;

    public RestoreTask(URI uri, EventDispatcher dispatcher,
            BackupAgent backupAgent, StorageAgent storageAgent)
    {
        this.uri = uri;
        this.eventDispatcher = dispatcher;
        this.backupAgent = backupAgent;
        this.storageAgent = storageAgent;
    }

    /**
     * Execute the backup task.
     */
    public Boolean call() throws BackupException
    {
        logger.info("Backup task starting...");
        boolean completed = false;
        try
        {
            // Retrieve the file.
            logger.info("Retrieving backup file: uri=" + uri);
            BackupSpecification bspec = storageAgent.retrieve(uri);

            // Restore database.
            logger.info("Restoring database from file: uri=" + uri);
            backupAgent.restore(bspec);

            // Turn the resulting file over to storage.
            completed = true;
            logger.info("Restore completed successfully; uri=" + uri);
        }
        catch (InterruptedException e)
        {
            logger.warn("Restore was cancelled");
        }
        catch (Exception e)
        {
            String message = "Restore operation failed: " + e.getMessage();
            logger.error(message, e);
            try
            {
                eventDispatcher.put(new ErrorNotification(message, e));
            }
            catch (InterruptedException ie)
            {
                // No need to handle; thread is dying anyway.
            }
        }
        finally
        {
        }

        // Post a backup completion event.
        if (completed)
        {
            logger.info("Restore task completed normally: uri=" + uri);
            try
            {
                eventDispatcher.put(new RestoreCompletionNotification(
                        uri));
            }
            catch (InterruptedException ie)
            {
                logger
                        .warn("Restore task interrupted while posting completion event");
            }
        }
        else
        {
            logger.warn("Restore task did not complete");
        }
        return completed;
    }
}