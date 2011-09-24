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
 * Processes a backup command including dumping data from the database and
 * storing the resulting file(s).
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class BackupTask implements Callable<String>
{
    private static final Logger   logger = Logger.getLogger(BackupTask.class);
    private final EventDispatcher eventDispatcher;
    private final String          backupAgentName;
    private final BackupAgent     backupAgent;
    private final StorageAgent    storageAgent;

    public BackupTask(EventDispatcher dispatcher, String backupAgentName,
            BackupAgent backupAgent, StorageAgent storageAgent)
    {
        this.eventDispatcher = dispatcher;
        this.backupAgentName = backupAgentName;
        this.backupAgent = backupAgent;
        this.storageAgent = storageAgent;
    }

    /**
     * Execute the backup task.
     */
    public String call() throws BackupException
    {
        logger.info("Backup task starting...");
        URI uri = null;
        BackupSpecification bspec = null;
        try
        {
            // Run the backup.
            logger.info("Starting backup using agent: "
                    + backupAgent.getClass().getName());

            // Create a backup specification for storage.
            bspec = backupAgent.backup();

            // Turn the resulting file over to storage.
            logger.info("Storing backup result...");
            bspec.setAgentName(backupAgentName);
            uri = storageAgent.store(bspec);
        }
        catch (InterruptedException e)
        {
            logger.warn("Backup was cancelled");
        }
        catch (Exception e)
        {
            String message = "Backup operation failed: " + e.getMessage();
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
            if (bspec != null)
                bspec.releaseLocators();
        }

        // Post a backup completion event.
        if (uri == null)
        {
            logger.warn("Backup task did not complete");
        }
        else
        {
            logger.info("Backup completed normally: uri=" + uri);
            try
            {
                eventDispatcher.put(new BackupCompletionNotification(
                        uri));
            }
            catch (InterruptedException ie)
            {
                logger
                        .warn("Backup task interrupted while posting completion event");
            }
        }
        if (uri == null)
            return null;
        else
            return uri.toString();
    }
}