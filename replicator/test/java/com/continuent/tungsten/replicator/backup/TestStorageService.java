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
import java.net.URI;
import java.util.Date;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;

/**
 * This class tests the SimpleFileStorageAgent class.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class TestStorageService extends TestCase
{
    private static Logger logger           = Logger
                                                   .getLogger(TestStorageService.class);
    private static String STORAGE_DIR_NAME = "store-test/";

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
     * Tests basic configuration and release.
     */
    public void testStorageConfiguration() throws Exception
    {
        StorageAgent agent = this.configureStorageService(
                "testStorageConfiguration", 1);

        // Ensure no backups are present.
        assertEquals("Initial backups should be 0", 0, agent.list().length);

        agent.release();
    }

    /**
     * Test ability to store and retrieve a file from the storage service.
     */
    public void testStoreRetrieve() throws Exception
    {
        StorageAgent agent = this.configureStorageService("testStoreRetrieve",
                3);
        assertEquals("Initial backups should be 0", 0, agent.list().length);

        // Storage and retrieve file.
        BackupSpecification backupSpecStore = createBackup("testStoreRetrieve",
                10000);
        URI uri = agent.store(backupSpecStore);
        BackupSpecification backupSpecRetrieve = agent.retrieve(uri);

        // Ensure that one backup is listed and that files compare equally.
        assertEquals("Final backups should be 1", 1, agent.list().length);
        for (int i = 0; i < backupSpecStore.getBackupLocators().size(); i++)
        {
            compareFile(backupSpecStore.getBackupLocators().get(i)
                    .getContents(), backupSpecRetrieve.getBackupLocators().get(
                    i).getContents());
        }
        agent.release();
    }

    /**
     * Verify that URLs are returned in backup order. This incidentally tests
     * the ability to store and retrieve multiple backups.
     */
    public void testOrder() throws Exception
    {
        StorageAgent agent = this.configureStorageService("testOrder", 3);
        assertEquals("Initial backups should be 0", 0, agent.list().length);

        int size = 1;
        for (int i = 0; i < 3; i++)
        {
            // Store the file.
            size = size * 10;
            BackupSpecification backupSpecStore = createBackup("testOrder",
                    size);
            URI uri = agent.store(backupSpecStore);
            StorageSpecification storeSpec = agent.getSpecification(uri);
            assertEquals("Returned specification matches file size", size,
                    storeSpec.getFileLength(0));

            // List all available files and ensure the number of files matches
            // the number we have so far.
            URI[] availableUris = agent.list();
            assertEquals("URI list size must match number of backups", i + 1,
                    availableUris.length);

            // Ensure the order of files matches.
            int size2 = 1;
            for (int j = 0; j <= i; j++)
            {
                size2 = size2 * 10;
                StorageSpecification spec = agent
                        .getSpecification(availableUris[j]);
                assertEquals("URI file size must match order i=" + i + " j="
                        + j, size2, spec.getFileLength(0));
            }
        }

        agent.release();
    }

    /**
     * Verify that the last() call always returns the last backup we added.
     */
    public void testLast() throws Exception
    {
        StorageAgent agent = this.configureStorageService("testLast", 3);
        assertEquals("Initial backups should be 0", 0, agent.list().length);

        for (int i = 0; i < 20; i++)
        {
            BackupSpecification backupSpecStore = createBackup("testLast", 100);
            URI uri = agent.store(backupSpecStore);
            URI lastUri = agent.last();
            assertEquals("Last backup should match", uri, lastUri);
        }

        agent.release();
    }

    /**
     * Test limits on file retention: the stored backups should always be the
     * last N backups where where N is the retention size, and backups should be
     * properly ordered within that range.
     */
    public void testRetention() throws Exception
    {
        // Configure and ensure no backups are present.
        int retain = 3;
        StorageAgent agent = this.configureStorageService("testRetention",
                retain);
        assertEquals("Initial backups should be 0", 0, agent.list().length);

        int size = 1000;
        int[] backupSizes = new int[50];
        for (int i = 0; i < backupSizes.length; i++)
        {
            // Store the file.
            size = size + 10;
            backupSizes[i] = size;
            BackupSpecification backupSpecStore = createBackup("testRetention",
                    size);
            agent.store(backupSpecStore);

            // List all available files and ensure the number is at most the
            // retention size.
            URI[] availableUris = agent.list();
            if (i < retain)
            {
                assertEquals("URI list size must match number of backups",
                        i + 1, availableUris.length);
            }
            else
            {
                assertEquals(
                        "URI list size must match retained number of backups",
                        retain, availableUris.length);
            }

            // Ensure the order of files matches.
            int offset = (i < retain) ? 0 : i - retain + 1;
            for (int j = 0; j <= i && j < 3; j++)
            {
                StorageSpecification spec = agent
                        .getSpecification(availableUris[j]);
                assertEquals("URI file size must match order i=" + i + " j="
                        + j + " offset=" + offset, backupSizes[j + offset],
                        spec.getFileLength(0));
            }
        }

        agent.release();
    }

    /**
     * Test operations to delete individual files.
     */
    public void testDeletion() throws Exception
    {
        StorageAgent agent = this.configureStorageService("testOrder", 10);
        assertEquals("Initial backups should be 0", 0, agent.list().length);

        // Populate backups.
        for (int i = 0; i < 5; i++)
        {
            // Store the file.
            int size = 1000 + (i * 10);
            BackupSpecification backupSpecStore = createBackup("testDeletion",
                    size);
            agent.store(backupSpecStore);
        }

        // Delete each backup and ensure the list is correctly maintained.
        for (int i = 0; i < 5; i++)
        {
            // Ensure the list is correct.
            URI[] currentUris = agent.list();
            assertEquals("URI list must account for deletions", 5 - i,
                    currentUris.length);

            // Assert the size of the first backup.
            StorageSpecification spec = agent.getSpecification(currentUris[0]);
            assertEquals("Next backup must be correct size in order...",
                    1000 + i * 10, spec.getFileLength(0));

            // Delete the backup from the list.
            boolean success = agent.delete(currentUris[0]);
            assertTrue("Backup deleted successfully", success);
        }

        // Should not be any remaining backups.
        assertEquals("After deletion no remaining backups expected", 0, agent
                .list().length);

        agent.release();
    }

    // Configure the storage service.
    protected StorageAgent configureStorageService(String name, int retention)
            throws BackupException
    {
        // Set up file storage.
        File directory = new File(STORAGE_DIR_NAME + File.separator + name);
        logger.debug("Preparing storage directory for test: "
                + directory.getAbsolutePath());
        if (directory.exists())
            deleteRecursive(directory);
        directory.mkdirs();

        // Create storage instance.
        FileSystemStorageAgent agent = new FileSystemStorageAgent();
        agent.setDirectory(directory);
        agent.setRetention(retention);

        agent.configure();
        return agent;
    }

    // Recursively delete directory
    protected void deleteRecursive(File f)
    {
        if (f.isDirectory())
        {
            for (File child : f.listFiles())
            {
                deleteRecursive(child);
            }
        }
        logger.debug("Deleting file: " + f.getAbsolutePath());
        f.delete();
    }

    // Create a new file and return a backup specification for same.
    protected File createFile(String prefix, long size) throws Exception
    {
        // Create random file data.
        String base = "abcdefghijklmnopqrstuvwxyz";
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < 10; i++)
        {
            char c = base.charAt((int) (Math.random() * base.length()));
            sb.append(c);
        }
        String contents = sb.toString();

        // Create and fill the file with data.
        File f = File.createTempFile(prefix, ".dat");
        FileWriter fw = new FileWriter(f);
        int contentIndex = 0;
        for (int i = 0; i < size; i++)
        {
            if (contentIndex >= contents.length())
                contentIndex = 0;
            fw.write(contents.charAt(contentIndex));
        }
        fw.flush();
        fw.close();
        logger.debug("Generated test file: name=" + f.getAbsolutePath()
                + " length=" + f.length());

        return f;
    }

    // Creates a file and accompanying backup specification.
    protected BackupSpecification createBackup(String prefix, int length)
            throws Exception
    {
        File f = createFile(prefix, length);
        BackupSpecification bspec = new BackupSpecification();
        bspec.setAgentName("dummy");
        bspec.setBackupDate(new Date());
        bspec.addBackupLocator(new FileBackupLocator(f, false));
        return bspec;
    }

    // Compare two files and throw exception if they are not equal.
    protected void compareFile(File in, File out) throws Exception
    {
        long len = in.length();
        if (len != out.length())
        {
            throw new Exception("Files have different length: in="
                    + in.getAbsolutePath() + " length=" + in.length() + " out="
                    + out.getAbsolutePath() + " length=" + out.length());
        }
    }
}