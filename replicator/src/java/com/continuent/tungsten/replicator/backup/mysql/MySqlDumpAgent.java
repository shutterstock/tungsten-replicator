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

package com.continuent.tungsten.replicator.backup.mysql;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.backup.AbstractBackupAgent;
import com.continuent.tungsten.replicator.backup.BackupCapabilities;
import com.continuent.tungsten.replicator.backup.BackupException;
import com.continuent.tungsten.replicator.backup.BackupLocator;
import com.continuent.tungsten.replicator.backup.BackupSpecification;
import com.continuent.tungsten.replicator.backup.FileBackupLocator;
import com.continuent.tungsten.replicator.backup.ProcessHelper;

/**
 * Implements a backup agent that works using mysqldump to dump data and mysql
 * to restore.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class MySqlDumpAgent extends AbstractBackupAgent
{
    private static Logger      logger           = Logger
                                                        .getLogger(MySqlDumpAgent.class);

    // Backup parameters.
    private String             host             = "localhost";
    private int                port             = 3306;
    private String             user             = "root";
    private String             password         = "";
    private String             dumpDirName      = "/tmp";
    private String             mysqldumpOptions = "--all-databases --skip-lock-tables";
    private String             mysqlOptions     = "";
    private boolean            hotBackupEnabled = false;

    // Private data.
    private File               dumpDir;
    private String[]           mysqldumpCommandArray;
    private String[]           mysqlCommandArray;
    private ProcessHelper      processHelper;

    public MySqlDumpAgent()
    {
    }

    public String getHost()
    {
        return host;
    }

    public void setHost(String host)
    {
        this.host = host;
    }

    public int getPort()
    {
        return port;
    }

    public void setPort(int port)
    {
        this.port = port;
    }

    public String getUser()
    {
        return user;
    }

    public void setUser(String user)
    {
        this.user = user;
    }

    public String getPassword()
    {
        return password;
    }

    public void setPassword(String password)
    {
        this.password = password;
    }

    public String getDumpDirName()
    {
        return dumpDirName;
    }

    public void setDumpDir(String dumpDirName)
    {
        this.dumpDirName = dumpDirName;
    }

    public String getMysqldumpOptions()
    {
        return mysqldumpOptions;
    }

    public void setMysqldumpOptions(String mysqldumpOptions)
    {
        this.mysqldumpOptions = mysqldumpOptions;
    }

    public String getMysqlOptions()
    {
        return mysqlOptions;
    }

    public void setMysqlOptions(String mysqlOptions)
    {
        this.mysqlOptions = mysqlOptions;
    }

    public boolean isHotBackupEnabled()
    {
        return hotBackupEnabled;
    }

    public void setHotBackupEnabled(boolean hotBackupEnabled)
    {
        this.hotBackupEnabled = hotBackupEnabled;
    }

    public BackupSpecification backup() throws BackupException
    {
        BackupSpecification spec = new BackupSpecification();
        spec.setBackupDate(new Date());

        File dumpFile = null;
        FileWriter fw = null;
        try
        {
            // Create temp file and add it with commands to turn off logging.
            dumpFile = File.createTempFile("mysqldump-", ".sql", dumpDir);
            logger.info("Selecting temp file for database dump: "
                    + dumpFile.getAbsolutePath());

            fw = new FileWriter(dumpFile);
            BufferedWriter bw = new BufferedWriter(fw);
            bw
                    .write("-- Tungsten database dump - should not be logged on restore");
            bw.newLine();
            bw.write("SET SESSION SQL_LOG_BIN=0;");
            bw.newLine();
            bw.flush();
            fw.close();

            // Execute mysqldump utility.
            processHelper.exec("Dumping database using mysqldump",
                    mysqldumpCommandArray, null, dumpFile, null, true, false);
        }
        catch (BackupException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            throw new BackupException("Unexpected error on backup: "
                    + e.getMessage(), e);
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

        spec.addBackupLocator(new FileBackupLocator(dumpFile, true));
        return spec;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.backup.BackupPlugin#configure()
     */
    public void configure() throws BackupException
    {
        // Configure process helper. We don't need a command prefix as
        // mysql[dump] can run using any login.
        processHelper = new ProcessHelper();
        processHelper.configure();

        // Ensure the dump directory exists.
        dumpDir = new File(dumpDirName);
        if (dumpDir.canRead() && dumpDir.isDirectory())
            logger.info("Setting dump directory for backups: "
                    + dumpDir.getAbsolutePath());
        else
            throw new BackupException(
                    "Dump directory not found or not readable: "
                            + dumpDir.getAbsolutePath());

        // Create the mysqldump command.
        String[] mysqldumpBase = {"mysqldump", "-u" + user, "-p" + password,
                "-h" + host, "-P" + port};
        mysqldumpCommandArray = processHelper.mergeArrays(mysqldumpBase,
                mysqldumpOptions.split("\\s"));

        // Create the mysql command.
        String[] mysqlBase = {"mysql", "-u" + user, "-p" + password,
                "-h" + host, "-P" + port};
        mysqlCommandArray = processHelper.mergeArrays(mysqlBase, mysqlOptions
                .split(" "));

        // Record capabilities.
        capabilities = new BackupCapabilities();
        capabilities.setHotBackupEnabled(hotBackupEnabled);
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
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.backup.AbstractBackupAgent#restoreOneLocator(com.continuent.tungsten.replicator.backup.BackupLocator)
     */
    @Override
    protected void restoreOneLocator(BackupLocator locator)
            throws BackupException, FileNotFoundException
    {
        FileInputStream fis = null;
        try
        {
            // Load the backup storage.
            locator.open();

            // Execute mysql utility to restore.
            logger.info("Restoring database file: "
                    + locator.getContents().getAbsolutePath());
            fis = new FileInputStream(locator.getContents());
            processHelper.exec("Restoring database using mysql",
                    mysqlCommandArray, fis, null, null, false, false);
        }
        catch (BackupException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            throw new BackupException("Unexpected error on restore: "
                    + e.getMessage(), e);
        }
        finally
        {
            if (fis != null)
            {
                try
                {
                    fis.close();
                }
                catch (IOException e)
                {
                }
            }
        }
    }
}