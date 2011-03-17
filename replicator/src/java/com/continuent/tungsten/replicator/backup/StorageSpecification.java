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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.continuent.tungsten.commons.config.TungstenProperties;

/**
 * Contains storage metadata.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class StorageSpecification
{
    // Property serialization information.
    private static final String VERSION_NO  = "1.0";
    private static final String VERSION     = "version";
    private static final String AGENT       = "agent";
    private static final String FILE_NAME   = "file_name";
    private static final String FILE_LENGTH = "file_length";
    private static final String FILE_CRC    = "file_crc";
    private static final String BACKUP_DATE = "backup_date";
    private static final String URI         = "uri";
    private static final String FILE_COUNT = "archive.file_count";
    private static final String DB_NAME     = "database_name";

    // Specification values.
    private String              version;
    private String              agent;
    private List<String>        fileNames;
    private List<Long>          fileLengths;
    private List<Long>          fileCrcs;
    private Date                backupDate;
    private String              uri;
    private int                 filesCount;
    private List<String>        databaseNames;

    /**
     * Creates a storage specification from existing properties.
     */
    public StorageSpecification(TungstenProperties props)
    {
        this();
        this.version = props.getString(VERSION);
        this.agent = props.getString(AGENT);
        this.uri = props.getString(URI);
        String propFilesCount = props.getString(FILE_COUNT);
        if (propFilesCount != null)
        {
            this.filesCount = Integer.parseInt(propFilesCount);

            for (int i = 0; i < this.filesCount; i++)
            {
                this.fileNames.add(props.getString(buildPropertyName(FILE_NAME,
                        i)));
                this.fileLengths.add(props.getLong(buildPropertyName(
                        FILE_LENGTH, i)));
                this.fileCrcs
                        .add(props.getLong(buildPropertyName(FILE_CRC, i)));
                String dbName = props.getString(buildPropertyName(DB_NAME, i));
                if (dbName != null)
                {
                    this.databaseNames.add(dbName);
                }
            }
        }
        else
        {
            // Try the old specification file format
            this.filesCount = 1;
            this.fileNames.add(props.getString(FILE_NAME));
            this.fileLengths.add(props.getLong(FILE_LENGTH));
            this.fileCrcs.add(props.getLong(FILE_CRC));
        }
    }

    /**
     * Creates a new storage specification whose values must be filled in.
     */
    public StorageSpecification()
    {
        this.version = VERSION_NO;
        fileNames = new ArrayList<String>();
        fileLengths = new ArrayList<Long>();
        fileCrcs = new ArrayList<Long>();
        databaseNames = new ArrayList<String>();
    }

    public String getAgent()
    {
        return agent;
    }

    public void setAgent(String agent)
    {
        this.agent = agent;
    }

    public Date getBackupDate()
    {
        return backupDate;
    }

    public void setBackupDate(Date backupDate)
    {
        this.backupDate = backupDate;
    }

    public long getFileCrc(int index)
    {
        return fileCrcs.get(index);
    }

    public void setFileCrc(long fileCrc)
    {
        this.fileCrcs.add(fileCrc);
    }

    public String getFileName(int index)
    {
        return fileNames.get(index);
    }

    public void setFileName(String fileName)
    {
        this.fileNames.add(fileName);
    }

    public long getFileLength(int index)
    {
        return fileLengths.get(index);
    }

    public void setFileLength(long fileLength)
    {
        this.fileLengths.add(fileLength);
    }

    public String getUri()
    {
        return uri;
    }

    public void setUri(String uri)
    {
        this.uri = uri;
    }

    public String getVersion()
    {
        return version;
    }

    public TungstenProperties toProperties()
    {
        TungstenProperties props = new TungstenProperties(true);
        props.setString(VERSION, version);
        props.setString(AGENT, agent);
        props.setString(URI, uri);
        props.setDate(BACKUP_DATE, backupDate);

        for (int i = 0; i < filesCount; i++)
        {
            props.setString(buildPropertyName(FILE_NAME, i), getFileName(i));
            props.setLong(buildPropertyName(FILE_LENGTH, i), getFileLength(i));
            props.setLong(buildPropertyName(FILE_CRC, i), getFileCrc(i));
            String dbName = getDatabaseName(i);
            if(dbName != null)
            {
                props.setString(buildPropertyName(DB_NAME, i), dbName);
            }
        }

        props.setInt(FILE_COUNT, this.filesCount);
        return props;
    }

    private String buildPropertyName(String propertyName, int propertyNumber)
    {
        return "archive." + propertyNumber + "." + propertyName;
    }

    public void setFilesCount(int count)
    {
        this.filesCount = count;
    }

    public int getFilesCount()
    {
        return this.filesCount;
    }

    public void setDatabaseName(String databaseName)
    {
        this.databaseNames.add(databaseName);
    }
    
    public String getDatabaseName(int index)
    {
        if(databaseNames.size() <= index)
            return null;
        return this.databaseNames.get(index);
    }
    
}