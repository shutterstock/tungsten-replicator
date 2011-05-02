/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2008 Continuent Inc.
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
 * Initial developer(s): Seppo Jaakola
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.extractor.mysql;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;

import org.apache.log4j.Logger;

/**
 * Position inside binlog file
 * 
 * @author <a href="mailto:seppo.jaakola@continuent.com">Seppo Jaakola</a>
 * @version 1.0
 */
public class BinlogPosition implements FilenameFilter, Cloneable
{
    static Logger               logger = Logger.getLogger(MySQLExtractor.class);

    /* if binlog file is open, we have this stream to read from */
    private BufferedInputStream bIS;
    private FileInputStream     fIS;
    private DataInputStream     dIS;

    /* binlog file's name and directory */
    private String              fileName;
    private String              directory;

    /* position in file */
    private long                position;

    /* id of last event read */
    private int                 eventID;

    /* binlog file's base name */
    private String              baseName;

    private int                 bufferSize;

    /*
     * @brief defines only binlog directory and binlog files' base name
     * @param directory directory path where binlog files should reside
     * @param baseName file name pattern for binlog files: basenName001.bin
     */
    public BinlogPosition(String directory, String baseName, int bufferSize)
    {
        fIS = null;
        dIS = null;
        bIS = null;

        setPosition(0);
        this.eventID = 0;
        setFileName(null);
        setDirectory(directory);
        setBaseName(baseName);
        this.bufferSize = bufferSize;
    }

    /*
     * @brief defines all possible binlog parameters
     * @param position location in the file, will skip until that
     * @param fileName full file path name to open
     * @param directory directory path where binlog files should reside
     * @param baseName file name pattern for binlog files: basenName001.bin
     */
    public BinlogPosition(long position, String fileName, String directory,
            String baseName, int bufferSize) throws MySQLExtractException
    {
        fIS = null;
        dIS = null;
        bIS = null;

        setPosition(position);
        this.eventID = 0;
        setFileName(fileName);
        setDirectory(directory);
        setBaseName(baseName);
        this.bufferSize = bufferSize;
        openFile();
    }

    public BinlogPosition clone()
    {
        BinlogPosition cloned = new BinlogPosition(directory, baseName,
                bufferSize);

        /* binlog file's name and directory */
        cloned.setFileName(fileName);

        /* position in file */
        cloned.setPosition(position);

        /* id of last event read */
        cloned.setEventID(eventID);

        return cloned;
    }

    /*
     * @brief resets binlog position, but leaves directory and base name intact
     */
    public void reset() throws MySQLExtractException
    {
        try
        {
            dIS.close();
            bIS.close();
            fIS.close();
        }
        catch (IOException e)
        {
            throw new MySQLExtractException(
                    "Failed to close file input streams", e);
        }
        dIS = null;
        bIS = null;
        fIS = null;

        setPosition(0);
        setEventID(0);
        setFileName(null);
    }

    void openFile() throws MySQLExtractException
    {
        try
        {
            if (getFileName() == null)
            {
                logger.error("binlog filename not specified, cannot open");
                throw new MySQLExtractException("No binlog file specified");
            }

            // Hack to avoid crashing during log rotate. MySQL seems to write
            // log rotate event in the old file before creating new file. We
            // wait for a second, polling file every 10 msecs.
            File file = new File(getDirectory() + File.separator
                    + getFileName());
            int tryCnt = 0;
            while (file.exists() == false && tryCnt++ < 500)
            {
                Thread.sleep(10);
            }

            if (logger.isDebugEnabled())
                logger.debug("Opening file " + file.getName()
                        + " with buffer = " + bufferSize);

            fIS = new FileInputStream(file);
            bIS = new BufferedInputStream(fIS, bufferSize);
            dIS = new DataInputStream(bIS);

            /*
             * if we have predefined position to start from, let's skip until
             * the position. This situation can happen, if extractor is started
             * from a known position.
             */
            if (getPosition() > 0)
            {
                bIS.skip(getPosition());
            }
        }
        catch (FileNotFoundException e)
        {
            logger.error("unable to open binlog file: " + e);
            throw new MySQLExtractException("unable to open binlog file:" + e);
        }
        catch (IOException e)
        {
            logger.error("unable to scan binlog file: " + e);
            throw new MySQLExtractException("unable to scan binlog file:" + e);
        }
        catch (InterruptedException e)
        {
            logger.error("unable to open binlog file: " + e);
            throw new MySQLExtractException("unable to open binlog file");
        }
    }

    public void read(byte[] buf) throws IOException, MySQLExtractException
    {
        int pos = 0;
        int remaining = buf.length;

        while (remaining > 0)
        {
            int readLen = 0;
            try
            {
                readLen = getDataInputStream().read(buf, pos, remaining);
            }
            catch (EOFException e)
            {
                // writer has not finished yet
                logger.debug("got EOF during binlog read");
            }
            catch (IOException e)
            {
                break;
            }
            remaining -= readLen;
            if (remaining > 0)
            {
                logger.debug("could not read full buffer");
                pos += readLen;

                try
                {
                    Thread.sleep(10);
                }
                catch (InterruptedException e)
                {
                    logger.error("sleep failed during binlog reading" + e);
                    throw (new MySQLExtractException("binlog read failure"));
                }
            }
        }

        // update file position
        setPosition(getPosition() + buf.length);

        return;
    }

    /* member getters and setters */
    public void setPosition(long newPosition)
    {
        position = newPosition;
    }

    public long getPosition()
    {
        return position;
    }

    public void setFileName(String fileName)
    {
        this.fileName = fileName;
    }

    public String getFileName()
    {
        return (fileName);
    }

    public void setDirectory(String directory)
    {
        this.directory = directory;
    }

    public String getDirectory()
    {
        return (directory);
    }

    public boolean accept(File dir, String name)
    {
        if (name.startsWith(baseName))
            return true;
        return false;
    }

    public String getBaseName()
    {
        return baseName;
    }

    public void setBaseName(String baseName)
    {
        this.baseName = baseName;
    }

    public InputStream getFileInputStream()
    {
        return bIS;
    }

    public DataInputStream getDataInputStream()
    {
        return dIS;
    }

    public int getEventID()
    {
        return eventID;
    }

    public void setEventID(int eventID)
    {
        this.eventID = eventID;
    }

    @Override
    public String toString()
    {
        return fileName + " (" + getPosition() + ")";
    }
}
