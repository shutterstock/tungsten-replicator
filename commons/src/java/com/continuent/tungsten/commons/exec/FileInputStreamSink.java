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
 * Initial developer(s): Robert Hodges
 * Contributor(s):
 */

package com.continuent.tungsten.commons.exec;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.log4j.Logger;

/**
 * Runnable to read from an InputStream and store the results in a file.
 */
public class FileInputStreamSink implements InputStreamSink
{
    private final static Logger    logger = Logger
                                                  .getLogger(FileInputStreamSink.class);
    private final String           tag;
    private final InputStream      inputStream;
    private final File             outputFile;
    private final FileOutputStream fos;

    /**
     * Creates a new instance.
     * 
     * @param tag A tag for this processor to help with logging
     * @param in InputStream from which we read
     * @param outputFile File in which to store output
     * @param append If true append output to file, otherwise overwrite
     */
    public FileInputStreamSink(String tag, InputStream in, File outputFile,
            boolean append) throws FileNotFoundException
    {
        this.tag = tag;
        this.inputStream = in;
        this.outputFile = outputFile;
        fos = new FileOutputStream(outputFile, append);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.exec.InputStreamSink#run()
     */
    public void run()
    {

        try
        {
            // Write output from stream.
            byte[] buff = new byte[1024];
            int len = 0;
            while ((len = inputStream.read(buff)) != -1)
            {
                fos.write(buff, 0, len);
            }
        }
        catch (IOException e)
        {
            logger.warn("[" + tag + "] Writing of data to output file "
                    + this.outputFile.getAbsolutePath()
                    + " halted by exception", e);
        }
        finally
        {
            try
            {
                inputStream.close();
            }
            catch (IOException e)
            {
                logger.warn("[" + tag
                        + "] Input stdin close operation generated exception",
                        e);
            }
            try
            {
                fos.close();
            }
            catch (IOException e)
            {
                logger
                        .warn(
                                "["
                                        + tag
                                        + "] Process stdin close operation generated exception",
                                e);
            }
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.exec.InputStreamSink#getOutput()
     */
    public String getOutput()
    {
        return null;
    }
}
