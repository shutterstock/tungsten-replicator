/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2010 Continuent Inc.
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
 * Initial developer(s): Linas Virbalas
 * Contributor(s):
 */

package com.continuent.tungsten.commons.exec;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import org.apache.log4j.Logger;

/**
 * Runnable to read from an InputStream until exhausted and append results into
 * a Logger.
 */
public class LoggerInputStreamSink implements InputStreamSink
{
    private static final Logger logger    = Logger
                                                  .getLogger(LoggerInputStreamSink.class);
    private final InputStream   inputStream;
    private final Logger        outLogger;
    private final String        tag;
    private final boolean       info;
    private final StringBuffer  output    = new StringBuffer();

    /**
     * Creates a new instance.
     * 
     * @param tag A tag for this processor to help with logging. If "stderr" is
     *            given, log will be appended into ERROR stream instead of the
     *            usual INFO.
     * @param in InputStream from which we read.
     * @param outLogger Logger to append output of the stream into.
     */
    public LoggerInputStreamSink(String tag, InputStream in, Logger outLogger)
    {
        this.tag = tag;
        if(tag.compareTo("stderr")==0)
            this.info = false;
        else
            this.info = true;
        this.inputStream = in;
        this.outLogger = outLogger;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.exec.InputStreamSink#run()
     */
    public void run()
    {
        Reader reader = new InputStreamReader(inputStream);
        BufferedReader bufferedReader = new BufferedReader(reader);

        try
        {
            String s;
            while ((s = bufferedReader.readLine()) != null)
            {
                if (info)
                    outLogger.info(s);
                else
                    outLogger.error(s);
            }
        }
        catch (IOException ioe)
        {
            logger.warn("[" + tag + "] Error on reading stream data: "
                    + ioe.getMessage(), ioe);
        }
        finally
        {
            // Must close stream in this thread to avoid synchronization
            // problems
            try
            {
                reader.close();
            }
            catch (IOException ioe)
            {
                logger.warn("[" + tag + "] Error while closing stream: "
                        + ioe.getMessage(), ioe);
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
        return output.toString();
    }
}
