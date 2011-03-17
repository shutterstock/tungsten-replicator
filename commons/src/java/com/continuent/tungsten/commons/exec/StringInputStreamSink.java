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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import org.apache.log4j.Logger;

/**
 * Runnable to read from an InputStream until exhausted or the character limit
 * is exceeded and store results in a String.
 */
public class StringInputStreamSink implements InputStreamSink
{
    private static final Logger logger    = Logger
                                                  .getLogger(StringInputStreamSink.class);
    private static final int    MAX_CHARS = 1000000;
    private final InputStream   inputStream;
    private final String        tag;
    private final int           maxChars;
    private final StringBuffer  output    = new StringBuffer();

    /**
     * Creates a new instance.
     * 
     * @param tag A tag for this processor to help with logging
     * @param in InputStream from which we read
     * @param maxChars Maximum number of characters to read (0 = MAX_CHARS)
     */
    public StringInputStreamSink(String tag, InputStream in, int maxChars)
    {
        this.tag = tag;
        this.inputStream = in;
        if (maxChars <= 0 || maxChars > MAX_CHARS)
            this.maxChars = MAX_CHARS;
        else
            this.maxChars = maxChars;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.exec.InputStreamSink#run()
     */
    public void run()
    {
        Reader reader = new InputStreamReader(inputStream);

        try
        {
            int c;
            int read = 0;

            while ((c = reader.read()) != -1)
            {
                read++;
                if (read <= maxChars)
                {
                    output.append((char) c);
                }
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
