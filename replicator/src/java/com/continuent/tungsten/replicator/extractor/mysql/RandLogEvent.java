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
 * Initial developer(s): Seppo Jaakola
 * Contributor(s): Stephane Giron
 */

package com.continuent.tungsten.replicator.extractor.mysql;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.extractor.mysql.conversion.LittleEndianConversion;

/**
 * @author <a href="mailto:seppo.jaakola@continuent.com">Seppo Jaakola</a>
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class RandLogEvent extends LogEvent
{
    /**
     * Fixed data part: Empty
     * <p>
     * Variable data part:
     * <ul>
     * <li>8 bytes. The value for the first seed.</li>
     * <li>8 bytes. The value for the second seed. </li>
     * </ul>
     * Source : http://forge.mysql.com/wiki/MySQL_Internals_Binary_Log
     */

    static Logger  logger = Logger.getLogger(MySQLExtractor.class);

    private String query;
    private long   seed1;
    private long   seed2;

    public String getQuery()
    {
        return query;
    }

    public RandLogEvent(byte[] buffer, int eventLength,
            FormatDescriptionLogEvent descriptionEvent)
            throws ReplicatorException
    {
        super(buffer, descriptionEvent, MysqlBinlog.RAND_EVENT);

        int commonHeaderLength, postHeaderLength;
        int offset;

        commonHeaderLength = descriptionEvent.commonHeaderLength;
        postHeaderLength = descriptionEvent.postHeaderLength[type - 1];

        if (logger.isDebugEnabled())
            logger.debug("event length: " + eventLength
                    + " common header length: " + commonHeaderLength
                    + " post header length: " + postHeaderLength);

        offset = commonHeaderLength + postHeaderLength
                + MysqlBinlog.RAND_SEED1_OFFSET;

        /*
         * Check that the event length is greater than the calculated offset
         */
        if (eventLength < offset)
        {
            throw new MySQLExtractException("rand event length is too short");
        }

        try
        {
            seed1 = LittleEndianConversion.convert8BytesToLong(buffer, offset);
            offset = offset + MysqlBinlog.RAND_SEED2_OFFSET;
            seed2 = LittleEndianConversion.convert8BytesToLong(buffer, offset);

            query = new String("SET SESSION rand_seed1 = " + seed1
                    + " , rand_seed2 = " + seed2);
        }
        catch (Exception e)
        {
            throw new MySQLExtractException("Unable to read rand event", e);
        }

        return;
    }
}
