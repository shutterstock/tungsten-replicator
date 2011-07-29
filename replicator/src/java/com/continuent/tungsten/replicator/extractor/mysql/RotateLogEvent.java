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

/**
 * @author <a href="mailto:seppo.jaakola@continuent.com">Seppo Jaakola</a>
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class RotateLogEvent extends LogEvent
{
    /**
     * Fixed data part:
     * <ul>
     * <li>8 bytes. The position of the first event in the next log file.
     * Always contains the number 4 (meaning the next event starts at position 4
     * in the next binary log). This field is not present in v1; presumably the
     * value is assumed to be 4.</li>
     * </ul>
     * <p>
     * Variable data part:
     * <ul>
     * <li>The name of the next binary log. The filename is not
     * null-terminated. Its length is the event size minus the size of the fixed
     * parts. </li>
     * </ul>
     * Source : http://forge.mysql.com/wiki/MySQL_Internals_Binary_Log
     */
    static Logger  logger = Logger.getLogger(MySQLExtractor.class);

    private int    filenameLength;
    private String filename;

    public String getNewBinlogFilename()
    {
        return filename;
    }

    /**
     * Creates a new <code>Rotate_log_event</code> object read normally from
     * log.
     * @throws ReplicatorException 
     */
    public RotateLogEvent(byte[] buffer, int eventLength,
            FormatDescriptionLogEvent descriptionEvent) throws ReplicatorException
    {
        super(buffer, descriptionEvent, MysqlBinlog.START_EVENT_V3);
        
        type = MysqlBinlog.ROTATE_EVENT;

        int headerSize = descriptionEvent.commonHeaderLength;
        int postHeaderLength = descriptionEvent.postHeaderLength[MysqlBinlog.ROTATE_EVENT - 1];
        int filenameOffset = headerSize + postHeaderLength;

        if (eventLength < headerSize)
        {
            throw new MySQLExtractException("Rotate event length is too short");
        }

        // Removing code that does not seem to be very useful
        // try
        // {
        // long pos = post_header_len > 0 ? MysqlBinlog.u64intToLong(buffer,
        // MysqlBinlog.R_POS_OFFSET) : 4;
        // }
        // catch (IOException e)
        // {
        // logger.error("rotate event error while reading post header");
        // return;
        // }

        filenameLength = eventLength - filenameOffset;

        if (filenameLength > MysqlBinlog.FN_REFLEN - 1)
        {
            filenameLength = MysqlBinlog.FN_REFLEN - 1;
        }
        filename = new String(buffer, filenameOffset, filenameLength);

        if (logger.isDebugEnabled())
            logger.debug("New binlog file is : " + filename);
        return;
    }

    /**
     * Creates a new <code>Rotate_log_event</code> without log information.
     * This is used to generate missing log rotation events.
     */
    public RotateLogEvent(String newLogFilename)
    {
        this.filename = newLogFilename;
        this.filenameLength = -1;
    }
}
