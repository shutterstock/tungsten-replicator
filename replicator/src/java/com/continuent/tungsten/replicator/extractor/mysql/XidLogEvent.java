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

import java.io.IOException;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.extractor.mysql.conversion.LittleEndianConversion;

/**
 * @author <a href="mailto:seppo.jaakola@continuent.com">Seppo Jaakola</a>
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class XidLogEvent extends LogEvent
{
    long xid;

    public XidLogEvent(byte[] buffer, int eventLength,
            FormatDescriptionLogEvent descriptionEvent)
            throws ReplicatorException
    {
        super(buffer, descriptionEvent, MysqlBinlog.XID_EVENT);
        try
        {
            xid = LittleEndianConversion.convert8BytesToLong(buffer,
                    descriptionEvent.commonHeaderLength);
        }
        catch (IOException e)
        {
            logger.error("could not extract trx id: " + e);
            throw new MySQLExtractException(
                    "problem in extracting xid_log_event");
        }
    }

    public long getXid()
    {
        return xid;
    }
}
