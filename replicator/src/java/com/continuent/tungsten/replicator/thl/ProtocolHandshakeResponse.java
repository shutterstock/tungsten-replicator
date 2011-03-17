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
 * Initial developer(s): Teemu Ollakka
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.thl;

/**
 * This class defines a ProtocolHandshakeResponse, which clients return to the
 * THL server.
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class ProtocolHandshakeResponse extends ProtocolMessage
{
    static final long    serialVersionUID = 123452345L;
    private final String sourceId;
    private final long   lastEpochNumber;
    private final long   lastSeqno;

    /**
     * Create a new instance.
     * 
     * @param sourceId Source ID of client.
     */
    public ProtocolHandshakeResponse(String sourceId, long lastEpochNumber,
            long lastSeqno)
    {
        super(null);
        this.sourceId = sourceId;
        this.lastEpochNumber = lastEpochNumber;
        this.lastSeqno = lastSeqno;
    }

    /** Returns the source ID. */
    public String getSourceId()
    {
        return this.sourceId;
    }

    /** Returns the last epoch number in log. */
    public long getLastEpochNumber()
    {
        return this.lastEpochNumber;
    }

    /** Returns the sequence number in log. */
    public long getLastSeqno()
    {
        return this.lastSeqno;
    }
}
