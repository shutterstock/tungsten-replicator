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


public class ProtocolReplEventRequest extends ProtocolMessage
{

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    long seqNo;
    long prefetchRange;
    
    /**
     * 
     * Creates a new <code>ProtocolReplicationDBMSEventRequest</code> object
     * 
     * @param seqNo
     * @param prefetchRange
     */
    public ProtocolReplEventRequest(long seqNo, long prefetchRange)
    {
        super(null);
        this.seqNo = seqNo;
        this.prefetchRange = prefetchRange;
    }

    /**
     * 
     * TODO: getSeqNo definition.
     * 
     * @return seqno
     */
    public long getSeqNo()
    {
        return seqNo;
    }
    
    /**
     * 
     * TODO: getPrefetchRange definition.
     * 
     * @return prefetch range
     */
    public long getPrefetchRange()
    {
        return prefetchRange;
    }

}
