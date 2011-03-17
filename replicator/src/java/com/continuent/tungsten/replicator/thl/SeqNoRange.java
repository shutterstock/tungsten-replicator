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

import java.io.Serializable;

/**
 * 
 * This class defines a SeqNoRange
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class SeqNoRange implements Serializable
{
    static final long serialVersionUID = 345634563456L;
    long minSeqNo;
    long maxSeqNo;
    
    /**
     * 
     * Creates a new <code>SeqNoRange</code> object
     * 
     * @param minSeqNo
     * @param maxSeqNo
     */
    public SeqNoRange(long minSeqNo, long maxSeqNo)
    {
        this.minSeqNo = minSeqNo;
        this.maxSeqNo = maxSeqNo;
        
    }
    
    /**
     * 
     * TODO: getMinSeqNo definition.
     * 
     * @return min seqno
     */
    public long getMinSeqNo()
    {
        return minSeqNo;
    }
    
    /**
     * 
     * TODO: getMaxSeqNo definition.
     * 
     * @return max seqno
     */
    public long getMaxSeqNo()
    {
        return maxSeqNo;
    }
}
