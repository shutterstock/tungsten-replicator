/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2010 Continuent Inc.
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
 * Initial developer(s): Stephane Giron
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.thl;

/**
 * This class defines a BinaryEvent
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class THLBinaryEvent
{

    private long seqno;
    private short fragno;
    private boolean lastFrag;
    private byte[] data;
    /**
     * Creates a new <code>THLBinaryEvent</code> object
     * 
     * @param seqno
     * @param fragno
     * @param lastFrag
     * @param data
     */
    public THLBinaryEvent(long seqno, short fragno, boolean lastFrag,
            byte[] data)
    {
        this.seqno = seqno;
        this.fragno = fragno;
        this.lastFrag = lastFrag;
        this.data = data;
    }
    /**
     * Returns the seqno value.
     * 
     * @return Returns the seqno.
     */
    public long getSeqno()
    {
        return seqno;
    }
    /**
     * Returns the fragno value.
     * 
     * @return Returns the fragno.
     */
    public short getFragno()
    {
        return fragno;
    }
    /**
     * Returns the lastFrag value.
     * 
     * @return Returns the lastFrag.
     */
    public boolean isLastFrag()
    {
        return lastFrag;
    }
    /**
     * Returns the data value.
     * 
     * @return Returns the data.
     */
    public byte[] getData()
    {
        return data;
    }
}
