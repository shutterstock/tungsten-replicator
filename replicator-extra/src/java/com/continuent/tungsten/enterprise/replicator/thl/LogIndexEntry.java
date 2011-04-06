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
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.enterprise.replicator.thl;

/**
 * Implements a sortable index entry, where entries are sorted by sequence
 * number.
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
// Class to implement a sortable index entry, where entries
// are sorted by sequence number.
public class LogIndexEntry implements Comparable<LogIndexEntry>
{
    long   startSeqno;
    long   endSeqno;
    String fileName;

    /**
     * Creates a new <code>IndexEntry</code> object
     * 
     * @param startSeqno
     * @param fileName
     */
    public LogIndexEntry(long startSeqno, long endSeqno, String fileName)
    {
        this.startSeqno = startSeqno;
        this.endSeqno = endSeqno;
        this.fileName = fileName;
    }

    /** Returns true if the index entry contains this sequence number. */
    public boolean hasSeqno(long seqno)
    {
        return (seqno >= startSeqno && seqno <= endSeqno);
    }

    /**
     * Implementation required for Comparable so that we can sort entries.
     */
    public int compareTo(LogIndexEntry o)
    {
        if (this.startSeqno < o.startSeqno)
            return -1;
        else if (this.startSeqno == o.startSeqno)
            return 0;
        else
            return 1;
    }

    /** Returns true if the given seqno is in the file that this entry indexes. */
    public boolean contains(long seqno)
    {
        return startSeqno <= seqno && seqno <= endSeqno;
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        return this.getClass().getSimpleName() + " " + fileName + "("
                + startSeqno + ":" + endSeqno + ")";
    }
}
