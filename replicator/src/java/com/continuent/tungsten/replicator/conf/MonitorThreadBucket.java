/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2009 Continuent Inc.
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
 * Initial developer(s): Scott Martin
 * Contributor(s): 
 */
package com.continuent.tungsten.replicator.conf;

import java.io.Serializable;
import org.apache.log4j.Logger;

/**
 * This class implements a storage location for thread information
 * relevant to performance
 * 
 * @author <a href="mailto:scott.martin@continuent.com">Scott Martin</a>
 * @version 1.0
 */
public class MonitorThreadBucket implements Serializable
{
    @SuppressWarnings("unused")
    private static Logger      logger          = Logger.getLogger(MonitorThreadBucket.class);
    private static final long   serialVersionUID = 1L;
    private int                 count = 0 ;
    private long                value = 0L;

    public MonitorThreadBucket()
    {
       this.count = 0;
       this.value = 0L;
    }

    public void setCount(int count)
    {
       this.count = count;
    }
    public int getCount()
    {
       return this.count;
    }
    public void setValue(long value)
    {
       this.value = value;
    }
    public long getValue()
    {
       return this.value;
    }
    public void clear()
    {
        this.count = 0;
        this.value = 0L;
    }
    public void increment(long amount)
    {
        this.count++;
        this.value += amount;
        //logger.info("count = " + count + " amount = " + amount + " value = " + value);
    }
}





