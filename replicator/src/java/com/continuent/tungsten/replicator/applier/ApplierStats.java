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

package com.continuent.tungsten.replicator.applier;

import java.io.Serializable;

/**
 * 
 * This class defines a ReplicationEventApplierStatistics
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class ApplierStats implements Serializable
{
    static final long serialVersionUID = 288282825L;

    long              applied          = 0;
    long              firstAppliedTime = -1;
    long              lastAppliedTime  = -1;

    /**
     * 
     * Creates a new <code>ReplicationEventApplierStatistics</code> object
     * 
     */
    public ApplierStats()
    {

    }

    /**
     * 
     * Creates a new <code>ApplierStatistics</code> object
     * 
     * @param stats
     */
    public ApplierStats(ApplierStats stats)
    {
        applied = stats.applied;
        firstAppliedTime = stats.firstAppliedTime;
        lastAppliedTime = stats.lastAppliedTime;
    }

    /**
     * 
     * TODO: updateApplied definition.
     * 
     */
    public void updateApplied()
    {
        long time = new java.util.Date().getTime();
        if (firstAppliedTime == -1)
            firstAppliedTime = time;
        lastAppliedTime = time;
        applied++;
    }

    /**
     * 
     * TODO: getApplied definition.
     * 
     * @return applied counter
     */
    public long getApplied()
    {
        return applied;
    }

    /**
     * 
     * TODO: getSeconds definition.
     * 
     * @return applier uptime in seconds 
     */
    public double getSeconds()
    {
        return 1.e-3*(lastAppliedTime - firstAppliedTime);
    }
    
    /**
     * 
     * TODO: getAppliedPerSec definition.
     * 
     * @return apllying rate statistics
     */
    double getAppliedPerSec()
    {
        if (lastAppliedTime == firstAppliedTime)
            return 0.;
        return applied / getSeconds();
    }

    
    /**
     * 
     * {@inheritDoc}
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        return "applied " + getApplied() + " in "
                + getSeconds() + " secs "
                + getAppliedPerSec() + "/sec";
    }

}
