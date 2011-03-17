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
public class MonitorThreadInfo implements Serializable
{
    @SuppressWarnings("unused")
    private static Logger      logger          = Logger.getLogger(MonitorThreadInfo.class);

    /* List of named/interesting threads */
    static public final String   THLSRV     = "THLSrv";
    static public final String   THLCLI     = "THLCli";
    static public final String   APPLIER    = "Applier";
    static public final String   EXTRACTOR  = "Extract";

    private static final long   serialVersionUID = 1L;
    private long                threadID;
    private String              name;
    private long                cpuTime;
    private MonitorThreadBucket cpuTimes[];
    private MonitorThreadBucket realTimes[];
    private MonitorThreadBucket counters[];

    public MonitorThreadInfo(long threadID, String name, long cpuTime)
    {
        int i;

        this.threadID = threadID;
        this.name     = name;
        this.cpuTime  = cpuTime;

        this.cpuTimes = new MonitorThreadBucket[ReplicatorMonitor.CPU_COUNT];
        for (i = 0; i < ReplicatorMonitor.CPU_COUNT; i++) this.cpuTimes[i] = new MonitorThreadBucket();

        this.realTimes = new MonitorThreadBucket[ReplicatorMonitor.REAL_COUNT];
        for (i = 0; i < ReplicatorMonitor.REAL_COUNT; i++) this.realTimes[i] = new MonitorThreadBucket();

        this.counters = new MonitorThreadBucket[ReplicatorMonitor.COUNT_COUNT];
        for (i = 0; i < ReplicatorMonitor.COUNT_COUNT; i++) this.counters[i] = new MonitorThreadBucket();
    }

    public void recordEvent(int eventID, long value)
    {
        counters[eventID].increment(value);
    }

    public void cpuTimeEvent(int eventID, long value)
    {
        cpuTimes[eventID].increment(value);
    }

    public void realTimeEvent(int eventID, long value)
    {
        realTimes[eventID].increment(value);
    }

    public void counterEvent(int eventID, long value)
    {
        counters[eventID].increment(value);
    }

    public void setCPUTimes(MonitorThreadBucket cpuTimes[])
    {
        this.cpuTimes = cpuTimes;
    }
    public MonitorThreadBucket[] getCPUTimes()
    {
        return this.cpuTimes;
    }

    public void setRealTimes(MonitorThreadBucket realTimes[])
    {
        this.realTimes = realTimes;
    }
    public MonitorThreadBucket[] getRealTimes()
    {
        return this.realTimes;
    }

    public void setCounters(MonitorThreadBucket counters[])
    {
        this.counters = counters;
    }

    public MonitorThreadBucket[] getCounters()
    {
        return this.counters;
    }

    public void setThreadID(long threadID)
    {
        this.threadID = threadID;
    }
    public long getThreadID()
    {
        return this.threadID;
    }
    public void setName(String name)
    {
        this.name = name;
    }
    public String getName()
    {
        return this.name;
    }
    public void setCPUTime(long cpuTime)
    {
        this.cpuTime = cpuTime;
    }
    public long getCPUTime()
    { 
        return this.cpuTime;
    }
}





