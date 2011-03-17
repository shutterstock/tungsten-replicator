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
 * Initial developer(s): Gilles Rayrat.
 * Contributor(s): ______________________.
 */

package com.continuent.tungsten.commons.cluster.resource.notification;

import com.continuent.tungsten.commons.config.TungstenProperties;

/**
 * Provides utilities for resource status specific properties, specialized in
 * data source representation
 * 
 * @author <a href="mailto:gilles.rayrat@continuent.com">Gilles Rayrat</a>
 * @version 1.0
 */
public class HostStatus extends ResourceStatus
{
    public static final String PROPERTY_KEY_CLUSTER  = "dataServiceName";
    public static final String PROPERTY_KEY_HOST     = "host";
    public static final String PROPERTY_KEY_UPTIME   = "uptime";
    public static final String PROPERTY_LOAD_AVG_1   = "loadAverage1";
    public static final String PROPERTY_LOAD_AVG_5   = "loadAverage5";
    public static final String PROPERTY_LOAD_AVG_15  = "loadAverage5";
    public static final String PROPERTY_KEY_CPUCOUNT = "cpuCount";

    private int                cpuCount              = 1;
    private double             loadAverage1          = 0.0;
    private double             loadAverage5          = 0.0;
    private double             loadAverage15         = 0.0;
    private String             uptime                = null;
    private String             host                  = null;
    private String             service               = null;

    public HostStatus(String type, String name, String state,
            String clusterName, String host, String uptime, int cpuCount,
            double loadAverage1, double loadAverage5, double loadAverage15)
    {
        super(type, name, state, null);
        TungstenProperties hostProperties = new TungstenProperties();
        hostProperties.setString(PROPERTY_KEY_CLUSTER, clusterName);
        hostProperties.setString(PROPERTY_KEY_HOST, host);
        hostProperties.setString(PROPERTY_KEY_UPTIME, uptime);
        hostProperties.setInt(PROPERTY_KEY_CPUCOUNT, cpuCount);
        hostProperties.setDouble(PROPERTY_LOAD_AVG_1, loadAverage1);
        hostProperties.setDouble(PROPERTY_LOAD_AVG_5, loadAverage5);
        hostProperties.setDouble(PROPERTY_LOAD_AVG_15, loadAverage5);

    }

    public int getCpuCount()
    {
        return cpuCount;
    }

    public String getUptime()
    {
        return uptime;
    }

    public String getHost()
    {
        return host;
    }

    public String getService()
    {
        return service;
    }

    public double getLoadAverage1()
    {
        return loadAverage1;
    }

    public double getLoadAverage5()
    {
        return loadAverage5;
    }

    public double getLoadAverage15()
    {
        return loadAverage15;
    }
}
