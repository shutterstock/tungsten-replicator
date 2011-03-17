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
 * Initial developer(s): Edward Archibald
 * Contributor(s): ______________________.
 */

package com.continuent.tungsten.commons.cluster.resource.physical;

import javax.management.remote.JMXConnector;

import com.continuent.tungsten.commons.cluster.resource.Resource;
import com.continuent.tungsten.commons.cluster.resource.ResourceType;
import com.continuent.tungsten.commons.exception.ResourceException;

public class Process extends Resource
{
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private String            service          = null;
    private int               port             = 0;
    private String            member           = null;

    JMXConnector              connection       = null;

    public Process(String name) throws ResourceException
    {
        super(ResourceType.PROCESS, name);
        this.setService(name);
        this.childType = ResourceType.RESOURCE_MANAGER;
        this.isContainer = true;
    }

    public String getMember()
    {
        return member;
    }

    /**
     * @return the port
     */
    public int getPort()
    {
        return port;
    }

    /**
     * @param port the port to set
     */
    public void setPort(int port)
    {
        this.port = port;
    }

    /**
     * @return the service
     */
    public String getService()
    {
        return service;
    }

    /**
     * @param service the service to set
     */
    public void setService(String service)
    {
        this.service = service;
    }

    /**
     * @param member the member name to set
     */
    public void setMember(String member)
    {
        this.member = member;
    }

    public void setConnection(JMXConnector connection)
    {
        this.connection = connection;
    }

}
