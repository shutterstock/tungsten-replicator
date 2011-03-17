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

package com.continuent.tungsten.replicator.plugin;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;

public class DummyPluginImplementation implements DummyPluginInterface
{
    static Logger logger = Logger.getLogger(DummyPluginImplementation.class);
    public void method1()
    {
        // TODO Auto-generated method stub
        logger.debug("");
    }

    public void method2()
    {
        // TODO Auto-generated method stub
        logger.debug("");
    }

    /**
     * {@inheritDoc}
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#configure(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void configure(PluginContext context) throws ReplicatorException
    {
        // TODO Auto-generated method stub
        logger.debug("");
    }

    /**
     * {@inheritDoc}
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#prepare(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void prepare(PluginContext context) throws ReplicatorException
    {
        // TODO Auto-generated method stub
        logger.debug("");
    }

    /**
     * {@inheritDoc}
     * @see com.continuent.tungsten.replicator.plugin.ReplicatorPlugin#release(com.continuent.tungsten.replicator.plugin.PluginContext)
     */
    public void release(PluginContext context) throws ReplicatorException
    {
        // TODO Auto-generated method stub
        logger.debug("");
    }

    String c = null;
    public void setC(String c)
    {
        logger.debug("value=" + c);
        this.c = c;
    }
    
    public String getC()
    {
        return c;
    }
    
    String s = null;
    public void setStringVal(String s)
    {
        this.s = s;
    }
    
    public String getStringVal()
    {
        return s;
    }
    
    
    Integer i = null;
    public void setIntVal(Integer i)
    {
        this.i = i;
    }

    public Integer getIntVal()
    {
        return i;
    }
    
}
