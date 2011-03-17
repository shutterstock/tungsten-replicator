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

/**
 * 
 * This class defines a DummyPlugin
 * 
 * @author <a href="mailto:teemu.ollakka@continuent.com">Teemu Ollakka</a>
 * @version 1.0
 */
public class DummyPlugin implements ReplicatorPlugin
{

    static Logger logger = Logger.getLogger(DummyPlugin.class);
   
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
    
    String a = null;
    public void setA(String a)
    {
        logger.debug("value=" + a);
        this.a = a;
    }

    public String getA()
    {
        return a;
    }
    
    String b = null;
    public void setB(String b)
    {
        logger.debug("value=" + b);        
        this.b = b;
    }

    public String getB()
    {
        return b;
    }

    {
        // TODO Auto-generated method stub
        
    }

}
