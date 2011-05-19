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
 * Initial developer(s): Seppo Jaakola
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.commons.cluster.resource.physical;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.continuent.tungsten.commons.config.TungstenProperties;

/**
 * Defines capabilities of an open replicator. Managers can use this to
 * determine operations supported by a particular replicator type.
 * 
 * @author <a href="mailto:seppo.jaakola@continuent.com">Seppo Jaakola</a>
 * @version 1.0
 */
public class ReplicatorCapabilities extends Capabilities
        implements
            Serializable
{
    private static final long   serialVersionUID = -3752349488516771325L;

    /*
     * Null or missing value.
     */
    public static final String  UNKNOWN          = "unknown";

    /*
     * Possible roles for replicator, these can be used in
     * OpenReplicatorPlugin::setRole() method
     */
    public static final String  ROLES            = "roles";
    public static final String  ROLE_MASTER      = "master";
    public static final String  ROLE_SLAVE       = "slave";
    public static final String  ROLE_STANDBY     = "standby";

    /*
     * Replication model: push = master pushes replication events to slaves pull
     * = slave polls replication events from master peer = true multi-master,
     * all nodes are identical
     */
    public static final String  MODEL            = "model";
    public static final String  MODEL_PUSH       = "push";
    public static final String  MODEL_PULL       = "pull";
    public static final String  MODEL_PEER       = "peer";

    /*
     * Driver for provisioning donor = donor (master) sends DB state to joining
     * node (slave) joiner = joining node (slave) polls donor (master) for DB
     * state
     */
    private static final String PROVISION        = "provision";
    public static final String  PROVISION_DONOR  = "donor";
    public static final String  PROVISION_JOINER = "joiner";

    // Replicator capabilities.
    private static final String CAP_CONSISTENCY  = "consistency";
    private static final String CAP_HEARTBEAT    = "heartbeat";
    private static final String CAP_FLUSH        = "flush";

    private ArrayList<String>   roles            = new ArrayList<String>();
    private String              model            = UNKNOWN;

    /* driver for provisioning: master or slave */
    private String              provisionDriver  = UNKNOWN;

    /* is consistency check supported */
    private boolean             isConsistencyCheck;

    /* is heartbeat supported */
    private boolean             isHeartbeat;

    /* is flush supported */
    private boolean             isFlush;

    /**
     * Create an instance with default properties.
     */
    public ReplicatorCapabilities()
    {
    }

    /**
     * Create a capabilities instance from TungstenProperties instance.
     */
    public ReplicatorCapabilities(TungstenProperties props) throws Exception
    {
        Set<String> keys = props.keyNames();
        for (String key : keys)
        {
            if (key.equals(ROLES))
            {
                List<String> tags = props.getStringList(key);
                for (String tag : tags)
                {
                    if (tag.equalsIgnoreCase(ROLE_MASTER))
                    {
                        addRole(ReplicatorCapabilities.ROLE_MASTER);
                    }
                    else if (tag.equalsIgnoreCase(ROLE_SLAVE))
                    {
                        addRole(ReplicatorCapabilities.ROLE_SLAVE);
                    }
                    else if (tag.equalsIgnoreCase(ROLE_STANDBY))
                    {
                        addRole(ReplicatorCapabilities.ROLE_STANDBY);
                    }
                    else
                    {
                        throw new Exception("Unknown capability role: " + tag);
                    }
                }
            }
            else if (key.equals(MODEL))
            {
                String tag = props.getString(key);
                if (tag.equalsIgnoreCase(MODEL_PUSH))
                {
                    setModel(MODEL_PUSH);
                }
                else if (tag.equalsIgnoreCase(MODEL_PULL))
                {
                    setModel(MODEL_PULL);
                }
                else if (tag.equalsIgnoreCase(MODEL_PEER))
                {
                    setModel(MODEL_PEER);
                }
                else if (tag.equalsIgnoreCase(UNKNOWN))
                {
                    setModel(UNKNOWN);
                }
                else
                {
                    throw new Exception("Unknown value for model: " + tag);
                }
            }
            else if (key.equals(CAP_CONSISTENCY))
            {
                setConsistencyCheck(props.getBoolean(CAP_CONSISTENCY));
            }
            else if (key.equals(CAP_HEARTBEAT))
            {
                setHeartbeat(props.getBoolean(CAP_HEARTBEAT));
            }
            else if (key.equals(CAP_FLUSH))
            {
                setFlush(props.getBoolean(CAP_FLUSH));
            }
            else if (key.equals(PROVISION))
            {
                String tag = props.getString(key);
                if (tag.equalsIgnoreCase(PROVISION_DONOR))
                {
                    setProvisionDriver(PROVISION_DONOR);
                }
                else if (tag.equalsIgnoreCase(PROVISION_JOINER))
                {
                    setProvisionDriver(PROVISION_JOINER);
                }
                else if (tag.equalsIgnoreCase(UNKNOWN))
                {
                    setProvisionDriver(UNKNOWN);
                }
                else
                {
                    throw new Exception("Unknown provision driver: " + tag);
                }
            }
            else
            {
                throw new Exception("Unknown capability: " + key);
            }
        }
    }

    public String getModel()
    {
        return model;
    }

    public void setModel(String model)
    {
        this.model = model;
    }

    public List<String> getRoles()
    {
        return roles;
    }

    public void setRoles(ArrayList<String> roles)
    {
        this.roles = roles;
    }

    public void addRole(String role)
    {
        roles.add(role);
    }

    public boolean isConsistencyCheck()
    {
        return isConsistencyCheck;
    }

    public void setConsistencyCheck(boolean isConcistencyCheck)
    {
        this.isConsistencyCheck = isConcistencyCheck;
    }

    public boolean isFlush()
    {
        return isFlush;
    }

    public void setFlush(boolean isFlush)
    {
        this.isFlush = isFlush;
    }

    public boolean isHeartbeat()
    {
        return isHeartbeat;
    }

    public void setHeartbeat(boolean isHeartbeat)
    {
        this.isHeartbeat = isHeartbeat;
    }

    public String getProvisionDriver()
    {
        return provisionDriver;
    }

    public void setProvisionDriver(String provisionDriver)
    {
        this.provisionDriver = provisionDriver;
    }

    /**
     * Writes capabilities to a properties instance for storage or transport.
     */
    public TungstenProperties asProperties()
    {
        TungstenProperties caps = new TungstenProperties();

        caps.setStringList(ROLES, getRoles());
        caps.setString(MODEL, getModel());
        caps.setBoolean(CAP_CONSISTENCY, this.isConsistencyCheck());
        caps.setBoolean(CAP_HEARTBEAT, this.isHeartbeat());
        caps.setBoolean(CAP_FLUSH, this.isFlush());
        caps.setString(PROVISION, getProvisionDriver());

        return caps;
    }
    
    /**
     * Writes capabilities to a HashMap instance for storage or transport.
     */
    public Map<String, String> asMap()
    {
        return asProperties().hashMap();
    }
}