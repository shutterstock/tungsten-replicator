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
 * Initial developer(s): Teemu Ollakka
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.commons.config.cluster;

/**
 * This class defines a TSRouterConf
 * 
 * @author <a href="mailto:edward.archibald@continuent.com">Edward Archibald</a>
 * @version 1.0
 */
public class ConfigurationConstants
{
    /** SERVICE WIDE PROPERTIES */
    static public final String CLUSTER_HOME           = "cluster.home";
    static public final String CLUSTER_CONF_DIR       = "conf";
    static public final String CLUSTER_DIR            = "cluster";
    static public final String CLUSTER_SITENAME       = "siteName";
    static public final String CLUSTER_CLUSTERNAME    = "clusterName";
    static public final String CLUSTER_MEMBERNAME     = "memberName";
    static public final String CLUSTER_PORT           = "port";
    static public final String CLUSTER_DEFAULT_NAME   = "default";

    /** SQLROUTER MANAGER */
    static public final String TR_PROPERTIES          = "router.properties";
    static public final String TR_RMI_PORT            = "router.rmi_port";
    static public final String TR_RMI_PORT_DEFAULT    = "10999";
    static public final String TR_RMI_DEFAULT_HOST    = "localhost";
    static public final String TR_SERVICE_NAME        = "router";
    static public final String TR_GW_PORT_DEFAULT     = "11999";

    /** POLICY MANAGER */
    static public final String PM_PROPERTIES          = "policymgr.properties";
    static public final String PM_RMI_PORT            = "policymgr.rmi_port";
    static public final String PM_NOTIFY_PORT         = "policymgr.notify_port";
    static public final String PM_SERVICE_NAME        = "cluster-policy-mgr";

    static public final String PM_RMI_DEFAULT_HOST    = "localhost";
    static public final String PM_RMI_PORT_DEFAULT    = "10011";
    static public final String PM_NOTIFY_PORT_DEFAULT = "10100";

}
