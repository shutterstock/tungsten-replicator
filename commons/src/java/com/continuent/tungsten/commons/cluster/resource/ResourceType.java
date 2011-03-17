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
 * Initial developer(s): Ed Archibald
 * Contributor(s): 
 */

package com.continuent.tungsten.commons.cluster.resource;

import java.io.Serializable;

public enum ResourceType implements Serializable
{

    ROOT, /* the root resource of any resource tree */

    /*
     * Physical resources
     */
    SITE, /* a site */
    CLUSTER, /* a cluster */
    DATASOURCE, /* a sql-router datasource */
    MEMBER, /* a manager of the cluster */
    PROCESS, /* a JVM/MBean server */
    RESOURCE_MANAGER, /*
                       * a class that is exported as a JMX MBean for a specific
                       * component
                       */
    OPERATION, /* an operation exported by a JMX MBean */
    DATASERVER, /* a database server */

    /*
     * Logical resources
     */

    EVENT, DATASERVICE, COMPOSITE_DATASERVICE, COMPOSITE_DATASOURCE, DATASHARD, DATASHARDFACET, SERVICE, EXTENSION, FOLDER, QUEUE, CONFIGURATION,

    HOST,

    SQLROUTER, /* a sql-router component */
    REPLICATOR, /* a replicator component */
    REPLICATION_SERVICE, /* A single replication service */

    DIRECTORY, /* a Directory instance */
    DIRECTORY_SESSION, UNDEFINED,

    NONE, ANY
    /* any resource */
}
