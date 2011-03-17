
package com.continuent.tungsten.commons.cluster.resource;

import java.io.Serializable;

public enum ResourceState implements Serializable
{
    ONLINE, OFFLINE, SYNCHRONIZING, JOINING, SUSPECT, STOPPED, UNKNOWN, SHUNNED, CONSISTENT, 
    MODIFIED, FAILED, BACKUP, UNREACHABLE, EXTENSION

}
