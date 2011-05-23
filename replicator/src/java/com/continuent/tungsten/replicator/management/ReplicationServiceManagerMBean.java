/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2010 Continuent Inc.
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
 * Initial developer(s): Robert Hodges
 * Contributor(s):
 */

package com.continuent.tungsten.replicator.management;

import java.util.List;
import java.util.Map;

import com.continuent.tungsten.commons.jmx.DynamicMBeanHelper;

/**
 * Management interface for main replicator control class.
 *
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public interface ReplicationServiceManagerMBean
{
    /**
     * Lists currently defined replicators and whether they are running or not.
     */
    public List<Map<String, String>> services() throws Exception;

    /**
     * Returns true if the MBean is alive. Used to test liveness of connections.
     */
    public boolean isAlive();

    /**
     * Returns status information.
     *
     * @throws Exception
     */
    public Map<String, String> status() throws Exception;

    /**
     * Starts a replication service.
     *
     * @param name Name of the replicator service
     * @return True if replicator service exists and was started
     * @throws Exception Thrown if service start-up fails
     */
    public boolean startService(String name) throws Exception;

    /**
     * Stops a replication service.
     *
     * @param name Name of the replicator service
     * @return True if replicator service exists and was stopped
     * @throws Exception Thrown if service stop fails
     */
    public boolean stopService(String name) throws Exception;

    /**
     * Resets a replication service.
     *
     * @param name Name of the replicator service
     * @return Map of strings that indicate actions taken.
     * @throws Exception Thrown if service stop fails
     */
    public Map<String, String> resetService(String name) throws Exception;

    /**
     * Returns a list of properties that have the status for each of the current
     * services.
     */
    public Map<String, String> replicatorStatus(String name) throws Exception;

    /**
     * Returns a map of status properties for all current replicators
     *
     * @throws Exception
     */
    public Map<String, String> getStatus() throws Exception;

    /**
     * Stops all replication services and exits the process cleanly.
     *
     * @throws Exception Thrown if service stop fails
     */
    public void stop() throws Exception;

    /**
     * Terminates the replicator process immediately without clean-up. This
     * command should be used only if stop does not work or for testing.
     */
    public void kill() throws Exception;

    /**
     * Returns a helper that supplies MBean metadata.
     */
    public abstract DynamicMBeanHelper createHelper() throws Exception;
}
