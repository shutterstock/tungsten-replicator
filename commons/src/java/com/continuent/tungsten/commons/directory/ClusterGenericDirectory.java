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
 * Contributor(s): Linas Virbalas
 */

package com.continuent.tungsten.commons.directory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.cluster.resource.ClusterManagerID;
import com.continuent.tungsten.commons.cluster.resource.Resource;
import com.continuent.tungsten.commons.cluster.resource.ResourceState;
import com.continuent.tungsten.commons.cluster.resource.ResourceType;
import com.continuent.tungsten.commons.cluster.resource.logical.DataService;
import com.continuent.tungsten.commons.cluster.resource.notification.ClusterResourceNotification;
import com.continuent.tungsten.commons.cluster.resource.notification.DirectoryNotification;
import com.continuent.tungsten.commons.cluster.resource.physical.DataSource;
import com.continuent.tungsten.commons.cluster.resource.physical.Operation;
import com.continuent.tungsten.commons.cluster.resource.shared.ResourceConfiguration;
import com.continuent.tungsten.commons.cluster.resource.shared.Root;
import com.continuent.tungsten.commons.config.TungstenProperties;
import com.continuent.tungsten.commons.exception.DirectoryException;
import com.continuent.tungsten.commons.exception.DirectoryNotFoundException;
import com.continuent.tungsten.commons.exception.ResourceException;
import com.continuent.tungsten.commons.patterns.notification.NotificationGroupMember;
import com.continuent.tungsten.commons.patterns.notification.ResourceNotificationException;
import com.continuent.tungsten.commons.patterns.notification.ResourceNotificationListener;
import com.continuent.tungsten.commons.patterns.notification.ResourceNotifier;
import com.continuent.tungsten.commons.utils.Command;
import com.continuent.tungsten.commons.utils.CommandLineParser;

/**
 * This class provides a means to organize cluster resources in an intuitive,
 * hierarchical form. It allows us to, effectively, extend the resources that
 * can be referred to, directly, in the Tungsten ResourceManager.
 * 
 * @author <a href="mailto:edward.archibald@continuent.com">Ed Archibald</a>
 * @version 1.0
 */
public class ClusterGenericDirectory extends ResourceTree
        implements
            Serializable,
            ResourceNotifier,
            Directory
{
    /**
     *
     */
    private static Logger                                       logger               = Logger.getLogger(Directory.class);

    private static final long                                   serialVersionUID     = 1L;

    protected final static String                               DEFAULT_CLUSTER_NAME = "default";

    protected ClusterManagerID                                  managerID            = null;
    protected String                                            siteName             = null;
    protected String                                            clusterName          = null;
    protected String                                            memberName           = null;
    protected boolean                                           recursive            = false;
    protected boolean                                           absolute             = false;
    protected boolean                                           detailed             = false;
    protected boolean                                           createParents        = false;

    protected transient ArrayList<ResourceNotificationListener> listeners            = new ArrayList<ResourceNotificationListener>();

    protected String                                            systemSessionID      = null;

    // Services folders for each node, with the key being the node name.
    protected Map<String, ResourceNode>                         servicesFolders      = new TreeMap<String, ResourceNode>();

    // This gets incremented for any changes to this instance.
    protected Long                                              currentVersion       = 01L;
    protected Long                                              lastMergedVersion    = 0L;
    protected boolean                                           merging              = false;

    private static DirectorySessionManager                      sessionManager       = null;

    private ResourceNode                                        rootNode             = null;

    protected static CommandLineParser                          parser               = new CommandLineParser(
                                                                                             null);

    protected transient DirectoryResourceFactory                resourceFactory      = null;

    protected DirectoryType                                     type                 = null;

    /**
     * Creates an instance of Directory with some base-level resources.
     * 
     * @param managerID TODO
     * @throws ResourceException
     */
    protected ClusterGenericDirectory(ClusterManagerID managerID,
            DirectoryResourceFactory factory, DirectoryType type,
            DirectorySessionManager sessionManager) throws ResourceException,
            DirectoryNotFoundException
    {

        super("tungsten");

        this.siteName = managerID.getSiteName();
        this.clusterName = managerID.getClusterName();
        this.memberName = managerID.getMemberName();
        this.resourceFactory = factory;
        this.type = type;
        this.sessionManager = sessionManager;

        rootNode = new ResourceNode(new Root());
        setRootNode(rootNode);

        String sessionID = UUID.randomUUID().toString();

        systemSessionID = connect(memberName, 0L, sessionID);
        systemSessionID = sessionID;

        ResourceNode site = getResourceFactory().addInstance(ResourceType.SITE,
                siteName, getRootNode());

        getResourceFactory().addInstance(ResourceType.CLUSTER, clusterName,
                site);

    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#connect(java.lang.String,
     *      long, java.lang.String)
     */
    public String connect(String domain, long handle, String sessionID)
            throws DirectoryNotFoundException
    {
        DirectorySession session = sessionManager.connect(domain, handle,
                sessionID, type);
        if (session.getCurrentNode(type) == null)
        {
            session.setCurrentNode(type, this.getRootNode());
        }

        return session.getSessionID();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#disconnect(java.lang.String,
     *      long, java.lang.String)
     */
    public synchronized void disconnect(String domain, long handle,
            String sessionID)
    {
        sessionManager.disconnect(domain, handle, sessionID);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#disconnectAll(java.lang.String,
     *      long)
     */
    public synchronized void disconnectAll(String domain, long handle)
    {
        sessionManager.disconnectAll(domain, handle);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#disconnectAll(java.lang.String)
     */
    public synchronized void disconnectAll(String domain)
    {
        sessionManager.disconnectAll(domain);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#getSession(java.lang.String)
     */
    public DirectorySession getSession(String sessionID)
            throws DirectoryNotFoundException
    {
        return sessionManager.getSession(sessionID);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#processCommand(java.lang.String,
     *      java.lang.String)
     */
    public String processCommand(String sessionID, String commandLine)
            throws Exception
    {
        Command cmd = parser.parse(commandLine);

        if (cmd == null)
        {
            throw new Exception("Cannot execute null command");
        }

        String command = cmd.getTokens()[0];
        String[] params = getParams(cmd.getTokens(), true);

        String result = null;

        if (command.equals(CD))
        {
            String path = (params != null ? params[0] : null);
            try
            {
                cd(sessionID, path);
            }
            catch (Exception e)
            {
                return e.getMessage();
            }
        }
        else if (command.equals(LIST))
        {
            String path = (params != null ? params[0] : null);
            ResourceNode startNode = getStartNode(sessionID, path);
            List<ResourceNode> entries = ls(sessionID, path, cmd.isRecursive());
            result = formatEntries(entries, startNode, cmd.isLong(),
                    cmd.isAbsolute());
        }
        else if (command.equals(CREATE))
        {
            String path = (params != null ? params[0] : null);
            create(sessionID, path, cmd.includeParents());
        }
        else if (command.equals(RM))
        {
            String path = (params != null ? params[0] : null);
            rm(sessionID, path);
        }
        else if (command.equals(CP))
        {
            String source = (params != null ? params[0] : null);
            String destination = (source != null && params.length == 2)
                    ? params[1]
                    : null;
            cp(sessionID, source, destination);
        }
        else if (command.equals(PWD))
        {
            result = pwd(sessionID);
        }
        else if (command.equals(CHKEXEC))
        {
            String path = (params != null ? params[0] : null);
            return String.format("%s", isExecutable(sessionID, path));
        }
        else if (command.equals(WHICH))
        {
            String path = (params != null ? params[0] : null);
            return String.format("%s", which(sessionID, path));
        }
        else if (command.equals(CONNECT))
        {
            throw new Exception(
                    "This interface to CONNECT is no longer supported");
        }
        else
        {
            if (isExecutable(sessionID, command))
            {
                throw new DirectoryException(String.format(
                        "Cannot execute '%s' in this context.", command));
            }
        }

        return result;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#disconnect(java.lang.String)
     */
    public synchronized void disconnect(String sessionID)
            throws DirectoryException
    {
        sessionManager.removeSession(sessionID);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#which(java.lang.String,
     *      java.lang.String)
     */
    public String which(String sessionID, String path)
            throws DirectoryNotFoundException
    {
        ResourceNode node = null;

        try
        {
            node = locate(sessionID, path);
        }
        catch (DirectoryNotFoundException d)
        {
            return null;
        }

        String thePath = formatPath(getAbsolutePath(getRootNode(), node, true),
                true);

        int paramStart = thePath.indexOf("(");

        if (paramStart != -1)
        {
            return thePath.substring(0, paramStart);
        }

        return thePath;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#isExecutable(java.lang.String,
     *      java.lang.String)
     */
    public boolean isExecutable(String sessionID, String path)
            throws DirectoryNotFoundException
    {
        ResourceNode execNode = locate(sessionID, path);

        if (execNode.getResource() == null)
        {
            throw new DirectoryNotFoundException(String.format(
                    "Cannot execute '%s'", path));
        }

        if (execNode.getResource() instanceof Operation)
        {
            return true;
        }

        return false;

    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#getServiceFolder(java.lang.String)
     */
    public ResourceNode getServiceFolder(String hostName)
    {
        return servicesFolders.get(hostName);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#locateRelative(java.lang.String,
     *      int)
     */
    public ResourceNode locateRelative(String sessionID, int levels)
            throws DirectoryException, DirectoryNotFoundException
    {
        return locateRelative(sessionID, levels, getCurrentNode(sessionID));
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#locateRelative(java.lang.String,
     *      int, com.continuent.tungsten.commons.directory.ResourceNode)
     */
    public synchronized ResourceNode locateRelative(String sessionID,
            int levels, ResourceNode startNode) throws DirectoryException,
            DirectoryNotFoundException
    {
        if (levels == 0)
            return getCurrentNode(sessionID);

        ResourceNode foundNode = null;
        ResourceNode nodeToSearch = startNode;

        for (int level = 0; level < levels; level++)
        {
            if ((foundNode = nodeToSearch.getParent()) != null)
            {
                nodeToSearch = nodeToSearch.getParent();

            }
            else
            {
                throw new DirectoryException(String.format(
                        "No parent element found for '%s'",
                        formatPath(
                                getAbsolutePath(nodeToSearch, nodeToSearch,
                                        true), true)));
            }
        }

        return foundNode;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#locate(java.lang.String,
     *      java.lang.String)
     */
    public ResourceNode locate(String sessionID, String path)
            throws DirectoryNotFoundException
    {
        return locate(sessionID, path, getCurrentNode(sessionID));
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#locate(java.lang.String,
     *      java.lang.String,
     *      com.continuent.tungsten.commons.directory.ResourceNode)
     */
    public synchronized ResourceNode locate(String sessionID, String path,
            ResourceNode startNode) throws DirectoryNotFoundException
    {

        if (path == null)
        {
            return getCurrentNode(sessionID);
        }
        else if (path.startsWith(ROOT_ELEMENT)
                && path.length() > ROOT_ELEMENT.length())
        {
            startNode = getRootNode();
            path = path.substring(ROOT_ELEMENT.length());
        }

        if (path.equals(CURRENT_ELEMENT))
            return getCurrentNode(sessionID);
        else if (path.equals(ROOT_ELEMENT))
            return getRootNode();

        ResourceNode foundNode = null;
        ResourceNode nodeToSearch = startNode;

        String pathElements[] = path.split(PATH_SEPARATOR);

        if (pathElements.length == 0)
        {
            return getRootNode();
        }

        for (String element : pathElements)
        {
            // Just skip blanks which result from extra slashes
            if (element.length() == 0)
                continue;

            if (element.equals(PARENT_ELEMENT))
            {
                if ((foundNode = nodeToSearch.getParent()) != null)
                {
                    nodeToSearch = nodeToSearch.getParent();

                }
                else
                {
                    throw new DirectoryNotFoundException(String.format(
                            "element '%s' not found", path));
                }
            }
            else
            {

                Map<String, ResourceNode> children = nodeToSearch.getChildren();

                // For the present, we just handle the wildcard as if it
                // means to return 'any' element rather than all elements.
                if (element.equals(ANY_ELEMENT))
                {
                    if (nodeToSearch.getType() == ResourceType.CLUSTER)
                    {
                        foundNode = children.get(memberName);
                    }
                    else if (children.size() > 0)
                    {
                        foundNode = getFirst(children);
                    }
                    else
                    {
                        throw new DirectoryNotFoundException(
                                String.format(
                                        "the element '%s' of path '%s' resolves to more than one element",
                                        element, path));
                    }
                }
                else
                {

                    foundNode = children.get(element);
                }

                if (foundNode == null)
                {
                    throw new DirectoryNotFoundException(String.format(
                            "element '%s' not found in path '%s'",
                            element,
                            formatPath(
                                    getAbsolutePath(getRootNode(),
                                            nodeToSearch, true), true)));
                }

                nodeToSearch = foundNode;
            }
        }

        return foundNode;
    }

    /**
     * @param map
     * @throws DirectoryNotFoundException
     */
    private ResourceNode getFirst(Map<String, ResourceNode> map)
            throws DirectoryNotFoundException
    {
        for (ResourceNode node : map.values())
            return node;

        return null;
    }

    /**
     * @param path
     * @throws DirectoryNotFoundException
     */
    private ResourceNode getStartNode(String sessionID, String path)
            throws DirectoryNotFoundException
    {
        ResourceNode startNode = null;

        startNode = locate(sessionID, path);

        return startNode;

    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#ls(java.lang.String,
     *      java.lang.String, boolean)
     */
    public synchronized List<ResourceNode> ls(String sessionID, String path,
            boolean doRecurse) throws DirectoryNotFoundException
    {
        ResourceNode startNode = null;
        startNode = getStartNode(sessionID, path);
        List<ResourceNode> entries = new LinkedList<ResourceNode>();

        if (!startNode.isContainer())
        {
            entries.add(startNode);
            return entries;
        }
        getEntries(startNode, entries, doRecurse);

        return entries;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#cp(java.lang.String,
     *      java.lang.String, java.lang.String)
     */
    public synchronized void cp(String sessionID, String sourcePath,
            String destinationPath) throws DirectoryException,
            DirectoryNotFoundException
    {

        ResourceNode destination = null;
        ResourceNode source = null;

        if (sourcePath == null || destinationPath == null)
        {
            throw new DirectoryException(
                    "cp: <source> <destination>: missing operand");
        }

        try
        {
            source = locate(sessionID, sourcePath);
        }
        catch (DirectoryNotFoundException c)
        {
            throw new DirectoryException(String.format(
                    "cp: the source element '%s' does not exist", sourcePath));

        }

        try
        {
            destination = locate(sessionID, destinationPath);

            // If the destination path exists and is not a container,
            // or it is of the same type, don't allow the copy.
            if (!destination.getResource().isContainer()
                    || destination.getResource().getType() == source
                            .getResource().getType())
            {

                throw new DirectoryException(String.format(
                        "cp: cannot copy over an existing element '%s'",
                        destinationPath));
            }
        }
        catch (DirectoryNotFoundException c)
        {
            // We have more checking to do.....
        }

        if (destination == null)
        {
            // If the destination path refers only to the new name,
            // the destination is the current node.
            if (lastElement(destinationPath).equals(
                    elementPrefix(destinationPath)))
            {
                destination = getCurrentNode(sessionID);
            }
            else
            {
                destination = locate(sessionID, elementPrefix(destinationPath));
            }
        }

        Resource copy = null;

        try
        {
            copy = resourceFactory.copyInstance(source,
                    lastElement(destinationPath), destination);

        }
        catch (ResourceException c)
        {
            throw new DirectoryException(String.format(
                    "unable to create a copy of '%s', reason='%s'", sourcePath,
                    c.getMessage()));
        }

        destination.addChild(copy);
        flush();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#getEntries(com.continuent.tungsten.commons.directory.ResourceNode,
     *      java.util.List, boolean)
     */
    public synchronized List<ResourceNode> getEntries(
            ResourceNode nodeToSearch, List<ResourceNode> entries,
            boolean doRecurse)
    {
        for (ResourceNode entry : nodeToSearch.getChildren().values())
        {
            entries.add(entry);

            if (doRecurse)
            {
                getEntries(entry, entries, doRecurse);
            }

        }

        return entries;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#cd(java.lang.String,
     *      java.lang.String)
     */
    public synchronized ResourceNode cd(String sessionID, String path)
            throws DirectoryNotFoundException, DirectoryException
    {
        ResourceNode node = null;

        if (path == null)
        {
            node = getRootNode();
        }
        else
        {
            node = locate(sessionID, path);
        }

        if (!node.isContainer())
        {
            throw new DirectoryException(String.format(
                    "the element referenced by '%s' is not a container", path));
        }

        getSession(sessionID).setCurrentNode(type, node);

        flush();

        return node;

    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#create(java.lang.String,
     *      java.lang.String, boolean)
     */
    public synchronized ResourceNode create(String sessionID, String name,
            boolean createParents) throws DirectoryException,
            DirectoryNotFoundException
    {
        return create(sessionID, name, getCurrentNode(sessionID), createParents);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#create(java.lang.String,
     *      java.lang.String,
     *      com.continuent.tungsten.commons.directory.ResourceNode)
     */
    public synchronized ResourceNode create(String sessionID, String name,
            ResourceNode parent) throws DirectoryException
    {
        ResourceNode newElement = null;

        if (name == null)
        {

            throw new DirectoryException(
                    String.format("mkdir: missing name to create"));
        }

        try
        {

            newElement = resourceFactory.addInstance(parent.getResource()
                    .getChildType(), name, parent);

        }
        catch (ResourceException r)
        {
            throw new DirectoryException(r.getMessage());
        }

        flush();
        return newElement;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#create(java.lang.String,
     *      java.lang.String,
     *      com.continuent.tungsten.commons.directory.ResourceNode, boolean)
     */
    public synchronized ResourceNode create(String sessionID, String path,
            ResourceNode startNode, boolean createParents)
            throws DirectoryException
    {

        String pathElements[] = path.split(PATH_SEPARATOR);

        if (pathElements.length == 0)
        {
            throw new DirectoryException(
                    "missing operand: usage: create <path>");
        }

        ResourceNode foundNode = startNode;
        ResourceNode createdNode = null;

        // First look at all of the elements up to the final element.
        // If createParents is set, create missing elements
        for (int i = 0; i < pathElements.length - 1; i++)
        {
            if (pathElements[i].length() == 0)
                continue;

            try
            {
                foundNode = locate(sessionID, pathElements[i], startNode);
            }
            catch (DirectoryNotFoundException d)
            {
                if (createParents)
                {
                    foundNode = create(sessionID, pathElements[i], startNode);
                }
                else
                {
                    String message = String.format(
                            "element '%s' does not exist in path '%s'",
                            pathElements[i], path);
                    logger.warn(message);

                    throw new DirectoryException(message);
                }
            }

            startNode = foundNode;
        }

        createdNode = create(sessionID, pathElements[pathElements.length - 1],
                startNode);

        flush();
        return createdNode;

    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#rm(java.lang.String,
     *      java.lang.String)
     */
    public synchronized void rm(String sessionID, String path)
            throws DirectoryNotFoundException, DirectoryException
    {
        ResourceNode nodeToRemove = null;

        nodeToRemove = getStartNode(sessionID, path);
        rm(sessionID, nodeToRemove);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#rm(java.lang.String,
     *      com.continuent.tungsten.commons.directory.ResourceNode)
     */
    public synchronized void rm(String sessionID, ResourceNode node)
    {
        node.getParent().removeChild(node.getKey());
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#pwd(java.lang.String)
     */
    public String pwd(String sessionID) throws DirectoryNotFoundException
    {
        return formatPath(getCwd(sessionID), true);
    }

    public String formatPath(Directory directory, String sessionID,
            ResourceNode node) throws DirectoryNotFoundException
    {
        return directory.formatPath(
                directory.getAbsolutePath(directory, sessionID, node), true);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#formatPath(java.util.List,
     *      boolean)
     */
    public String formatPath(List<ResourceNode> pathElements,
            boolean reverseOrder)
    {
        StringBuilder builder = new StringBuilder();

        int elementCount = 0;

        for (ResourceNode element : pathElements)
        {
            String elementString = null;

            if (element.getResource() instanceof Operation)
            {
                elementString = (element.getResource() != null ? element
                        .getResource().toString() : element.toString());
            }
            else
            {
                // elementString = element.getKey();
                // if (element.isContainer() && element != rootNode)
                elementString = (element.getResource() != null ? element
                        .getResource().toString() : element.toString());
                // elementString += "/";
            }

            if (reverseOrder)
            {
                builder.insert(0, elementString);
            }
            else
            {
                if (element.getParent() == rootNode)
                {
                    builder.append("/");
                }
                builder.append(elementString);
                if (elementCount++ > 0)
                {
                    builder.append("/");
                }
            }
        }

        return builder.toString();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#formatEntries(java.util.List,
     *      com.continuent.tungsten.commons.directory.ResourceNode, boolean,
     *      boolean)
     */
    public String formatEntries(List<ResourceNode> entries,
            ResourceNode startNode, boolean detailed, boolean absolute)
    {
        StringBuilder builder = new StringBuilder();
        TreeMap<String, String> sortedEntries = new TreeMap<String, String>();

        for (ResourceNode entry : entries)
        {
            if (detailed)
            {
                Resource res = entry.getResource();
                String detail = (res != null ? res.describe(detailed) : entry
                        .toString());

                sortedEntries.put(detail, detail);
            }
            else
            {
                boolean includeStartNode = (startNode == rootNode
                        ? true
                        : false) || absolute;

                if (absolute)
                {
                    startNode = rootNode;
                }
                String formattedEntry = formatPath(
                        getAbsolutePath(startNode, entry, includeStartNode),
                        true);
                sortedEntries.put(formattedEntry, formattedEntry);
            }

        }

        for (String entry : sortedEntries.values())
        {
            builder.append(entry).append("\n");
        }

        return builder.toString();
    }

    public List<ResourceNode> getAbsolutePath(ResourceNode fromNode,
            ResourceNode toNode, boolean includeFromNode)
    {
        List<ResourceNode> absolutePath = new LinkedList<ResourceNode>();

        absolutePath.add(toNode);

        ResourceNode parent = toNode.getParent();

        while (parent != null)
        {
            if (parent == fromNode)
            {
                if (includeFromNode)
                    absolutePath.add(parent);

                break;
            }

            absolutePath.add(parent);

            parent = parent.getParent();
        }

        return absolutePath;
    }

    public List<ResourceNode> getAbsolutePath(Directory directory,
            String sessionID, ResourceNode node)
            throws DirectoryNotFoundException
    {
        return getAbsolutePath(directory.getCurrentNode(sessionID), node, true);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#getCurrentNode(java.lang.String)
     */
    public ResourceNode getCurrentNode(String sessionID)
            throws DirectoryNotFoundException
    {
        ResourceNode currentNode = null;
        DirectorySession session = getSession(sessionID);

        currentNode = session.getCurrentNode(type);

        if (currentNode == null)
        {
            currentNode = getRootNode();
            session.setCurrentNode(type, currentNode);
        }

        return currentNode;

    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#setCurrentNode(java.lang.String,
     *      com.continuent.tungsten.commons.directory.ResourceNode)
     */
    public void setCurrentNode(String sessionID, ResourceNode currentNode)
            throws DirectoryNotFoundException
    {
        getSession(sessionID).setCurrentNode(type, currentNode);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#getCwd(java.lang.String)
     */
    public List<ResourceNode> getCwd(String sessionID)
            throws DirectoryNotFoundException
    {
        DirectorySession session = getSession(sessionID);
        ResourceNode currentNode = session.getCurrentNode(type);
        if (currentNode == null)
        {
            currentNode = getRootNode();
            session.setCurrentNode(type, currentNode);
        }
        return getAbsolutePath(getRootNode(), currentNode, true);
    }

    /**
     * This method merges the current directory with another directory such that
     * the current directory has all elements, by name, which exist in both
     * directories and any elements that have identical names remain in the
     * current directory.
     * 
     * @param source
     * @param destination
     * @return the count of the number of nodes merged
     * @throws DirectoryException
     */
    public synchronized Directory merge(Directory source, Directory destination)
            throws DirectoryException
    {
        merging = true;
        ResourceNode sourceNode = source.getRootNode();
        ResourceNode destinationNode = destination.getRootNode();
        Integer nodesMerged = new Integer(0);

        _merge(sourceNode, destinationNode, nodesMerged);

        // While we are at it, make sure that we add any sessionsByID we do
        // not already know about.
        destination.getSessionManager().mergeSessions(source);

        lastMergedVersion = currentVersion;
        merging = false;
        return this;
    }

    private void _merge(ResourceNode sourceNode, ResourceNode destinationNode,
            Integer nodesMerged)
    {
        Map<String, ResourceNode> sourceChildren = sourceNode.getChildren();
        Map<String, ResourceNode> destinationChildren = destinationNode
                .getChildren();

        for (ResourceNode sourceChild : sourceChildren.values())
        {
            ResourceNode destinationChild = destinationChildren.get(sourceChild
                    .getKey());

            if (destinationChild == null)
            {
                destinationChild = destinationNode.addChild(sourceChild
                        .getResource());
                nodesMerged++;
            }

            _merge(sourceChild, destinationChild, nodesMerged);
        }
    }

    /**
     * paramStart
     * 
     * @param args
     */
    protected String[] getParams(String[] args, boolean parseFlags)
            throws DirectoryException
    {
        if (args == null || args.length == 1)
            return null;

        int paramIndex = 1;

        if (args[paramIndex].startsWith("-") && parseFlags)
        {
            byte chars[] = args[paramIndex].getBytes();
            // Skip the flag
            for (int i = 1; i < chars.length; i++)
            {
                if (chars[i] == FLAG_LONG)
                    detailed = true;
                else if (chars[i] == FLAG_RECURSIVE)
                    recursive = true;
                else if (chars[i] == FLAG_PARENTS)
                    createParents = true;
                else if (chars[i] == FLAG_ABSOLUTE)
                    absolute = true;
                else
                    throw new DirectoryException(String.format(
                            "Unrecognized option '%c'", chars[i]));
            }

            paramIndex++;

        }

        if (args.length == paramIndex)
            return null;

        String[] params = new String[args.length - paramIndex];
        int countParams = args.length - paramIndex;

        for (int i = 0; i < countParams; i++)
        {
            params[i] = args[paramIndex++];
        }

        return params;

    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#toString()
     */
    @Override
    public String toString()
    {
        String ret = null;

        try
        {
            List<ResourceNode> listResult = ls(systemSessionID, ROOT_ELEMENT,
                    true);
            Collections.sort(listResult, new ResourceNodeComparator());

            ret = formatEntries(ls(systemSessionID, ROOT_ELEMENT, true), null,
                    detailed, false);
        }
        catch (Exception e)
        {
            ret = "NOT AVAILABLE BECAUSE OF EXCEPTION=" + e;
        }

        return ret;
    }

    class ResourceNodeComparator
            implements
                Comparator<ResourceNode>,
                Serializable
    {
        /**
         *
         */
        private static final long serialVersionUID = 1L;

        public int compare(ResourceNode o1, ResourceNode o2)
        {
            return o1.getKey().compareTo(o2.getKey());
        }
    }

    private String lastElement(String path)
    {
        return path.substring((path.lastIndexOf(PATH_SEPARATOR) + 1));
    }

    private String elementPrefix(String path)
    {
        int separatorLocation = path.lastIndexOf(PATH_SEPARATOR);

        if (separatorLocation == -1)
        {
            return null;
        }

        return path.substring(0, separatorLocation);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#exists(java.lang.String,
     *      java.lang.String)
     */
    public boolean exists(String sessionID, String path)
    {
        ResourceNode nodeToVerify = null;

        try
        {
            nodeToVerify = locate(sessionID, path);
        }
        catch (DirectoryNotFoundException c)
        {
            return false;
        }

        return ((nodeToVerify == null ? false : true));

    }

    public static boolean isDirectoryCommand(String command)
    {
        for (String cmd : directoryCommands)
        {
            if (command.compareToIgnoreCase(cmd) == 0)
                return true;
        }

        return false;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#getSystemSessionID()
     */
    public String getSystemSessionID()
    {
        return systemSessionID;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#setSystemSessionID(java.lang.String)
     */
    public void setSystemSessionID(String systemSessionID)
    {
        this.systemSessionID = systemSessionID;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#addListener(com.continuent.tungsten.commons.patterns.notification.ResourceNotificationListener)
     */
    public void addListener(ResourceNotificationListener listener)
    {
        if (listener == null)
        {
            logger.warn("Attempting to add a null listener");
            return;
        }
        listeners.add(listener);
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#notifyListeners(com.continuent.tungsten.commons.cluster.resource.notification.ClusterResourceNotification)
     */
    public void notifyListeners(ClusterResourceNotification notification)
            throws ResourceNotificationException
    {
        for (ResourceNotificationListener listener : listeners)
        {
            listener.notify(notification);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#run()
     */
    public void run()
    {
        // TODO Auto-generated method stub
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#flush()
     */
    public synchronized void flush()
    {
        if (merging)
            return;

        currentVersion++;

        TungstenProperties resourceProps = new TungstenProperties();
        resourceProps.setObject("directory", this);

        DirectoryNotification notification = new DirectoryNotification(
                clusterName, memberName, getClass().getSimpleName(), getClass()
                        .getSimpleName(), ResourceState.MODIFIED, resourceProps);

        try
        {
            notifyListeners(notification);
        }
        catch (ResourceNotificationException r)
        {
            logger.error(
                    String.format(
                            "Could not send directory synchronization request, reason=%s",
                            r.getLocalizedMessage()), r);

        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#getVersion()
     */
    public long getVersion()
    {
        return currentVersion;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#getNotificationGroupMembers()
     */
    public Map<String, NotificationGroupMember> getNotificationGroupMembers()
    {
        return new LinkedHashMap<String, NotificationGroupMember>();
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#getClusterName()
     */
    public String getClusterName()
    {
        return clusterName;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#getSiteName()
     */
    public String getSiteName()
    {
        return siteName;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#setClusterName(java.lang.String)
     */
    public void setClusterName(String clusterName)
    {
        this.clusterName = clusterName;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#getMemberName()
     */
    public String getMemberName()
    {
        return memberName;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#setMemberName(java.lang.String)
     */
    public void setMemberName(String memberName)
    {
        this.memberName = memberName;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#prepare()
     */
    public void prepare() throws Exception
    {
        // TODO Auto-generated method stub

    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#getResourceFactory()
     */
    public DirectoryResourceFactory getResourceFactory()
    {
        return resourceFactory;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#setResourceFactory(com.continuent.tungsten.commons.directory.DirectoryResourceFactory)
     */
    public void setResourceFactory(DirectoryResourceFactory resourceFactory)
    {
        this.resourceFactory = resourceFactory;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.commons.directory.Directory#getSessionManager()
     */
    public DirectorySessionManager getSessionManager()
    {
        return sessionManager;
    }

    /********************************************************************************
     * Helper methods common to logical and physical clusters.
     ********************************************************************************/

    /**
     * @param sessionID
     * @param siteName
     * @param clusterName
     * @throws Exception
     */
    public ResourceNode getClusterConfNode(String sessionID, String siteName,
            String clusterName) throws DirectoryNotFoundException
    {
        String path = String.format("/%s/%s/conf", siteName, clusterName);

        return locate(sessionID, path);

    }

    /**
     * @param sessionID
     * @param siteName
     * @param clusterName
     * @throws Exception
     */
    public ResourceNode getClusterNode(String sessionID, String siteName,
            String clusterName) throws DirectoryNotFoundException
    {
        String path = String.format("/%s/%s", siteName, clusterName);

        return locate(sessionID, path);

    }

    /**
     * TODO: getClusterResourceConfNode definition.
     * 
     * @param sessionID
     * @param siteName
     * @param clusterName
     * @param resourceType
     * @throws Exception
     */
    public ResourceNode getClusterResourceConfNode(String sessionID,
            String siteName, String clusterName, ResourceType resourceType)
            throws Exception
    {
        String path = String.format("/%s/%s/conf/%s", siteName, clusterName,
                resourceType.toString().toLowerCase());

        return locate(sessionID, path);

    }

    /**
     * TODO: getDataServiceConfNode definition.
     * 
     * @param sessionID
     * @param siteName
     * @param clusterName
     * @param dataServiceName
     * @throws Exception
     */
    public ResourceNode getDataServiceConfNode(String sessionID,
            String siteName, String clusterName, String dataServiceName)
            throws Exception
    {
        String path = String.format("/%s/%s/conf/%s/%s", siteName, clusterName,
                ResourceType.DATASERVICE.toString().toLowerCase(),
                dataServiceName);
        try
        {
            return locate(sessionID, path);
        }
        catch (DirectoryNotFoundException s)
        {
            return create(sessionID, path, true);
        }

    }

    public void addDataService(String sessionID, String clusterName,
            String dataServiceName, TungstenProperties serviceProperties)
            throws Exception
    {
        logger.info(String.format("Adding DATASERVICE '%s:%s//%s'", siteName,
                clusterName, dataServiceName));

        ResourceNode dataServiceConfNode = getDataServiceConfNode(sessionID,
                siteName, clusterName, dataServiceName);

        ResourceConfiguration config = new ResourceConfiguration("global",
                serviceProperties);

        config.setExecutable(false);

        dataServiceConfNode.addChild(config);

    }

    public void addDataService(String sessionID, DataService dataService)
            throws Exception
    {
        logger.info(String.format("Adding DATASERVICE '%s'",
                dataService.getFqn()));

        ResourceNode dataServiceConfNode = getDataServiceConfNode(sessionID,
                siteName, clusterName, dataService.getName());

        ResourceConfiguration config = new ResourceConfiguration("global",
                dataService.getProperties());

        dataServiceConfNode.addChild(config);
        config.setExecutable(false);

        Map<String, DataSource> dsMap = dataService.getAllDataSources();
        for (String dsName : dsMap.keySet())
        {
            config = new ResourceConfiguration(dsName, dsMap.get(dsName)
                    .toProperties());

            dataServiceConfNode.addChild(config);
            config.setExecutable(false);
        }
    }

    public TungstenProperties getDataServiceConfiguration(String sessionID,
            String clusterName, String dataServiceName) throws Exception
    {
        ResourceNode dataServiceConfNode = getDataServiceConfNode(sessionID,
                siteName, clusterName, dataServiceName);

        ResourceNode configNode = dataServiceConfNode.getChildren().get(
                "global");

        if (configNode != null)
        {
            return ((ResourceConfiguration) configNode.getResource())
                    .getProperties();

        }

        return null;
    }

    public void removeDataService(String sessionID, String clusterName,
            String dataServiceName) throws Exception
    {
        logger.info(String.format("Removing DATASERVICE '%s:%s//%s'", siteName,
                clusterName, dataServiceName));

        ResourceNode dataServiceConfNode = getDataServiceConfNode(sessionID,
                siteName, clusterName, dataServiceName);

        dataServiceConfNode.getParent().removeChild(
                dataServiceConfNode.getKey());
    }

    public void addDataSource(String sessionID, String clusterName,
            String dsName, TungstenProperties dataSourceProperties)
            throws Exception
    {
        logger.info(String.format("Adding DATASOURCE '%s'", dsName));

        ResourceNode dsConfNode = getClusterResourceConfNode(sessionID,
                siteName, clusterName, ResourceType.DATASOURCE);

        ResourceConfiguration config = new ResourceConfiguration(
                dataSourceProperties.getString(DataSource.NAME),
                dataSourceProperties);

        config.setExecutable(false);

        dsConfNode.addChild(config);

    }

    public void removeDataSource(String sessionID, String clusterName,
            String dsName) throws Exception
    {
        logger.info(String.format("Removing DATASOURCE '%s'", dsName));

        ResourceNode dsConfNode = getClusterResourceConfNode(sessionID,
                siteName, clusterName, ResourceType.DATASOURCE);

        dsConfNode.removeChild(dsName);

    }

    /**
     * Returns the managerID value.
     * 
     * @return Returns the managerID.
     */
    public ClusterManagerID getManagerID()
    {
        return managerID;
    }

    /**
     * Sets the managerID value.
     * 
     * @param managerID The managerID to set.
     */
    public void setManagerID(ClusterManagerID managerID)
    {
        this.managerID = managerID;
    }

    public DirectoryType getDirectoryType()
    {
        return type;
    }
}
