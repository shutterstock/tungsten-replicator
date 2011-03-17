
package com.continuent.tungsten.commons.directory;

import java.util.List;
import java.util.Map;

import com.continuent.tungsten.commons.cluster.resource.notification.ClusterResourceNotification;
import com.continuent.tungsten.commons.exception.DirectoryException;
import com.continuent.tungsten.commons.exception.DirectoryNotFoundException;
import com.continuent.tungsten.commons.patterns.notification.NotificationGroupMember;
import com.continuent.tungsten.commons.patterns.notification.ResourceNotificationException;
import com.continuent.tungsten.commons.patterns.notification.ResourceNotificationListener;

public interface Directory
{
    public final static String   ROOT_ELEMENT            = "/";
    public final static String   CURRENT_ELEMENT         = ".";
    public final static String   PARENT_ELEMENT          = "..";
    public final static String   ANY_ELEMENT             = "*";
    public final static String   AMPERSAND               = "&";
    public final static String   PATH_SEPARATOR          = "/";
    public static final String   DIRECTORY               = "directory";
    public static final String   EXECUTE                 = "execute";
    public static final String   LIST                    = "ls";
    public static final String   CD                      = "cd";
    public static final String   CP                      = "cp";
    public static final String   RM                      = "rm";
    public static final String   CREATE                  = "create";
    public static final String   PWD                     = "pwd";
    public static final String   CHKEXEC                 = "chkexec";
    public static final String   WHICH                   = "which";
    public static final String   CONNECT                 = "connect";
    public static final String   DISCONNECT              = "disconnect";
    public static final String   DISCONNECT_ALL          = "disconnectAll";
    public static final String   GET_CHILDREN            = "getChildren";
    public static final String   VIEW                    = "view";
    public static final String   MERGE                   = "merge";
    public static final String   SERVICE                 = "service";
    public static final String   EXTENSION               = "extension";
    public static final char     FLAG_LONG               = 'l';
    public static final char     FLAG_RECURSIVE          = 'R';
    public static final char     FLAG_ABSOLUTE           = 'A';
    public static final char     FLAG_PARENTS            = 'p';
    public static final String   KEY_COMMAND             = "command";
    public static final String   GETDATASOURCE           = "getDataSource";
    public static final String   GETDATASERVICE          = "getDataService";
    public static final String   GETGLOBALDATASERVICE    = "getGlobalDataService";
    public static final String   REMOVEGLOBALDATASERVICE = "removeGlobalDataService";

    public static final String[] directoryCommands       = {LIST, CD, CP, RM,
            CREATE, PWD, EXECUTE, CHKEXEC, WHICH, CONNECT, MERGE, SERVICE,
            DISCONNECT, DISCONNECT_ALL, VIEW, GET_CHILDREN, GETDATASERVICE,
            GETGLOBALDATASERVICE, REMOVEGLOBALDATASERVICE};

    public abstract String connect(String domain, long handle, String sessionID)
            throws DirectoryNotFoundException;

    public abstract void disconnect(String domain, long handle, String sessionID);

    public abstract void disconnectAll(String domain, long handle);

    public abstract void disconnectAll(String domain);

    /**
     * Returns an existing session or throws an exception.
     * 
     * @param sessionID
     * @return a context for a given session
     * @throws DirectoryNotFoundException
     */
    public abstract DirectorySession getSession(String sessionID)
            throws DirectoryNotFoundException;

    /**
     * This is the main interface for the directory with respect to text-based
     * operations.
     * 
     * @param sessionID
     * @return the output for a given command
     * @throws Exception
     */
    public abstract String processCommand(String sessionID, String commandLine)
            throws Exception;

    public abstract void disconnect(String sessionID) throws DirectoryException;

    /**
     * This command returns the full path to a given resource, as long as the
     * resource exists
     * 
     * @param sessionID
     * @param path
     * @return a string representing the full, absolute path for a given path
     *         element
     * @throws DirectoryNotFoundException
     */
    public abstract String which(String sessionID, String path)
            throws DirectoryNotFoundException;

    /**
     * A primitive that indicates whether or not the resource associated with a
     * node is executable.
     * 
     * @param sessionID
     * @param path
     * @return true if the path is executable, otherwise false
     * @throws DirectoryNotFoundException
     */
    public abstract boolean isExecutable(String sessionID, String path)
            throws DirectoryNotFoundException;

    /**
     * Returns services folder which is created upon construction of this
     * directory and which holds services to execute commands upon.
     */
    public abstract ResourceNode getServiceFolder(String hostName);

    /**
     * Traverse back the indicated number of levels, starting at the current
     * node, and return the node found there.
     * 
     * @param levels
     * @return the resource that is relative to the current node, proceeding via
     *         the parents, by the indicated levels
     */
    public abstract ResourceNode locateRelative(String sessionID, int levels)
            throws DirectoryException, DirectoryNotFoundException;

    /**
     * Traverse back the indicated number of levels, starting at the current
     * node, and return the node found there.
     * 
     * @param levels
     * @return the resource that is relative to the current node, proceeding via
     *         the parents, by the indicated levels, starting at the indicated
     *         node.
     */
    public abstract ResourceNode locateRelative(String sessionID, int levels,
            ResourceNode startNode) throws DirectoryException,
            DirectoryNotFoundException;

    /**
     * Locates a given resource node, given a path. This method takes into
     * account the 'current' working node.
     * 
     * @param sessionID
     * @param path
     * @return the resource specified by path
     * @throws DirectoryNotFoundException if the resource cannot be found
     */
    public abstract ResourceNode locate(String sessionID, String path)
            throws DirectoryNotFoundException;

    /**
     * Locates a given resource give a path and a starting node.
     * 
     * @param path
     * @param startNode
     * @return the resource specified by path
     * @throws DirectoryNotFoundException if the resource cannot be found
     */
    public abstract ResourceNode locate(String sessionID, String path,
            ResourceNode startNode) throws DirectoryNotFoundException;

    /**
     * Returns a list of resource nodes according to the path passed in.
     * 
     * @param path
     * @return a list of resources as indicated by the path.
     * @throws DirectoryNotFoundException if the specified resources cannot be
     *             found
     */
    public abstract List<ResourceNode> ls(String sessionID, String path,
            boolean doRecurse) throws DirectoryNotFoundException;

    /**
     * Makes a copy of a specific resource node in the destination.
     * 
     * @param sourcePath
     * @param destinationPath
     * @throws DirectoryException
     * @throws DirectoryNotFoundException
     */
    public abstract void cp(String sessionID, String sourcePath,
            String destinationPath) throws DirectoryException,
            DirectoryNotFoundException;

    /**
     * @param nodeToSearch
     * @param entries
     * @return all of the entries in the nodes below the node to search
     */
    public abstract List<ResourceNode> getEntries(ResourceNode nodeToSearch,
            List<ResourceNode> entries, boolean doRecurse);

    /**
     * @param path
     * @throws DirectoryNotFoundException
     */
    public abstract ResourceNode cd(String sessionID, String path)
            throws DirectoryNotFoundException, DirectoryException;

    public abstract ResourceNode create(String sessionID, String name,
            boolean createParents) throws DirectoryException,
            DirectoryNotFoundException;

    /**
     * @param name
     * @param parent
     * @return the resource that was created
     * @throws DirectoryException if the parent cannot be found
     */
    public abstract ResourceNode create(String sessionID, String name,
            ResourceNode parent) throws DirectoryException;

    /**
     * @param path
     * @param startNode
     * @param createParents
     * @return the resource that was created
     * @throws DirectoryException if the startNode cannot be found
     */
    public abstract ResourceNode create(String sessionID, String path,
            ResourceNode startNode, boolean createParents)
            throws DirectoryException;

    /**
     * @param sessionID
     * @param path
     * @throws DirectoryNotFoundException
     */
    public abstract void rm(String sessionID, String path)
            throws DirectoryNotFoundException, DirectoryException;

    public abstract void rm(String sessionID, ResourceNode node);

    /**
     * @param sessionID
     * @return a string representing the working directory/resourceNode
     * @throws DirectoryNotFoundException
     */
    public abstract String pwd(String sessionID)
            throws DirectoryNotFoundException;

    /**
     * @param pathElements
     * @param reverseOrder
     * @return a formatted representation of the path
     */
    public abstract String formatPath(List<ResourceNode> pathElements,
            boolean reverseOrder);

    /**
     * @param entries
     * @param detailed
     * @return string representation of the entries
     */
    public abstract String formatEntries(List<ResourceNode> entries,
            ResourceNode startNode, boolean detailed, boolean absolute);

    public abstract ResourceNode getCurrentNode(String sessionID)
            throws DirectoryNotFoundException;

    public abstract void setCurrentNode(String sessionID,
            ResourceNode currentNode) throws DirectoryNotFoundException;

    public abstract List<ResourceNode> getCwd(String sessionID)
            throws DirectoryNotFoundException;

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
    public abstract Directory merge(Directory source, Directory destination)
            throws DirectoryException;

    public abstract String toString();

    public abstract boolean exists(String sessionID, String path);

    /**
     * @return the systemSessionID
     */
    public abstract String getSystemSessionID();

    /**
     * @param systemSessionID the systemSessionID to set
     */
    public abstract void setSystemSessionID(String systemSessionID);

    public abstract void addListener(ResourceNotificationListener listener);

    public abstract void notifyListeners(
            ClusterResourceNotification notification)
            throws ResourceNotificationException;

    public abstract void run();

    public abstract void flush();

    public abstract long getVersion();

    public abstract Map<String, NotificationGroupMember> getNotificationGroupMembers();

    public List<ResourceNode> getAbsolutePath(ResourceNode fromNode,
            ResourceNode toNode, boolean includeFromNode);

    public abstract List<ResourceNode> getAbsolutePath(Directory directory,
            String sessionID, ResourceNode node)
            throws DirectoryNotFoundException;

    public abstract String getClusterName();
    
    public abstract String getSiteName();

    public abstract void setClusterName(String clusterName);

    public abstract String getMemberName();

    public abstract void setMemberName(String memberName);

    public abstract void prepare() throws Exception;

    public abstract DirectoryResourceFactory getResourceFactory();

    public abstract ResourceNode getRootNode();

    public abstract void setResourceFactory(
            DirectoryResourceFactory resourceFactory);

    public abstract DirectorySessionManager getSessionManager();
    
    public abstract DirectoryType getDirectoryType();

}