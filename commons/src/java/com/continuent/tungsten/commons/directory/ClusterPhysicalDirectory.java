
package com.continuent.tungsten.commons.directory;

import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.cluster.resource.ClusterManagerID;
import com.continuent.tungsten.commons.cluster.resource.ResourceType;
import com.continuent.tungsten.commons.cluster.resource.physical.Member;
import com.continuent.tungsten.commons.cluster.resource.physical.PhysicalResourceFactory;
import com.continuent.tungsten.commons.cluster.resource.physical.Process;
import com.continuent.tungsten.commons.cluster.resource.shared.ResourceConfiguration;
import com.continuent.tungsten.commons.config.TungstenConfiguration;
import com.continuent.tungsten.commons.config.TungstenConfigurationManager;
import com.continuent.tungsten.commons.config.TungstenProperties;
import com.continuent.tungsten.commons.exception.DirectoryException;
import com.continuent.tungsten.commons.exception.DirectoryNotFoundException;
import com.continuent.tungsten.commons.exception.ResourceException;
import com.continuent.tungsten.commons.exec.ProcessExecutor;
import com.continuent.tungsten.commons.utils.CLUtils;
import com.continuent.tungsten.commons.utils.Command;

public class ClusterPhysicalDirectory extends ClusterGenericDirectory
        implements
            DirectoryChangeNotifier
{
    /**
     *
     */
    private static Logger                   logger           = Logger.getLogger(ClusterPhysicalDirectory.class);
    private static ClusterPhysicalDirectory _instance        = null;

    /**
     *
     */
    private static final long               serialVersionUID = 1L;

    private ClusterPhysicalDirectory(ClusterManagerID managerID,
            DirectorySessionManager sessionManager) throws ResourceException,
            DirectoryNotFoundException
    {
        super(managerID, new PhysicalResourceFactory(), DirectoryType.PHYSICAL,
                sessionManager);

        ResourceNode cluster = getClusterNode(systemSessionID, siteName,
                clusterName);

        ResourceNode confFolder = getResourceFactory().addInstance(
                ResourceType.FOLDER, "conf", cluster);

        getResourceFactory().addInstance(ResourceType.FOLDER,
                ResourceType.DATASOURCE.toString().toLowerCase(), confFolder);

        getResourceFactory().addInstance(ResourceType.FOLDER,
                ResourceType.DATASERVICE.toString().toLowerCase(), confFolder);

        ResourceNode member = getResourceFactory().addInstance(
                ResourceType.MEMBER, memberName,
                getClusterNode(systemSessionID, siteName, clusterName));

        ((Member) member.getResource()).setProperty(Member.PORT,
                managerID.getPort());

        confFolder = getResourceFactory().addInstance(ResourceType.FOLDER,
                "conf", member);

        // Hold config of executable services (replicator, connector, etc.)
        ResourceNode serviceFolder = getResourceFactory().addInstance(
                ResourceType.FOLDER,
                ResourceType.SERVICE.toString().toLowerCase(), confFolder);

        ResourceNode procedureFolder = getResourceFactory().addInstance(
                ResourceType.FOLDER,
                ResourceType.EXTENSION.toString().toLowerCase(), confFolder);

        servicesFolders.put(memberName, serviceFolder);
        servicesFolders.put(memberName, procedureFolder);

    }

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

        if (command.equals(SERVICE))
        {
            String serviceSpec = (params != null ? params[0] : null);
            String serviceCmd = (params != null && params.length > 1
                    ? params[1]
                    : null);

            return executeExtension(ResourceType.SERVICE, serviceSpec,
                    "command", serviceCmd, null);
        }
        else if (command.equals(EXECUTE))
        {
            // Re-parse to ignore flags etc. We pass everthing
            // along to the extension as needed.
            cmd = parser.parse(commandLine, false);
            params = getParams(cmd.getTokens(), false);

            // Execute a procedure
            String type = (params != null ? params[0] : null);
            String extensionName = (params != null ? params[1] : null);
            String theCommand = (params != null && params.length > 2
                    ? params[2]
                    : null);

            if (type == null || extensionName == null || theCommand == null)
            {
                throw new DirectoryException(
                        String.format(
                                "Incorrectly formed command for execute:'%s'.",
                                command));
            }
            ResourceType extensionType = ResourceType.valueOf(type
                    .toUpperCase());

            String argList[] = null;
            if (params != null && params.length > 3)
            {
                argList = new String[params.length - 3];
                for (int i = 3; i < params.length; i++)
                    argList[i - 3] = params[i];
            }

            return executeExtension(extensionType, extensionName, KEY_COMMAND,
                    theCommand, argList);
        }
        else
        {
            return super.processCommand(sessionID, commandLine);
        }
    }

    public static ClusterPhysicalDirectory getInstance(
            ClusterManagerID managerID, DirectorySessionManager sessionManager)
            throws ResourceException, DirectoryNotFoundException
    {
        if (_instance == null)
        {
            _instance = new ClusterPhysicalDirectory(managerID, sessionManager);
        }

        return _instance;
    }

    /**
     * @param sessionID
     * @param siteName
     * @param clusterName
     * @param memberName
     * @throws Exception
     */
    public ResourceNode getMemberNode(String sessionID, String siteName,
            String clusterName, String memberName) throws Exception
    {
        String path = String.format("/%s/%s/%s", siteName, clusterName,
                memberName);

        return locate(sessionID, path);

    }

    /**
     * @param sessionID
     * @param siteName
     * @param clusterName
     * @param memberName
     * @throws Exception
     */
    public ResourceNode getClusterMemberConfNode(String sessionID,
            String siteName, String clusterName, String memberName,
            String extensionType) throws Exception
    {
        String path = String.format("/%s/%s/%s/conf/%s", siteName, clusterName,
                memberName, extensionType);

        return locate(sessionID, path);

    }

    public void addExtensionConfig(String sessionID, String siteName,
            String clusterName, String memberName, ResourceType extensionType,
            String extensionName, ResourceConfiguration config)
            throws Exception
    {

        logger.info(String.format("Adding %s definition: %s", extensionType,
                extensionName));

        ResourceNode extensionConfNode = getClusterMemberConfNode(sessionID,
                siteName, clusterName, memberName, extensionType.toString()
                        .toLowerCase());

        extensionConfNode.addChild(config);

    }

    /**
     * @param siteName TODO
     * @param clusterName
     * @param host
     * @param beanServiceName
     * @param port
     * @param component
     * @param managerName
     * @throws Exception
     */
    public ResourceNode getManagerNode(String sessionID, String siteName,
            String clusterName, String host, String beanServiceName, int port,
            String component, String managerName) throws Exception
    {
        // String path = String.format("%s/%s/%s:%d/%s", dataServiceName, host,
        // beanServiceName, port, managerName);

        String path = String.format("/%s/%s/%s/%s/%s", siteName, clusterName,
                host, beanServiceName, managerName);

        ResourceNode managerNode = null;

        try
        {
            managerNode = locate(sessionID, path);
        }
        catch (DirectoryNotFoundException d)
        {
            try
            {
                managerNode = create(sessionID, path, true);
            }
            catch (DirectoryException di)
            {
                throw new Exception(String.format(
                        "Unable to create directory entry, reason=%s",
                        di.getMessage()));
            }
        }

        return managerNode;

    }

    public synchronized ResourceNode createProcessNode(String sessionID,
            String siteName, String clusterName, String memberName,
            String componentName, String resourceManagerName, int port)
            throws DirectoryException
    {
        String path = String.format("/%s/%s/%s/%s", siteName, clusterName,
                memberName, resourceManagerName);

        if (memberName == null || port == 0)
        {
            String message = String
                    .format("Attempting to create an invalid process entry for component '%s'",
                            resourceManagerName);
            throw new DirectoryException(message);
        }

        try
        {
            ResourceNode processNode = create(sessionID, path, true);
            Process process = (Process) processNode.getResource();
            process.setMember(memberName);
            process.setPort(port);
            return processNode;
        }
        catch (Exception di)
        {
            throw new DirectoryException(String.format(
                    "Unable to create directory entry, reason=%s",
                    di.getMessage()), di);
        }
    }

    /**
     * Processes are found at: /<site>/<cluster>/<member>/<component>/<process>
     *
     * @param sessionID
     * @param siteName
     * @param clusterName
     * @param memberName
     */
    public ResourceNode getProcessNode(String sessionID, String siteName,
            String clusterName, String memberName, String processName)
    {
        String path = String.format("/%s/%s/%s/%s", siteName, clusterName,
                memberName, processName);

        ResourceNode processNode = null;

        try
        {
            processNode = locate(sessionID, path, getRootNode());
        }
        catch (DirectoryNotFoundException d)
        {
            return null;
        }

        return processNode;

    }

    /**
     * @throws Exception
     */
    public ResourceNode getOperationNode(String sessionID, String path,
            String processName) throws Exception
    {
        ResourceNode operationNode = null;

        try
        {
            operationNode = locate(sessionID, path, getCurrentNode(sessionID));
        }
        catch (DirectoryNotFoundException d)
        {
            if (processName == null)
                return null;

        }

        return operationNode;

    }

    /**
     * Extensions config are located in:
     * /<site>/<cluster>/<member>/<conf>/<extensionType>/<extensionName>
     *
     * @param sessionID
     * @param extensionType
     * @param extensionName
     * @throws Exception
     */
    ResourceNode getExtensionNode(String sessionID, String extensionType,
            String extensionName) throws Exception
    {
        String extensionPath = String.format("/%s/%s/%s/conf/%s/%s", siteName,
                clusterName, memberName, extensionType, extensionName);

        ResourceNode extensionNode = locate(getSystemSessionID(), extensionPath);

        if (extensionNode == null)
        {
            logger.error(String
                    .format("Could not find an extension of type %s named %s on member %s",
                            extensionType, extensionName, memberName));
        }

        return extensionNode;

    }

    /**
     * Executes a command on a service. The command must be defined in the
     * service configuration like this for eg.:<br/>
     * command.start=../../tungsten-replicator/bin/replicator start
     *
     * @param serviceSpec Name of the service.
     * @param serviceCmd Command name to execute on a service.
     * @return Stdout of the process have been executed.
     * @throws Exception If service node not found, or service does not support
     *             the requested command.
     */
    public String service(String serviceSpec, String serviceCmd)
            throws Exception
    {
        System.out.println(String.format("######## SERVICE %s %s", serviceSpec,
                serviceCmd));
        return executeExtension(ResourceType.SERVICE, serviceSpec, KEY_COMMAND,
                serviceCmd, null);
    }

    /**
     * Executes a command on a service. The command must be defined in the
     * service configuration like this for eg.:<br/>
     * command.start=../../tungsten-replicator/bin/replicator start
     *
     * @return Stdout of the process have been executed.
     * @throws Exception If service node not found, or service does not support
     *             the requested command.
     */
    public String procedure(String procedureSpec, String command)
            throws Exception
    {
        return executeExtension(ResourceType.EXTENSION, procedureSpec, "run",
                command, null);
    }

    /**
     * Execute an 'extension' by looking it up in the appropriate cluster
     * configuration directory. This is a generic facility that can execute any
     * previously configured command at the OS level and includes the ability to
     * execute concurrently across a set of nodes. The arguments passed in
     * determine where we will look for the 'extension' and what we will
     * execute.
     *
     * @param extensionType - the type of the extension. This determines the
     *            directory that is searched for the extension.
     * @param extensionName - the name of the .properties file to look for in
     *            the extension directory.
     * @param commandPrefix - the string to look for, in the extension
     *            properties file as an 'introducer' to a specific operation.
     *            This may vary depending on the function of the extension.
     * @param command - the command 'key' to use to find the command to execute.
     * @param args - any args required by the command. These are treated
     *            positionally
     */
    public String executeExtension(ResourceType extensionType,
            String extensionName, String commandPrefix, String command,
            String[] args) throws Exception
    {
        logger.debug(String.format("executeExtension(%s %s %s %s %s)",
                extensionType, extensionName, commandPrefix, command,
                args != null ? CLUtils.printArgs(args) : ""));

        if (extensionName == null)
        {
            return String
                    .format("You must provide the component name for this command");
        }

        String cmdPrefix = String.format("%s.", commandPrefix);
        String cmdProp = String.format("%s.%s", commandPrefix, command);

        ResourceNode extensionNode = getExtensionNode(getSystemSessionID(),
                extensionType.toString().toLowerCase(), extensionName);

        if (extensionNode == null)
        {
            return String
                    .format("Could not find an extension of type %s named %s on member %s",
                            extensionType, extensionName, memberName);
        }

        ResourceConfiguration config = (ResourceConfiguration) extensionNode
                .getResource();

        TungstenProperties tp = config.getProperties();

        if (command == null)
        {
            return String
                    .format("%s extension for %s takes one of the following commands:\n%s",
                            extensionType, extensionName,
                            tp.subset(cmdPrefix, true).keyNames());
        }

        String execPath = tp.getString(cmdProp);

        if (execPath == null)
        {
            return String.format(
                    "The %s extension for component %s does not support the command %s\n"
                            + "It takes one of the following commands\n:%s",
                    extensionType, extensionName, command,
                    tp.subset(cmdPrefix, true).keyNames());
        }
        else
        {
            ArrayList<String> execList = new ArrayList<String>();

            for (String arg : execPath.split(" +"))
            {
                execList.add(arg);
            }

            // original string from which args were taken
            // may have included quoted strings but the
            // basic command parser in cctrl doesn't leave them
            // alone. Since I don't want to mess with that parser
            // right now and break something else, just
            // reconstruct the quoted strings here.
            if (args != null)
            {
                boolean inQuotedString = false;
                String quotedString = "";
                for (String arg : args)
                {
                    if (arg.startsWith("\""))
                    {
                        inQuotedString = true;
                        quotedString = "";
                        quotedString = quotedString + arg + " ";
                        continue;
                    }
                    else if (inQuotedString)
                    {
                        if (arg.endsWith("\""))
                        {
                            inQuotedString = false;
                            quotedString = quotedString + arg;
                            execList.add(quotedString);
                        }
                        else
                        {
                            quotedString = quotedString + arg + " ";
                        }
                        continue;
                    }
                    else
                    {
                        execList.add(arg);
                    }
                }
            }
            ProcessExecutor processExecutor = new ProcessExecutor();
            processExecutor.setWorkDirectory(null); // Uses current working dir.
            processExecutor.setCommands(execList.toArray(new String[execList
                    .size()]));
            processExecutor.run();

            String stdOut = processExecutor.getStdout();
            String stdErr = processExecutor.getStderr();

            return stdErr + stdOut;
        }
    }

    /**
     * @see com.continuent.tungsten.commons.directory.Directory#ls(java.lang.String,
     *      java.lang.String, boolean)
     */
    public static TungstenConfiguration toConfiguration(
            ClusterPhysicalDirectory dir) throws DirectoryNotFoundException
    {
        TungstenConfigurationManager configMgr = new TungstenConfigurationManager(
                dir.getRootNode().getResource());

        getConfig(dir.getRootNode(), configMgr, configMgr.getRoot());

        return configMgr.getRoot();
    }

    /**
     * {@inheritDoc}
     *
     * @see com.continuent.tungsten.commons.directory.Directory#getEntries(com.continuent.tungsten.commons.directory.ResourceNode,
     *      java.util.List, boolean)
     */
    private static void getConfig(ResourceNode nodeToSearch,
            TungstenConfigurationManager configMgr, TungstenConfiguration config)
    {
        for (ResourceNode entry : nodeToSearch.getChildren().values())
        {
            if (entry.getType() == ResourceType.FOLDER)
                continue;
            else if (entry.getType() == ResourceType.RESOURCE_MANAGER)
                continue;
            else if (entry.getType() == ResourceType.ROOT)
                continue;

            TungstenConfiguration childConfig = configMgr.addChild(config,
                    entry.getResource());

            getConfig(entry, configMgr, childConfig);
        }
    }

}
