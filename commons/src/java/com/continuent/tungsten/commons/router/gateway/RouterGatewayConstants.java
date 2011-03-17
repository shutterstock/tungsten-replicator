
package com.continuent.tungsten.commons.router.gateway;

/**
 * Defines static strings used by the router gateway system for exchanging data
 * and commands over the network
 * 
 * @author <a href="mailto:gilles.rayrat@continuent.com">Gilles Rayrat</a>
 * @version 1.0
 */
public class RouterGatewayConstants
{
    // Commands
    public static final char   COMMAND_PING             = 'p';
    public static final char   COMMAND_UPDATE_DS        = 'u';
    public static final char   COMMAND_REMOVE_DS        = 'r';
    public static final char   COMMAND_STATUS           = 's';
    public static final char   COMMAND_STATISTICS       = 't';
    public static final char   COMMAND_CONFIGURE        = 'c';
    public static final char   COMMAND_DIAG             = 'd';
    public static final char   COMMAND_OFFLINE          = 'f';
    public static final char   COMMAND_ONLINE           = 'n';
    public static final char   COMMAND_DATASOURCE_MAP   = 'm';
    public static final char   COMMAND_QUIT             = 'q';

    // Property IDs inside Tungsten properties
    public static final String MANAGER_LIST             = "managers";
    public static final String ROUTER_ID                = "router.id";

    public static final String ERROR                    = "error";
    public static final String SERVICE_NAME             = "serviceName";
    public static final String ROUTER_NAME              = "memberName";
    public static final String METHOD_NAME              = "methodName";
    public static final String RESULT                   = "result";
    public static final String NOTIFICATION_PREFIX      = "notification.";
    public static final String NOTIFICATION_ARGS_PREFIX = "notificationArgs.";

}
