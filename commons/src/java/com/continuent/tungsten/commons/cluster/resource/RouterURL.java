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
 * Initial developer(s): Edward Archibald
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.commons.cluster.resource;

import java.sql.SQLException;
import java.util.Properties;
import java.util.UUID;

import org.apache.log4j.Logger;

/**
 * Implements a simple parser for SQLRouter URLs. It identifies and strips out
 * t-router properties
 * 
 * @author <a href="mailto:edward.archibald@continuent.com">Edward Archibald</a>
 * @version 1.0
 */
public class RouterURL
{
    private static Logger       logger                    = Logger
                                                                  .getLogger(RouterURL.class);

    // Keys for specific connection properties
    private static final String KEY_MAX_APPLIED_LATENCY   = "maxAppliedLatency";
    private static final String KEY_QOS                   = "qos";
    private static final String KEY_SESSION_ID            = "sessionId";

    private static final String CONNECTION                = "CONNECTION";
    private static final String DATABASE                  = "DATABASE";
    private static final String USER                      = "USER";

    // Parsed URL data
    private String              dataServiceName           = "UNDEFINED";
    private String              dbname                    = "UNDEFINED";
    private Properties          props                     = new Properties();
    private QualityOfService    qos                       = QualityOfService.RW_STRICT;
    public static double        MAX_APPLIED_LATENCY_UNDEF = -2;
    private double              maxAppliedLatency         = MAX_APPLIED_LATENCY_UNDEF;
    private String              sessionId                 = null;
    private boolean             autoSession               = false;

    // Parsing information.
    private String              url;
    private int                 pos                       = 0;

    /**
     * Creates a parsed URL object.
     * 
     * @param url SQL router URL
     * @param info Properties for URL
     * @throws SQLException Thrown if the URL cannot be parsed
     */
    public RouterURL(String url, Properties info) throws SQLException
    {
        parseUrl(url, info, true);
    }

    public RouterURL(String url, Properties info, boolean fullUrl)
            throws SQLException
    {
        parseUrl(url, info, fullUrl);
    }

    public String getService()
    {
        return dataServiceName;
    }

    public String getDbname()
    {
        return dbname;
    }

    public Properties getProps()
    {
        return props;
    }

    public QualityOfService getQos()
    {
        return qos;
    }

    /**
     * Parse the driver URL and extract the properties.
     * 
     * @param url the URL to parse
     * @param info any existing properties already loaded in a
     *            <code>Properties</code> object Valid driver URL is:
     *            jdbc:t-router//service[[/<datastore-name>]
     *            ?qos={RW_STRICT|RW_RELAXED|RO_STRICT|RO_RELAXED}] The default
     *            qos (Quality of service) is RW_STRICT unless specified or
     *            unless overridden by the service configuration.
     */
    private void parseUrl(String url, Properties info, boolean fullUrl)
            throws SQLException
    {
        // Set up data.
        this.url = url;
        this.pos = 0;

        // Add input properties if supplied.
        if (info != null)
            props.putAll(info);

        if (fullUrl)
        {
            // Skip jdbc protocol.
            if (!"jdbc".equalsIgnoreCase(nextToken()))
            {
                throw new SQLException("URL lacks 'jdbc' protocol: " + url);
            }

            // Skip sub-protocol.
            if (!"t-router".equalsIgnoreCase(nextToken()))
            {
                throw new SQLException("URL lacks 't-router' sub-protocol: "
                        + url);
            }

            // Get the service name.
            dataServiceName = nextToken();

            // Get the database name.
            dbname = nextToken();
        }

        // Get properties.
        String key;
        while ((key = nextToken()) != null)
        {
            String value = nextToken();
            props.setProperty(key, value);
        }

        // If QOS is among the properties, remove it and set the QOS
        // explicitly.
        String qosValue = (String) props.remove(KEY_QOS);
        if (qosValue != null)
        {
            try
            {
                qos = QualityOfService
                        .valueOf(QualityOfService.class, qosValue);

            }
            catch (IllegalArgumentException i)
            {
                StringBuilder msg = new StringBuilder();
                msg.append("Invalid value '").append(qosValue).append(
                        "' passed for the quality of service.").append(
                        " Valid values are: ");
                for (QualityOfService q : QualityOfService.values())
                {
                    msg.append(q.toString()).append(' ');
                }
                throw new SQLException(msg.toString());
            }
        }
        // Same for max latency
        String maxAppliedLatencyValue = (String) props
                .remove(KEY_MAX_APPLIED_LATENCY);
        if (maxAppliedLatencyValue != null)
        {
            try
            {
                this.maxAppliedLatency = Double
                        .parseDouble(maxAppliedLatencyValue);
            }
            catch (NumberFormatException nfe)
            {
                logger
                        .warn("URL option maxAppliedLatency value "
                                + maxAppliedLatencyValue
                                + " could not be parsed correctly - defaulting to -2 (undef)");
                this.maxAppliedLatency = MAX_APPLIED_LATENCY_UNDEF;
            }
        }

        sessionId = (String) props.remove(KEY_SESSION_ID);

        if (sessionId != null)
        {
            if (sessionId.equals(CONNECTION))
            {
                sessionId = UUID.randomUUID().toString();
                autoSession = true;
            }
            else if (sessionId.equals(DATABASE))
            {
                if (dbname != null)
                {
                    sessionId = dbname;
                }
                else
                {
                    throw new SQLException(
                            "You must supply a database name to use the DATABASE based sessionId");
                }
            }
            else if (sessionId.equals(USER))
            {
                String user = props.getProperty("user");
                if (user != null)
                {
                    sessionId = user;
                }
                else
                {
                    throw new SQLException(
                            "You must supply a user name for the URL property 'user' to use the USER based sessionId");
                }
            }
        }

        if (logger.isDebugEnabled())
        {
            logger.debug("Parsed t-router URL: service="
                    + dataServiceName
                    + " dbname="
                    + dbname
                    + " QOS="
                    + qos
                    + " maxAppliedLatency="
                    + ((this.maxAppliedLatency == MAX_APPLIED_LATENCY_UNDEF)
                            ? "<undef>"
                            : this.maxAppliedLatency) + " props=" + props);
        }
    }

    /**
     * Extract the next lexical token from the URL.
     * 
     * @param url The URL being parsed
     * @param pos The current position in the URL string.
     * @param token The buffer containing the extracted token.
     * @return The updated position as an <code>int</code>.
     */
    private String nextToken()
    {
        StringBuffer token = new StringBuffer();

        while (pos < url.length())
        {
            char ch = url.charAt(pos++);

            if (ch == ':' || ch == ';' || ch == '?' || ch == '&' || ch == '=')
            {
                break;
            }

            if (ch == '/')
            {
                if (pos < url.length() && url.charAt(pos) == '/')
                {
                    pos++;
                    continue;
                }
                else
                {
                    break;
                }
            }
            token.append(ch);
        }

        if (token.length() == 0)
            return null;
        else
            return token.toString();
    }

    /**
     * Returns the dataServiceName value.
     * 
     * @return Returns the dataServiceName.
     */
    public String getDataServiceName()
    {
        return dataServiceName;
    }

    /**
     * Sets the dataServiceName value.
     * 
     * @param dataServiceName The dataServiceName to set.
     */
    public void setDataServiceName(String dataServiceName)
    {
        this.dataServiceName = dataServiceName;
    }

    /**
     * Returns the url value.
     * 
     * @return Returns the url.
     */
    public String getUrl()
    {
        return url;
    }

    /**
     * Sets the url value.
     * 
     * @param url The url to set.
     */
    public void setUrl(String url)
    {
        this.url = url;
    }

    /**
     * Sets the dbname value.
     * 
     * @param dbname The dbname to set.
     */
    public void setDbname(String dbname)
    {
        this.dbname = dbname;
    }

    /**
     * Sets the qos value.
     * 
     * @param qos The qos to set.
     */
    public void setQos(QualityOfService qos)
    {
        this.qos = qos;
    }

    public double getMaxAppliedLatency()
    {
        return maxAppliedLatency;
    }

    public void setMaxAppliedLatency(double maxAppliedLatency)
    {
        this.maxAppliedLatency = maxAppliedLatency;
    }

    public String getSessionId()
    {
        return sessionId;
    }

    public void setSessionId(String sessionId)
    {
        this.sessionId = sessionId;
    }

    public String toString()
    {
        return getUrl() + props;
    }

    public boolean isAutoSession()
    {
        return autoSession;
    }

    public void setAutoSession(boolean autoSession)
    {
        this.autoSession = autoSession;
    }
}