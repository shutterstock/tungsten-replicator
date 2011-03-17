
package com.continuent.tungsten.commons.directory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.exception.DirectoryException;
import com.continuent.tungsten.commons.exception.DirectoryNotFoundException;

public class DirectorySessionManager implements Serializable
{
    /**
     * 
     */
    private static final long                        serialVersionUID = 1L;

    private static Logger                            logger           = Logger
                                                                              .getLogger(DirectorySessionManager.class);

    // private Directory parent = null;

    protected Map<String, DirectorySession>          sessionsByID     = new HashMap<String, DirectorySession>();
    // member <connectionID, list<sessions>>
    protected Map<String, Map<Long, Vector<String>>> sessionsByDomain = new HashMap<String, Map<Long, Vector<String>>>();

    public synchronized DirectorySession connect(String domain, long handle,
            String sessionID, DirectoryType type)
            throws DirectoryNotFoundException
    {
        synchronized (sessionsByDomain)
        {
            Map<Long, Vector<String>> domainSessions = sessionsByDomain
                    .get(domain);

            if (domainSessions == null)
            {
                domainSessions = new HashMap<Long, Vector<String>>();
                sessionsByDomain.put(domain, domainSessions);
            }

            Vector<String> sessions = domainSessions.get(handle);
            if (sessions == null)
            {
                sessions = new Vector<String>();
                domainSessions.put(handle, sessions);
            }

            DirectorySession session = null;

            try
            {
                session = getSession(sessionID);
            }
            catch (DirectoryNotFoundException ignored)
            {

            }

            if (session == null)
            {
                session = newSession(sessionID, type);

                if (session == null)
                {
                    throw new DirectoryNotFoundException(
                            "newSession() returned null");
                }

                sessions.add(session.getSessionID());
                if (logger.isDebugEnabled())
                {
                    logger
                            .debug(String
                                    .format(
                                            "Created new session for domain %s %s for connectionID=%d",
                                            domain, sessionID, handle));
                }
                return session;
            }
            else
            {
                logger.warn(String.format(
                        "Directory session %s %s already exists", domain,
                        sessionID));
                return session;
            }

        }
    }

    public synchronized void disconnect(String sessionID)
    {
        try
        {
            removeSession(sessionID);
        }
        catch (DirectoryException d)
        {
            logger.warn(String.format(
                    "Attempt to remove non-existent session %s", sessionID), d);
        }
    }

    public synchronized void disconnect(String domain, long handle,
            String sessionID)
    {
        Map<Long, Vector<String>> domainSessions = sessionsByDomain.get(domain);

        if (domainSessions == null)
        {
            return;
        }

        Vector<String> sessions = domainSessions.get(handle);

        if (sessions == null)
            return;

        disconnect(sessionID);

        if (sessions.remove(sessionID))
        {
            if (logger.isDebugEnabled())
            {
                logger
                        .debug(String
                                .format(
                                        "Removed active session for domain %s %s for connection %d",
                                        domain, sessionID, handle));
            }
        }

        if (sessions.isEmpty())
        {
            if (logger.isDebugEnabled())
            {
                logger.debug(String.format(
                        "Clearing session storage storage for connection %d",
                        handle));
            }
            sessions.remove(handle);
        }

        if (domainSessions.isEmpty())
        {
            if (logger.isInfoEnabled())
                logger.info(String.format(
                        "Clearing session storage for domain %s", domain));
            sessionsByDomain.remove(domain);
        }
    }

    public synchronized void disconnectAll(String domain, long handle)
    {
        Map<Long, Vector<String>> domainSessions = sessionsByDomain.get(domain);

        if (domainSessions == null)
        {
            return;
        }
        Vector<String> sessions = domainSessions.get(handle);

        if (sessions == null)
            return;

        Vector<String> sessionsToRemove = new Vector<String>(sessions);
        for (String sessionID : sessionsToRemove)
        {
            disconnect(domain, handle, sessionID);
        }
    }

    public synchronized void disconnectAll(String domain)
    {
        Map<Long, Vector<String>> domainSessions = sessionsByDomain.get(domain);

        if (domainSessions == null)
        {
            return;
        }

        logger.info(String.format("Removing all sessions for domain '%s'",
                domain));

        synchronized (sessionsByDomain)
        {
            Vector<Long> handlesToRemove = new Vector<Long>(domainSessions
                    .keySet());
            for (Long handle : handlesToRemove)
            {
                disconnectAll(domain, handle);
            }
        }

        if (logger.isInfoEnabled())
            logger.info(String.format("Clearing session storage for domain %s",
                    domain));

        sessionsByDomain.remove(domain);
    }

    /**
     * Creates a new Directory session. This session becomes a context for a
     * given directory user and is used, primarily, to track the current working
     * resource. In most cases, this functionality will be used for interactive
     * applications where a user uses 'cd' to set a context.
     * 
     * @param sessionID
     * @return returns a string identifier for a new session
     */
    private DirectorySession newSession(String sessionID, DirectoryType type)
    {
        DirectorySession session = null;

        synchronized (sessionsByID)
        {
            session = new DirectorySession(this, sessionID, type);
            sessionsByID.put(session.getSessionID(), session);
        }

        return session;
    }

    /**
     * Returns an existing session or throws an exception.
     * 
     * @param sessionID
     * @return a context for a given session
     * @throws DirectoryNotFoundException
     */
    public synchronized DirectorySession getSession(String sessionID)
            throws DirectoryNotFoundException
    {
        DirectorySession session = null;

        if (sessionID == null)
        {
            throw new DirectoryNotFoundException(
                    "null sessionID is not allowed");
        }

        synchronized (sessionsByID)
        {
            session = sessionsByID.get(sessionID);

            if (session == null)
            {
                throw new DirectoryNotFoundException(String.format(
                        "Session %s not found", sessionID));
            }
        }

        session.setLastTimeAccessed(System.currentTimeMillis());
        return session;
    }

    /**
     * Removes an existing Directory session.
     * 
     * @param sessionID
     * @throws DirectoryException
     */
    public synchronized void removeSession(String sessionID)
            throws DirectoryException
    {
        DirectorySession session = null;

        synchronized (sessionsByID)
        {
            session = sessionsByID.get(sessionID);

            if (session == null)
            {
                // Session may have been removed by another thread.
                if (logger.isDebugEnabled())
                    logger.debug(String.format(
                            "Directory session '%s' not found", sessionID));
            }
            sessionsByID.remove(sessionID);
        }

    }

    public synchronized Map<String, DirectorySession> getSessionMap()
    {
        synchronized (sessionsByID)
        {
            return new HashMap<String, DirectorySession>(sessionsByID);
        }
    }

    public synchronized void mergeSessions(Directory source)
    {

        Map<String, DirectorySession> sessionMap = source.getSessionManager()
                .getSessionsByID();
        for (String sessionId : sessionMap.keySet())
        {
            if (sessionsByID.get(sessionId) == null)
            {
                sessionsByID.put(sessionId, sessionMap.get(sessionId));
            }
        }

        Map<Long, Vector<String>> domainSessionsLocal = sessionsByDomain
                .get(source.getMemberName());

        // Create a place to store these remote domain sessions
        if (domainSessionsLocal == null)
        {
            domainSessionsLocal = new HashMap<Long, Vector<String>>();
            sessionsByDomain.put(source.getMemberName(), domainSessionsLocal);
        }

        Map<String, Map<Long, Vector<String>>> sourceDomainMap = source
                .getSessionManager().getSessionsByDomain();
        Map<Long, Vector<String>> domainSessionsRemote = sourceDomainMap
                .get(source.getMemberName());

        for (Long handle : domainSessionsRemote.keySet())
        {
            Vector<String> localSessions = domainSessionsLocal.get(handle);

            if (localSessions == null)
            {
                localSessions = new Vector<String>();
                domainSessionsLocal.put(handle, localSessions);
            }

            Vector<String> remoteSessions = domainSessionsRemote.get(handle);
            for (String sessionID : remoteSessions)
            {
                if (!localSessions.contains(sessionID))
                {
                    logger.debug(String.format(
                            "Merged session for domain %s %s for handle=%d",
                            source.getMemberName(), sessionID, handle));
                    localSessions.add(sessionID);
                }
            }
        }

    }

    public Map<String, Map<Long, Vector<String>>> getSessionsByDomain()
    {
        return sessionsByDomain;
    }

    public Map<String, DirectorySession> getSessionsByID()
    {
        return sessionsByID;
    }

    public void setSessionsByID(Map<String, DirectorySession> sessionsByID)
    {
        this.sessionsByID = sessionsByID;
    }

}
