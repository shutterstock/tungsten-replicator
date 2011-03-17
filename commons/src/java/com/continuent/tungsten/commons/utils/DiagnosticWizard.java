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
 * Initial developer(s): Linas Virbalas
 * Contributor(s): 
 */

package com.continuent.tungsten.commons.utils;

import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

import org.apache.log4j.Logger;

import sun.management.ManagementFactory;

/**
 * Methods to help diagnosing and debugging current JAVA process.
 * 
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 * @version 1.0
 */
public class DiagnosticWizard
{
    private static Logger logger = Logger.getLogger(DiagnosticWizard.class);

    /**
     * Returns all threads' ThreadInfo objects. Note that some array elements
     * might contain null elements if threads died while enumerating them.
     * 
     * @return All available ThreadInfo objects for this process.
     * @throws Exception
     */
    public static ThreadInfo[] getAllThreadInfos() throws Exception
    {
        final ThreadMXBean thbean = ManagementFactory.getThreadMXBean();
        final long[] ids = thbean.getAllThreadIds();

        ThreadInfo[] infos;
        infos = thbean.getThreadInfo(ids, Integer.MAX_VALUE);

        return infos;
    }

    /**
     * Dumps a list of threads with their stack traces into the log.
     */
    public static String dumpThreadStack() throws Exception
    {
        StringBuffer out = new StringBuffer();
        out.append("# Threads' stack traces\n");
        ThreadInfo[] threadInfos = getAllThreadInfos();
        for (ThreadInfo info : threadInfos)
        {
            if (info != null)
            {
                out.append("Thread (" + info.getThreadId() + "): "
                        + info.getThreadName() + "\n");
                StackTraceElement[] stack = info.getStackTrace();
                for (StackTraceElement s : stack)
                {
                    out.append("  " + s.toString() + "\n");
                }
            }
            out.append("\n");
        }
        return out.toString();
    }
    
    /**
     * The effect is the same as calling diag(null).
     * 
     * @return Diagnostic information without component specific details.
     */
    public static String diag() throws Exception
    {
        return diag(null);
    }

    /**
     * Dumps various debugging information (thread list, thread stack trace,
     * internal data structures, etc.) to the log.
     * 
     * @throws Exception
     * @param componentDiag Component specific diagnostic information returning
     *            callback which data is included in the return value of this
     *            method.
     */
    public static String diag(DiagnosticWizardPlugin componentDiag)
            throws Exception
    {
        StringBuffer out = new StringBuffer("\n");

        out.append("########################\n");
        out.append("# Start of diag output #\n");
        out.append("########################\n");

        if (componentDiag != null)
            out.append(componentDiag.diag() + "\n");
        
        out.append(dumpThreadStack());

        out.append("########################\n");
        out.append("#  End of diag output  #\n");
        out.append("########################\n");

        logger.info(out);

        return out.toString();
    }
}
