/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2009 Continuent Inc.
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

package com.continuent.tungsten.replicator.backup;

import java.io.File;
import java.io.InputStream;
import java.util.List;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.exec.ProcessExecutor;

/**
 * Implements a simple helper to execute operating system processes with
 * consistent logging and handling of errors.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class ProcessHelper
{
    private static Logger logger = Logger.getLogger(ProcessHelper.class);

    // Properties
    private String        cmdPrefix;

    // Root command prefix and various generated commands in array form.
    String[]              prefix;

    List<String>          outputLines;

    /**
     * Create a new helper.
     */
    public ProcessHelper()
    {
    }

    public String getCmdPrefix()
    {
        return cmdPrefix;
    }

    public void setCmdPrefix(String cmdPrefix)
    {
        this.cmdPrefix = cmdPrefix;
    }

    /**
     * Configure the helper. This must called before executing any commands.
     */
    public void configure()
    {
        // Ensure prefix is correctly set.
        if (cmdPrefix != null)
        {
            prefix = cmdPrefix.split(" ");
        }
    }

    /**
     * Execute a command accompanied by a description. A command prefix such as
     * 'sudo' is added automatically.
     * 
     * @param description Text description of the command
     * @param baseCommand Command as an array but without 'sudo' or other prefix
     * @param stdin InputStream for process stdin
     * @param stdout File to receive stdout
     * @param stderr File to receive stderr
     * @param stdoutAppend If true append to stdout file
     * @param stderrAppend If true append to stderr file
     * @throws BackupException If the command fails
     */
    public void exec(String description, String[] baseCommand,
            InputStream stdin, File stdout, File stderr, boolean stdoutAppend,
            boolean stderrAppend) throws BackupException
    {
        // Generate a properly prefixed command.
        String[] cmd = makeCmd(baseCommand);

        // Run process.
        logger.info(description + ": " + arrayToCommand(cmd));
        ProcessExecutor pe = new ProcessExecutor();
        pe.setCommands(cmd);
        if (stdin != null)
            pe.setStdin(stdin);
        if (stdout != null)
        {
            pe.setStdOut(stdout);
            pe.setStdOutAppend(stdoutAppend);
        }
        if (stderr != null)
        {
            pe.setStdErr(stderr);
            pe.setStdErrAppend(stderrAppend);
        }
        pe.run();

        // Check for status...
        if (pe.isSuccessful())
        {
            if (logger.isDebugEnabled())
            {
                logger.debug(this.processInfo(pe));
            }
            if (stdout == null)
            {
                outputLines = pe.getStdoutByLine();
            }
        }
        else
        {
            logger.error("Operating system command failed");
            logger.info(this.processInfo(pe));
            throw new BackupException(
                    "Operating system command failed; check log for details");
        }
    }

    /**
     * Convenience method to execute command without providing input or output.
     * 
     * @param description Text description of the command
     * @param baseCommand Command as an array but without 'sudo' or other prefix
     * @throws BackupException If the command fails
     */
    public void exec(String description, String[] baseCommand)
            throws BackupException
    {
        exec(description, baseCommand, null, null, null, false, false);
    }

    /**
     * Convenience method to execute a command that is given as a single string.
     * 
     * @param description Text description of the command
     * @param baseCommand Un-prefixed command; arguments must be space-separate
     * @throws BackupException If the command fails
     */
    public void exec(String description, String baseCommand)
            throws BackupException
    {
        exec(description, baseCommand.split(" "));
    }

    // Creates a command array with prefix.
    private String[] makeCmd(String[] base)
    {
        if (prefix == null)
            return base;
        else
            return mergeArrays(prefix, base);
    }

    // Utility function to merge arrays.
    public String[] mergeArrays(String[] a1, String[] a2)
    {
        if (a2.length == 1 && a2[0].length() == 0)
            return a1;

        String[] merged = new String[a1.length + a2.length];
        int index = 0;
        for (int i = 0; i < a1.length; i++)
            merged[index++] = a1[i];
        for (int i = 0; i < a2.length; i++)
            merged[index++] = a2[i];
        return merged;
    }

    // Print command array as string.
    private String arrayToCommand(String[] array)
    {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < array.length; i++)
        {
            if (i > 0)
                sb.append(" ");
            sb.append(array[i]);
        }
        return sb.toString();
    }

    // Generate message containing error output from process.
    private String processInfo(ProcessExecutor pe)
    {
        String sep = System.getProperty("line.separator");
        StringBuffer sb = new StringBuffer();
        sb.append("Process exit value: " + pe.getExitValue() + sep);
        sb.append("Process timed out: " + pe.isTimedout() + sep);
        sb.append("Process exception " + pe.getError() + sep);
        sb.append("Process stderr: " + pe.getStderr());
        return sb.toString();
    }

    public List<String> getOutputLines()
    {
        return outputLines;
    }
}