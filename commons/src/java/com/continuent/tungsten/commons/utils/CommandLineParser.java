/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2010 Continuent Inc.
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
 * Contributor(s): Linas Virbalas, Gilles Rayrat
 */

package com.continuent.tungsten.commons.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Vector;

import jline.ConsoleReader;

public class CommandLineParser
{
    private static final String COMMENT_INTRODUCER = "#";

    private static final String FLAG_INTRODUCER    = "-";
    private static final char   LONG_FLAG          = 'l';
    private static final char   RECURSIVE_FLAG     = 'R';
    public static final char    ABSOLUTE_FLAG      = 'A';
    public static final char    PARENTS_FLAG       = 'p';
    public static final char    BACKGROUND_FLAG    = '&';

    private static final String BACKGROUND_TOKEN   = "&";
    private static final String REDIRECT_OUT_TOKEN = ">";
    private static final String REDIRECT_IN_TOKEN  = "<";

    private Vector<String>      ctrl               = null;

    public static void main(String[] args)
    {
        try
        {
            ConsoleReader reader = new ConsoleReader();
            CommandLineParser parser = new CommandLineParser(null);
            Command cmd = null;

            while ((cmd = parser.getCommand(reader, "> ", null, false, true)) != null)
            {
                System.out.println(CLUtils.printArgs(cmd.getTokens()));
                System.out.println(cmd.isBackground()
                        ? "BACKGROUND"
                        : "FOREGROUND");
                System.out.println(cmd.isLong() ? "LONG" : "SHORT");
                System.out.println(cmd.isRecursive() ? "RECURSIVE" : "FLAT");
                System.out.println(cmd.isRedirectInput() ? "INPUT="
                        + cmd.getInput() : "STDIN");
                System.out.println(cmd.isRedirectOutput() ? "OUTPUT="
                        + cmd.getOutput() : "STDOUT");

            }
        }
        catch (IOException e)
        {
            System.out.println(e);
        }
    }

    public CommandLineParser(Vector<String> ctrl)
    {
        this.ctrl = ctrl;
    }

    public Command parse(String commandLine)
    {
        return parse(commandLine, true);
    }

    /**
     * This method parses a set of tokens returned by a simple command line
     * parser, strips out any 'meta' commands and sets the appropriate flags,
     * and returns the 'clean' set of tokens to be processed by the command
     * processor.
     */
    public Command parse(String commandLine, boolean parseFlags)
    {
        Command command = new Command(commandLine);

        String splitPattern = null;

        if (parseFlags)
        {
            splitPattern = "\\s+|\\s+-\\b|=|'|\"";
        }
        else
        {
            splitPattern = "\\s+|=|'|\"";
        }

        // First strip blanks
        Vector<String> noBlanks = new Vector<String>();
        noBlanks.toArray(new String[noBlanks.size()]);
        String[] tokens = commandLine.split(splitPattern);
        for (String token : tokens)
        {
            if (!token.trim().equals(""))
                noBlanks.add(token.trim());
        }

        // If there are no non-blank tokens, just return null;
        if (noBlanks.size() == 0)
        {
            return null;
        }

        // Now evaluate the tokens and set appropriate flags,
        // stripping out any tokens that are flag-specific
        int i = 0;
        while (true)
        {
            if (i == noBlanks.size())
                break;

            String currentToken = noBlanks.get(i);

            if (parseFlags && currentToken.trim().startsWith(FLAG_INTRODUCER))
            {
                char[] flagChars = currentToken.trim().toCharArray();

                for (int j = 1; j < flagChars.length; j++)
                {
                    char flagChar = flagChars[j];

                    if (flagChar == LONG_FLAG)
                        command.setIsLong(true);
                    else if (flagChar == RECURSIVE_FLAG)
                        command.setIsRecursive(true);
                    else if (flagChar == PARENTS_FLAG)
                        command.setIncludeParents(true);
                    else if (flagChar == ABSOLUTE_FLAG)
                        command.setIsAbsolute(true);
                    else if (flagChar == BACKGROUND_FLAG)
                    {
                        if (i + 1 == noBlanks.size()
                                && (j + 1 == flagChars.length))
                        {
                            command.setIsBackground(true);
                        }
                        else
                            CLUtils
                                    .println("The token '&' can only appear at the end of a command");
                    }
                }
            }
            else if (currentToken.trim().equals(REDIRECT_IN_TOKEN))
            {
                if (i + 1 < noBlanks.size())
                {
                    String input = noBlanks.get(++i);
                    command.setIsRedirectInput(true, input);
                }
                else
                {
                    CLUtils.println("No arg supplied for input redirection");
                }
            }
            else if (currentToken.trim().equals(REDIRECT_OUT_TOKEN))
            {
                if (i + 1 < noBlanks.size())
                {
                    String output = noBlanks.get(++i);
                    command.setIsRedirectOutput(true, output);
                }
                else
                {
                    CLUtils.println("No arg supplied for output redirection");
                }
            }
            else if (currentToken.trim().endsWith(BACKGROUND_TOKEN))
            {
                if (i + 1 == noBlanks.size())
                {
                    command.setIsBackground(true);
                    if (currentToken.trim().length() > 1)
                    {
                        currentToken = currentToken.trim().substring(0,
                                currentToken.indexOf(BACKGROUND_TOKEN));
                        command.addToken(currentToken);
                    }
                }
                else
                    CLUtils
                            .println("The token '&' can only appear at the end of a command");
            }
            else
            {
                command.addToken(currentToken);
            }

            i++;
        }

        if (command.getTokens() != null)
            return command;

        return null;
    }

    public Command getCommand(ConsoleReader cr, String prompt,
            BufferedReader in, boolean semiDelimited) throws IOException
    {
        return getCommand(cr, prompt, in, semiDelimited, true);
    }

    public Command getCommand(ConsoleReader cr, String prompt,
            BufferedReader in, boolean semiDelimited, boolean printPrompt)
            throws IOException
    {
        String inbuf = null;
        String dataToUse = "";
        int promptLength = prompt.length();
        int indent = 2;
        int lastIndent = 0;
        String altPrompt = "->  ";

        while (true)
        {
            if (cr != null)
            {
                if (printPrompt)
                    inbuf = cr.readLine(prompt);
                else
                    inbuf = cr.readLine("");
            }
            else
            {
                if (printPrompt)
                    System.out.print(prompt);
                inbuf = in.readLine();
            }

            if (inbuf == null)
            {
                CLUtils.println("\nExiting...");
                System.exit(0);
            }

            dataToUse += inbuf;

            if (!inbuf.trim().endsWith(";") && semiDelimited
                    && !isCtrl(inbuf.trim()))
            {
                dataToUse += "\n";

                if (inbuf.trim().endsWith("("))
                {
                    indent += 2;
                }
                else if (inbuf.trim().endsWith(")"))
                {
                    if (indent >= lastIndent)
                    {
                        indent -= lastIndent;
                    }
                }
                prompt = altPrompt + indent(indent + promptLength);
                lastIndent = indent;
                continue;
            }
            else
            {
                break;
            }
        }

        return parse(dataToUse, true);
    }

    public Command getContinuousCommand(ConsoleReader cr, String prompt,
            BufferedReader in, String continuationChar,
            boolean allowContinuation, boolean printPrompt) throws IOException
    {
        String inbuf = null;
        String dataToUse = "";
        int promptLength = prompt.length();
        int indent = 2;
        int lastIndent = 0;
        String altPrompt = "->  ";

        while (true)
        {
            if (cr != null)
            {
                if (printPrompt)
                    inbuf = cr.readLine(prompt);
                else
                    inbuf = cr.readLine("");
            }
            else
            {
                if (printPrompt)
                    System.out.print(prompt);
                inbuf = in.readLine();
            }

            if (inbuf == null)
            {
                CLUtils.println("\nExiting...");
                System.exit(0);
            }

            if (inbuf.startsWith(COMMENT_INTRODUCER))
                continue;

            if (inbuf.trim().endsWith(continuationChar) && allowContinuation
                    && !isCtrl(inbuf.trim()))
            {
                dataToUse += inbuf.trim().substring(0,
                        inbuf.trim().lastIndexOf(continuationChar));

                if (inbuf.trim().endsWith("("))
                {
                    indent += 2;
                }
                else if (inbuf.trim().endsWith(")"))
                {
                    if (indent >= lastIndent)
                    {
                        indent -= lastIndent;
                    }
                }
                prompt = altPrompt + indent(indent + promptLength);
                lastIndent = indent;
                continue;
            }
            else
            {
                dataToUse += inbuf.trim();
                break;
            }
        }

        return parse(dataToUse, true);
    }

    private String indent(int width)
    {
        String spaces = "";
        for (int i = 0; i < width; i++)
            spaces += " ";

        return spaces;
    }

    private boolean isCtrl(String text)
    {
        if (ctrl == null)
            return false;

        return (ctrl.contains(text));
    }

}
