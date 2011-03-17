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

package com.continuent.tungsten.commons.config;

/**
 * Represents methods used to work with wildcard patterns.
 * 
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 * @version 1.0
 */
public class WildcardPattern
{
    private static final long serialVersionUID = 1L;

    /**
     * Converts wildcard pattern to a regular expression pattern.
     * 
     * @param wildcard String that might contain * and ? wildcards.
     * @return Regular expression matching ready string.
     */
    public static String wildcardToRegex(String wildcard)
    {
        StringBuffer s = new StringBuffer(wildcard.length());
        s.append('^');
        for (int i = 0; i < wildcard.length(); i++)
        {
            char c = wildcard.charAt(i);
            switch (c)
            {
                // Support for * and ? wildcards:
                case '*' :
                    s.append(".*");
                    break;
                case '?' :
                    s.append(".");
                    break;
                // Escape special regular expression characters:
                case '(' :
                case ')' :
                case '[' :
                case ']' :
                case '$' :
                case '^' :
                case '.' :
                case '{' :
                case '}' :
                case '|' :
                case '\\' :
                    s.append("\\");
                    s.append(c);
                    break;
                default :
                    s.append(c);
                    break;
            }
        }
        s.append('$');
        return s.toString();
    }
}