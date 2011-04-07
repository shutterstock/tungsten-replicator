
package com.continuent.tungsten.commons.config;

public final class TungstenVersion
{
    public final static int    MAJOR           = 2;
    public final static int    MINOR           = 0;
    public final static String OPTIONAL_SUFFIX = "-rc-7";

    public final static String SEPARATOR       = "*********************************************************************";
    public final static String TITLE           = "* TUNGSTEN VERSION %s.%s%s";
    public final static String NEWLINE         = "\n";

    /**
     * Return formatted banner
     */
    public static String banner()
    {
        StringBuilder builder = new StringBuilder();

        builder.append(NEWLINE + NEWLINE + SEPARATOR + NEWLINE);
        builder.append(String.format(TITLE, MAJOR, MINOR, OPTIONAL_SUFFIX))
                .append(NEWLINE);
        builder.append(SEPARATOR).append(NEWLINE + NEWLINE);
        return builder.toString();
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        return banner();
    }

}
