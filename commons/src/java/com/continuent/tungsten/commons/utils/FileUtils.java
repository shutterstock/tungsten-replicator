
package com.continuent.tungsten.commons.utils;

import java.io.File;
import java.util.Map;

public class FileUtils
{

    /**
     * Simpler interface to removeDirectory
     *
     * @param directoryName
     */
    public static boolean removeDirectory(String directoryName)
    {
        File directory = new File(directoryName);

        return removeDirectory(directory, null);
    }

    /**
     * Utility function to recursively remove a directory hierarchy and all
     * files in it. This function tracks what it does by putting entries in the
     * 'progress' map passed in.
     *
     * @param directory - directory to start at
     * @param progress - initialized map to be used to track progress.
     */
    public static boolean removeDirectory(File directory,
            Map<String, String> progress)
    {
        if (directory == null)
            return false;
        if (!directory.exists())
            return true;
        if (!directory.isDirectory())
            return false;

        String[] list = directory.list();

        if (list != null)
        {
            for (int i = 0; i < list.length; i++)
            {
                File entry = new File(directory, list[i]);

                if (entry.isDirectory())
                {
                    if (!removeDirectory(entry, progress))
                        return false;
                }
                else
                {
                    if (progress != null)
                        progress.put("delete file", entry.getName());

                    if (!entry.delete())
                        return false;
                }
            }
        }

        if (progress != null)
            progress.put("delete directory", directory.getName());
        return directory.delete();
    }
}
