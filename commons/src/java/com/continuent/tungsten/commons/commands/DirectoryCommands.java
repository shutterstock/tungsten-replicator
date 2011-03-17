/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2007-2008 Continuent Inc.
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
 * Initial developer(s): Joe Daly
 * Contributor(s):
 */

package com.continuent.tungsten.commons.commands;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * Commands to assist in directory manipulation
 * 
 * @author <a href="mailto:joe.daly@continuent.com">Joe Daly</a>
 * @version 1.0
 */
public class DirectoryCommands
{

    static Logger logger = Logger.getLogger(DirectoryCommands.class);

    /**
     * Checks if the given path is a directory
     * 
     * @param path the path to check if its a directory
     * @return true if the path is a directory
     */
    public static boolean isDirectory(String path)
    {
        File f = new File(path);
        return f.isDirectory();
    }

    /**
     * Checks if the given path is a file
     * 
     * @param path the path to check if its a file
     * @return true if the given path is a file
     */
    public static boolean isFile(String path)
    {
        File f = new File(path);
        return f.isFile();
    }

    /**
     * Checks if the path given exists. No distinction is made between a file or
     * directory
     * 
     * @param path the path to check
     * @return true if a file or directry exists
     */
    public static boolean exists(String path)
    {
        File f = new File(path);
        return f.exists();
    }

    /**
     * Creates a directory
     * 
     * @param path the path to create
     * @return true if the directory was created
     */
    public static boolean mkdir(String path)
    {
        File f = new File(path);
        return f.mkdir();
    }

    /**
     * Creates a directory and any subdirectories
     * 
     * @param path the path to create, will also create any subdirectories
     * @return true if the directory was created
     */
    public static boolean mkdirs(String path)
    {
        File f = new File(path);
        return f.mkdirs();
    }

    /**
     * Removes the given directory and any sub directories. This is equivalent
     * on unix to running rm -rf.
     * 
     * @param path remove this directory and any sub directories
     * @return true if the directory no longer exists
     */
    public static boolean deleteDirectory(String path)
    {
        return deleteDirectory(new File(path));
    }

    /**
     * Deletes files in a directory matching a specific pattern
     * 
     * @param directoryPath the directory to delete files in
     * @param listingPattern the pattern the files must match to be deleted
     */
    public static void deleteFiles(String directoryPath, String listingPattern)
    {
        List<String> files = fileList(directoryPath, listingPattern, true);
        Iterator<String> filesIterator = files.iterator();
        while (filesIterator.hasNext())
        {
            String file = filesIterator.next();
            deleteFile(file);
        }
    }

    /**
     * Deletes a file
     * 
     * @param filePath the file to delete
     * @return true if the file is deleted
     */
    public static boolean deleteFile(String filePath)
    {
        File file = new File(filePath);
        if (file.isDirectory())
        {
            return false;
        }
        else
        {
            return file.delete();
        }
    }

    /**
     * Returns a listing of files in a directory matching a pattern.
     * 
     * @param directoryPath the directory to get a file listing of
     * @param listingPattern the pattern to search for in the file listing
     * @return the contents of the directory that match the pattern
     */
    public static List<String> fileList(String directoryPath,
            String listingPattern, boolean includeFullPath)
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("constructing file list from directory="
                    + directoryPath + " listPattern=" + listingPattern);
        }

        File directory = new File(directoryPath);
        List<String> fileList = new ArrayList<String>();

        if (directory.isDirectory())
        {
            File[] files = directory.listFiles();
            for (int i = 0; i < files.length; i++)
            {
                File f = files[i];
                if (f.isFile())
                {
                    String fileName = f.getName();
                    if (listingPattern == null)
                    {
                        if (includeFullPath)
                        {
                            if (logger.isDebugEnabled())
                            {
                                logger.debug("found file matching pattern="
                                        + listingPattern + " file="
                                        + directoryPath + File.separator
                                        + fileName);
                            }
                            fileList.add(directoryPath + File.separator
                                    + fileName);
                        }
                        else
                        {
                            fileList.add(fileName);
                        }
                    }
                    else
                    {
                        if (fileName.indexOf(listingPattern) != -1)
                        {
                            if (includeFullPath)
                            {
                                if (logger.isDebugEnabled())
                                {
                                    logger.debug("found file matching pattern="
                                            + listingPattern + " file="
                                            + directoryPath + File.separator
                                            + fileName);
                                }

                                fileList.add(directoryPath + File.separator
                                        + fileName);
                            }
                            else
                            {
                                if (logger.isDebugEnabled())
                                {
                                    logger.debug("found file matching pattern="
                                            + listingPattern + " file="
                                            + fileName);
                                }
                                fileList.add(fileName);
                            }
                        }
                    }
                }
            }
        }
        return fileList;
    }

    private static boolean deleteDirectory(File path)
    {
        if (path.exists())
        {
            File[] files = path.listFiles();
            for (int i = 0; i < files.length; i++)
            {
                if (files[i].isDirectory())
                {
                    deleteDirectory(files[i]);
                }
                else
                {
                    files[i].delete();
                }
            }
        }
        else
        {
            return true;
        }
        return (path.delete());
    }

    /**
     * Copy a file on the local filesystem
     * 
     * @param srcPath the source file
     * @param destPath where the file should be copied to
     * @throws Exception if there was a problem copying the file
     */
    public static void copyFile(String srcPath, String destPath)
            throws Exception
    {
        File srcFile = new File(srcPath);
        File destFile = new File(destPath);

        if (!srcFile.exists())
        {
            throw new Exception("source file does not exist");
        }

        String srcFileLocation = srcFile.getAbsolutePath();
        String destFileLocation = destFile.getAbsolutePath();
        if (srcFileLocation.equals(destFileLocation))
        {
            throw new Exception("source and destination are the same file");
        }

        InputStream input = new FileInputStream(srcFile);
        OutputStream output = new FileOutputStream(destFile);

        byte[] buf = new byte[1024];
        int length;
        while ((length = input.read(buf)) > 0)
        {
            output.write(buf, 0, length);
        }
        input.close();
        output.close();
    }

}
