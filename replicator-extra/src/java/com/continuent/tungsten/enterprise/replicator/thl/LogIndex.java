/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2010 Continuent Inc.
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
 * Initial developer(s): Stephane Giron
 * Contributor(s): Robert Hodges
 */

package com.continuent.tungsten.enterprise.replicator.thl;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;

import com.continuent.tungsten.commons.commands.FileCommands;
import com.continuent.tungsten.commons.config.Interval;
import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * Implements an in-memory index showing the starting sequence number of each
 * index file. Index operations are fully synchronized to ensure there are no
 * issues due to concurrent access across threads.
 * 
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 * @version 1.0
 */
public class LogIndex
{
    static Logger                    logger = Logger.getLogger(LogIndex.class);

    private ArrayList<LogIndexEntry> index;
    private File                     logDir;
    private String                   filePrefix;
    private long                     retentionMillis;

    /**
     * Creates a new in-memory instance on all log files in a particular
     * directory.
     * 
     * @param logDir Log directory
     * @param filePrefix Prefix for log files
     * @param retentionMillis Amount of time to retain log files before
     *            auto-deleting
     * @throws ReplicatorException Thrown in the event of an error constructing
     *             the index
     */
    public LogIndex(File logDir, String filePrefix, long retentionMillis)
            throws ReplicatorException, InterruptedException
    {
        index = new ArrayList<LogIndexEntry>();
        this.logDir = logDir;
        this.filePrefix = filePrefix;
        this.retentionMillis = retentionMillis;
        build();
    }

    // Builds the index.
    private synchronized void build() throws ReplicatorException,
            InterruptedException
    {
        logger.info("Building file index on log directory: " + logDir);

        // Find the log files and sort into file name order.
        FileFilter fileFilter = new FileFilter()
        {
            public boolean accept(File file)
            {
                return !file.isDirectory()
                        && file.getName().startsWith(filePrefix);
            }
        };
        File[] files = logDir.listFiles(fileFilter);
        Arrays.sort(files);

        // Scan each file to get the base sequence number of the file. This
        // is incremented to give the starting index number of this file.
        // We use the starting number of the next index entry to compute the
        // ending index entry.
        LogIndexEntry lastEntry = null;
        for (File file : files)
        {
            // Try to read the base sequence number. Any file that cannot be
            // read is ignored for indexing purposes.
            if (logger.isDebugEnabled())
                logger.debug("Checking " + file.getName());
            LogFile lf = new LogFile(file);
            lf.prepareRead();
            long seqno = lf.getBaseSeqno();

            // If we get -1 it means we have the first file in the
            // index. Try to read the header of the first record to
            // get the correct starting sequence.
            if (seqno < 0)
            {
                try
                {
                    LogRecord record1 = lf.readRecord(0);
                    if (!record1.isEmpty() && !record1.isTruncated())
                    {
                        if (record1.getData()[0] == LogRecord.EVENT_REPL)
                        {
                            LogEventReplReader eventReader = new LogEventReplReader(
                                    record1, null, false);
                            seqno = eventReader.getSeqno();
                            eventReader.done();
                        }
                        else
                        {
                            logger
                                    .warn("Unexpected record type in first log record: type="
                                            + record1.getData()[0]
                                            + " file="
                                            + lf.getFile().getAbsolutePath());
                        }
                    }
                }
                catch (IOException e)
                {
                    logger.warn(
                            "Unable to read sequence number of first log record: file="
                                    + lf.getFile().getAbsolutePath(), e);
                }
            }

            // Decrement to set the end seqno of the previous index entry.
            if (lastEntry != null)
            {
                lastEntry.endSeqno = seqno - 1;
                if (logger.isDebugEnabled())
                    logger.debug("Updating " + lastEntry);
            }

            // Create the next index entry.
            LogIndexEntry ie = new LogIndexEntry(seqno, Long.MAX_VALUE, lf
                    .getFile().getName());
            index.add(ie);
            if (logger.isDebugEnabled())
                logger.debug("Adding index entry: " + ie);

            // Remember this entry and release the log file.
            lastEntry = ie;
            lf.release();
        }
        Collections.sort(index);
        logger.info("Constructed index; total log files added=" + index.size());
    }

    /**
     * Returns true if the index is empty.
     * 
     * @return
     */
    public synchronized boolean isEmpty()
    {
        return index.isEmpty();
    }

    /**
     * Returns the number of files in the index.
     */
    public synchronized int size()
    {
        return index.size();
    }

    /**
     * Releases resources in the index.
     */
    public synchronized void release()
    {
        index.clear();
    }

    /**
     * {@inheritDoc}
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        StringBuffer ind = new StringBuffer();
        for (LogIndexEntry entry : index)
        {
            ind.append(entry);
            ind.append('\n');
        }
        return ind.toString();
    }

    /**
     * Sets the maximum sequence number this index knows about by updating the
     * last index entry. This routine takes care of patching up the first index
     * entry in a new log, which starts with the default value -1 until we find
     * the correct sequence number.
     */
    public synchronized void setMaxIndexedSeqno(long seqno)
    {
        if (index != null && !index.isEmpty())
        {
            LogIndexEntry entry = index.get(index.size() - 1);
            entry.endSeqno = seqno;
            if (entry.startSeqno < 0)
            {
                // Patch up start entry on new index.
                entry.startSeqno = seqno;
            }
        }
    }

    /**
     * Returns the maximum sequence number this index knows about.
     * @return -1 if index is empty, otherwise, return the max value
     */
    public synchronized long getMaxIndexedSeqno()
    {
        if (index == null || index.isEmpty())
            return -1;
        LogIndexEntry entry = index.get(index.size() - 1);
        if (entry.startSeqno < 0)
            return -1;
        else
            return entry.endSeqno;
    }

    /**
     * Returns the minimum sequence number this index knows about.
     */
    public synchronized long getMinIndexedSeqno()
    {
        if (index == null || index.isEmpty())
            return -1;
        else
            return index.get(0).startSeqno;
    }

    /**
     * Locates and returns the file that contains a given sequence number. The
     * implementation uses a linear search, so we assume this is a comparatively
     * rare operation on connection startup.
     */
    public synchronized String getFile(long seqno)
    {
        if (logger.isDebugEnabled())
            logger.debug("Request to find seqno in index: seqno=" + seqno
                    + " index size=" + index.size());

        // If the value is less than the smallest indexed sequence number,
        // we don't have a file to offer. Return a null in this case.
        if (seqno < getMinIndexedSeqno() || seqno < 0)
            return null;

        // Search the current file index.
        LogIndexEntry previousEntry = null;
        for (LogIndexEntry indexEntry : index)
        {
            if (indexEntry.contains(seqno))
                return indexEntry.fileName;
            previousEntry = indexEntry;
        }
        if (previousEntry != null)
            return previousEntry.fileName;
        else
            return null;
    }

    /**
     * Returns a copy of the index entries in sorted order.
     */
    public synchronized List<LogIndexEntry> getIndexCopy()
    {
        return new ArrayList<LogIndexEntry>(index);
    }

    /**
     * Returns the last index file or null if no such file exists.
     */
    public synchronized String getLastFile()
    {
        if (index.size() == 0)
            return null;
        else
            return index.get(index.size() - 1).fileName;
    }

    /**
     * Adds a new file to the index.
     * 
     * @seqno Starting sequence number in the file
     * @fileName Name of the log file
     */
    public synchronized void addNewFile(long seqno, String fileName)
    {
        // Add the entry.
        logger.info("Adding new index entry for " + fileName
                + " starting at seqno " + seqno);
        index.add(new LogIndexEntry(seqno, seqno, fileName));

        // If retentions are enabled, this is a good time to check for files
        // to purge. Note that we always retain the last two files in the
        // index to prevent unhappy accidents due to deleting a file that is
        // currently active.
        if (retentionMillis > 0)
        {
            File[] purgeCandidates = FileCommands.filesOverRetention(logDir,
                    filePrefix, 2);
            File[] filesToPurge = FileCommands.filesOverModDate(
                    purgeCandidates, new Interval(retentionMillis));
            if (filesToPurge.length > 0)
            {
                for (File file : filesToPurge)
                    removeFile(file.getName());

                FileCommands.deleteFiles(filesToPurge, false);
            }
        }
    }

    /**
     * Remove a file from the index.
     */
    public synchronized void removeFile(String fileName)
    {
        for (LogIndexEntry entry : index)
        {
            if (fileName.equals(entry.fileName))
            {
                index.remove(entry);
                logger.info("Removed file from disk log index: " + fileName);
                return;
            }
        }
        logger.warn("Attempt to remove non-existent file from disk log index: "
                + fileName);
    }

    /**
     * Validates the index by ensuring that each file exists and that the log
     * entries have matching start and end dates.
     */
    public void validate(File logDir) throws LogConsistencyException
    {
        long prevEndSeqno = -1;
        for (LogIndexEntry entry : this.index)
        {
            // Check for file existence.
            File f = new File(logDir, entry.fileName);
            if (!f.exists())
            {
                throw new LogConsistencyException("Indexed file is missing: "
                        + entry.toString());
            }

            // Ensure that there is no gap between sequence numbers on
            // index entries.
            if (prevEndSeqno >= 0 && (prevEndSeqno + 1) != entry.startSeqno)
            {
                throw new LogConsistencyException(
                        "Start seqno does not match previous entry's end seqno value: prev end seqno="
                                + prevEndSeqno + " " + entry.toString());
            }

            // Ensure that end sequence number is greater than or equal to
            // the start.
            if (entry.startSeqno > entry.endSeqno)
            {
                throw new LogConsistencyException(
                        "Start seqno greater than end seqno: "
                                + entry.toString());
            }

            prevEndSeqno = entry.endSeqno;
        }
    }
}