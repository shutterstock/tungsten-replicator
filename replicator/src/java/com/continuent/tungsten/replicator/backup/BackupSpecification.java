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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Contains backup metadata.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class BackupSpecification
{
    private String agentName;
    private List<BackupLocator> backupLocators;
    private Date backupDate;
    
    public BackupSpecification()
    {
    }

    public String getAgentName()
    {
        return agentName;
    }

    public void setAgentName(String agentName)
    {
        this.agentName = agentName;
    }

    public List<BackupLocator> getBackupLocators()
    {
        return backupLocators;
    }

    public Date getBackupDate()
    {
        return backupDate;
    }

    public void setBackupDate(Date backupDate)
    {
        this.backupDate = backupDate;
    }
    
    public void addBackupLocator(BackupLocator locator)
    {
        if(backupLocators == null)
            backupLocators = new ArrayList<BackupLocator>();
        backupLocators.add(locator);
    }

    public void releaseLocators()
    {
        for (BackupLocator locator : backupLocators)
        {
            locator.release();
        }
    }
}