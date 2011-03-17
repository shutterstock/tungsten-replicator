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
 * Initial developer(s): Robert Hodges
 */

package com.continuent.tungsten.replicator.management;

import java.util.regex.Pattern;

import com.continuent.tungsten.commons.patterns.fsm.Action;
import com.continuent.tungsten.commons.patterns.fsm.Event;

/**
 * Defines an event containing an extended command which a regexp specifying
 * states in which the command may be legally processed.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class ExtendedActionEvent extends Event
{
    private final String  stateRegexp;
    private final Pattern statePattern;
    private final Action  extendedAction;

    public ExtendedActionEvent(String stateRegexp, Action extendedAction)
    {
        super(null);
        this.stateRegexp = stateRegexp;
        this.extendedAction = extendedAction;
        this.statePattern = Pattern.compile(stateRegexp);
    }

    public String getStateRegexp()
    {
        return stateRegexp;
    }

    public Action getExtendedAction()
    {
        return extendedAction;
    }

    public Pattern getStatePattern()
    {
        return statePattern;
    }
}