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

/**
 * A simple interface used as a callback from DiagnosticWizard class.
 * 
 * @author <a href="mailto:linas.virbalas@continuent.com">Linas Virbalas</a>
 * @version 1.0
 */
public interface DiagnosticWizardPlugin
{
    /**
     * This method is being called from
     * {@link com.continuent.tungsten.commons.utils.DiagnosticWizard#diag(DiagnosticWizardPlugin)}
     * . It should return component specific diagnostic information, which
     * DiagnosticWizard will include into its return value. It is recommended to
     * write a header in the first line of the output for easier human
     * readibility, eg. if you're returning membership info, write
     * "# Members\n...".
     * 
     * @return Component specific diagnostic information.
     */
    public String diag();
}
