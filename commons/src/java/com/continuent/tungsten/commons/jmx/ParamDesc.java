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
 * Initial developer(s): Ed Archibald
 * Contributor(s):
 */
package com.continuent.tungsten.commons.jmx;

import java.lang.annotation.*;

/**
 * 
 * This class defines an annotation element that can be used to identify parameter
 * names and roles at runtime.  The primary function of this annotation is for use
 * in annotating MBeans so that they can be self-documenting to the management framework.
 * 
 * @author <a href="mailto:edward.archibald@continuent.com">Edward Archibald</a>
 * @version 1.0
 */

@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.RUNTIME)
public @interface ParamDesc
{
    String name();
    String description() default "";
    String displayName() default "";
}
