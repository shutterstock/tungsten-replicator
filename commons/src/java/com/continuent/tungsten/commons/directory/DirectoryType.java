
package com.continuent.tungsten.commons.directory;

import java.io.Serializable;

public enum DirectoryType implements Serializable
{
    PHYSICAL(0), LOGICAL(1), UNDEFINED(-1);

    private final int index;

    DirectoryType(int index)
    {
        this.index = index;
    }

    public int getIndex()
    {
        return this.index;
    }

}
