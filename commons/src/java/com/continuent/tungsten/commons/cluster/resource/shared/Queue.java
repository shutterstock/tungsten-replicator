
package com.continuent.tungsten.commons.cluster.resource.shared;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import com.continuent.tungsten.commons.cluster.resource.Resource;
import com.continuent.tungsten.commons.cluster.resource.ResourceType;
import com.continuent.tungsten.commons.config.TungstenProperties;

public class Queue<T> extends Resource
{
    private static final long      serialVersionUID = 8153881753668230575L;

    
    private LinkedBlockingQueue<T> items            = new LinkedBlockingQueue<T>();
    private T                      lastItem         = null;

    public Queue(TungstenProperties props)
    {
        super(ResourceType.QUEUE, props.getString("name", "queue", true));
        props.applyProperties(this, true);
    }

    public Queue(String name)
    {
        super(ResourceType.QUEUE, name);
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public void put(T item)
    {
        synchronized (items)
        {
            try
            {
                items.put(item);
                lastItem = item;
            }
            catch (InterruptedException i)
            {
                // ignored
            }

        }
    }

    public T take()
    {
        try
        {
            return items.take();
        }
        catch (InterruptedException i)
        {
            // ignored
        }

        return null;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        List<T> list = new ArrayList<T>();
        list.addAll(items);

        builder.append(name).append("\n");
        builder.append("{").append("\n");
        for (T item : list)
        {
            builder.append(String.format("  %s\n", item));
        }
        builder.append("}").append("\n");

        return builder.toString();
    }

    public Collection<T> getItems()
    {
        return items;
    }

    public T getLastItem()
    {
        return lastItem;
    }

}
