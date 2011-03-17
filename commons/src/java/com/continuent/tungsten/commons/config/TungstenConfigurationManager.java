
package com.continuent.tungsten.commons.config;

import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import sun.reflect.ReflectionFactory.GetReflectionFactoryAction;

import com.continuent.tungsten.commons.cluster.resource.Resource;
import com.continuent.tungsten.commons.cluster.resource.ResourceType;
import com.continuent.tungsten.commons.cluster.resource.physical.Member;
import com.continuent.tungsten.commons.cluster.resource.physical.PhysicalResourceFactory;
import com.continuent.tungsten.commons.cluster.resource.shared.Cluster;
import com.continuent.tungsten.commons.cluster.resource.shared.Root;
import com.continuent.tungsten.commons.cluster.resource.shared.Site;
import com.continuent.tungsten.commons.directory.ClusterGenericDirectory;
import com.continuent.tungsten.commons.directory.ClusterPhysicalDirectory;
import com.continuent.tungsten.commons.directory.ResourceNode;
import com.continuent.tungsten.commons.directory.ResourceTree;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.converters.Converter;
import com.thoughtworks.xstream.converters.MarshallingContext;
import com.thoughtworks.xstream.converters.UnmarshallingContext;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;

public class TungstenConfigurationManager
{
    private TungstenConfiguration              root            = null;
    private static PhysicalResourceFactory     resourceFactory = new PhysicalResourceFactory();
    private Map<String, TungstenConfiguration> index           = new TreeMap<String, TungstenConfiguration>();

    public TungstenConfigurationManager(Resource root)
    {
        this.setRoot(new TungstenConfiguration(root));
        index.put("/", getRoot());
    }

    /**
     * @param config
     * @see com.continuent.tungsten.commons.config.TungstenConfiguration#addChild(com.continuent.tungsten.commons.config.TungstenConfiguration)
     */
    public TungstenConfiguration addChild(TungstenConfiguration config,
            Resource resource)
    {
        TungstenConfiguration childConfig = new TungstenConfiguration(resource);
        config.addChild(childConfig);
        index.put(childConfig.getPath(), childConfig);
        return childConfig;
    }

    public TungstenConfiguration locate(String path)
    {
        return index.get(path);
    }

    public static TungstenConfiguration dirToConfiguration(
            ClusterPhysicalDirectory dir) throws Exception
    {
        return ClusterPhysicalDirectory.toConfiguration(dir);

    }

    public static TungstenConfiguration xmlToConfig(String xml)
    {
        XStream xstream = new XStream();
        xstream.processAnnotations(new Class[]{Resource.class,
                ClusterPhysicalDirectory.class, ResourceTree.class,
                ClusterGenericDirectory.class, TungstenConfiguration.class});

        xstream.registerConverter(new TungstenConfigurationConverter());
        xstream.addImplicitCollection(TungstenConfiguration.class, "children");
        xstream.alias("resource", Resource.class);
        xstream.alias("tungsten-configuration", TungstenConfiguration.class);
        xstream.alias("site", Site.class);
        xstream.alias("cluster", Cluster.class);
        xstream.alias("member", Member.class);

        TungstenConfiguration conf = (TungstenConfiguration) xstream
                .fromXML(xml);

        return conf;
    }

    public static String configToXML(TungstenConfiguration config)
            throws Exception
    {
        XStream xstream = new XStream();
        xstream.processAnnotations(new Class[]{Resource.class,
                ClusterPhysicalDirectory.class, ResourceTree.class,
                ClusterGenericDirectory.class, TungstenConfiguration.class});

        xstream.registerConverter(new TungstenConfigurationConverter());
        xstream.addImplicitCollection(TungstenConfiguration.class, "children");
        xstream.alias("resource", Resource.class);
        xstream.alias("tungsten-configuration", TungstenConfiguration.class);
        xstream.alias("site", Site.class);
        xstream.alias("cluster", Cluster.class);
        xstream.alias("member", Member.class);

        String conf = xstream.toXML(config);
        System.out.println("CONF:" + conf);

        TungstenConfigurationManager back = (TungstenConfigurationManager) xstream
                .fromXML(conf);

        System.out.println("BACK:" + back);
        System.out.println("FORTH:" + xstream.toXML(back.getRoot()));

        return conf;
    }

    // public TungstenConfiguration configFromXML()

    public static class TungstenConfigurationConverter implements Converter
    {
        Logger                       logger    = Logger
                                                       .getLogger(TungstenConfigurationConverter.class);
        TungstenConfigurationManager configMgr = null;

        public void marshal(Object value, HierarchicalStreamWriter writer,
                MarshallingContext context)
        {
            TungstenConfiguration config = (TungstenConfiguration) value;
            writer.startNode(String.format("%s", config.getResource()
                    .getClass().getSimpleName().toLowerCase()));
            writer.addAttribute("name", config.getResource().getName());
            attributesFromProperties(writer, config.toProperties());
            for (TungstenConfiguration child : config.getChildren())
            {
                marshal(child, writer, context);
            }
            writer.endNode();
        }

        public Object unmarshal(HierarchicalStreamReader reader,
                UnmarshallingContext context)
        {
            String value = reader.getNodeName().toUpperCase();

            if (value.equals("TUNGSTEN-CONFIGURATION"))
            {
                reader.moveDown();
                value = reader.getNodeName().toUpperCase();
            }

            ResourceType type = ResourceType.UNDEFINED;

            try
            {
                type = ResourceType.valueOf(value);

            }
            catch (Exception e)
            {
                System.out.println(e);
                reader.moveUp();
            }

            String name = reader.getAttribute("name");
            try
            {
                TungstenConfiguration currentNode = null;
                TungstenConfiguration childNode = null;

                if (type == ResourceType.ROOT)
                {
                    configMgr = new TungstenConfigurationManager(new Root());
                    childNode = configMgr.getRoot();
                }
                else
                {
                    currentNode = (TungstenConfiguration) context.get("config");
                    Resource resource = resourceFactory.createInstance(type,
                            name, new ResourceNode(currentNode.getResource()));
                    attributesToProperties(reader, resource.getProperties());
                    childNode = configMgr.addChild(currentNode, resource);
                }
                context.put("config", childNode);

                while (reader.hasMoreChildren())
                {
                    reader.moveDown();
                    unmarshal(reader, context);
                }
                reader.moveUp();

                return configMgr;

            }
            catch (Exception e)
            {
                logger.error("Unable to unmarshall", e);
                return null;
            }

        }

        @SuppressWarnings("unchecked")
        public boolean canConvert(Class clazz)
        {
            return (TungstenConfiguration.class.isAssignableFrom(clazz));
        }

        private void attributesFromProperties(HierarchicalStreamWriter writer,
                TungstenProperties attrProps)
        {
            Properties props = attrProps.getProperties();
            for (Object key : props.keySet())
            {
                writer.addAttribute(key.toString(), props.get(key).toString());
            }
        }

        private void attributesToProperties(HierarchicalStreamReader reader,
                TungstenProperties attrProps)
        {
            int attributeCount = reader.getAttributeCount();

            for (int i = 0; i < attributeCount; i++)
            {
                String key = reader.getAttributeName(i);
                Object value = reader.getAttribute(i);

                // names are taken care of elsewhere...
                if (key.equals(Resource.NAME))
                    continue;

                attrProps.setObject(key, value);
            }
        }
    }

    /**
     * Returns the root value.
     * 
     * @return Returns the root.
     */
    public TungstenConfiguration getRoot()
    {
        return root;
    }

    /**
     * Sets the root value.
     * 
     * @param root The root to set.
     */
    public void setRoot(TungstenConfiguration root)
    {
        this.root = root;
    }

    public String toString()
    {
        StringBuilder builder = new StringBuilder();

        for (String key : index.keySet())
        {
            builder.append(key).append("\n");
        }

        return builder.toString();
    }

}
