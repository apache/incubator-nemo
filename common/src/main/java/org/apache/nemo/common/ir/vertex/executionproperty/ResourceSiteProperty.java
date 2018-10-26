package org.apache.nemo.common.ir.vertex.executionproperty;

import org.apache.nemo.common.ir.executionproperty.VertexExecutionProperty;

import java.util.HashMap;

/**
 * Map between node name and the number of parallelism which will run on the node.
 * TODO #169: Use sites (not node names) in ResourceSiteProperty
 */
public final class ResourceSiteProperty extends VertexExecutionProperty<HashMap<String, Integer>> {
    /**
     * Default constructor.
     * @param value the map from location to the number of Task that must be executed on the node
     */
    public ResourceSiteProperty(final HashMap<String, Integer> value) {
        super(value);
    }

    /**
     * Static method for constructing {@link ResourceSiteProperty}.
     * @param value the map from location to the number of Task that must be executed on the node
     * @return the execution property
     */
    public static ResourceSiteProperty of(final HashMap<String, Integer> value) {
        return new ResourceSiteProperty(value);
    }
}
