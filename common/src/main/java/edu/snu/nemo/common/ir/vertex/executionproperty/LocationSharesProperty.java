/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.nemo.common.ir.vertex.executionproperty;

import edu.snu.nemo.common.ir.executionproperty.VertexExecutionProperty;

import java.util.HashMap;

/**
 * LocationShares ExecutionProperty.
 */
public final class LocationSharesProperty extends VertexExecutionProperty<HashMap<String, Integer>> {
    /**
     * Default constructor.
     * @param value the map from location to the number of TaskGroups that must be executed in that location
     */
    public LocationSharesProperty(final HashMap<String, Integer> value) {
        super(value);
    }

    /**
     * Static method for constructing {@link LocationSharesProperty}.
     * @param value the map from location to the number of TaskGroups that must be executed in that location
     * @return the execution property
     */
    public static LocationSharesProperty of(final HashMap<String, Integer> value) {
        return new LocationSharesProperty(value);
    }
}
