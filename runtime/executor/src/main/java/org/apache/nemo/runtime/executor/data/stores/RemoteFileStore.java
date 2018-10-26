package org.apache.nemo.runtime.executor.data.stores;

import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * Interface for remote block stores (e.g., GlusterFS, ...).
 */
@DefaultImplementation(GlusterFileStore.class)
public interface RemoteFileStore extends BlockStore {
}
