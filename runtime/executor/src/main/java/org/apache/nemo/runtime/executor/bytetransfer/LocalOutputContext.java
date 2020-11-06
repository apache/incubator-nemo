package org.apache.nemo.runtime.executor.bytetransfer;

// Temporary class for implemeting shared memory

import org.apache.nemo.common.punctuation.Finishmark;
import org.apache.nemo.runtime.executor.datatransfer.OutputWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;

public final class LocalOutputContext extends ByteOutputContext {
  private static final Logger LOG = LoggerFactory.getLogger(LocalOutputContext.class.getName());

  final ConcurrentLinkedQueue queue;

  public LocalOutputContext() {
    super(null, null, null, null);
    this.queue = new ConcurrentLinkedQueue();
  }

  @Override
  public void close() throws IOException {
    return;
  }

  public void write(final Object element) {
    queue.offer(element);
  }

  public Object read() {
    Object element = queue.poll();
    LOG.error("LocalOutput reading data : {}", element);
    return element;
  }

  public ConcurrentLinkedQueue getQueue() {
    return queue;
  }

  /**
  public class LocalOutputContextIterator implements Iterator {
    Iterator<ConcurrentLinkedQueue> iter;
    // Whether finishmark has been receieved
    boolean finished;

    LocalOutputContextIterator(Iterator iter) {
      this.iter = iter;
      finished = false;
    }

    // blocking call
    @Override
    public boolean hasNext() {
      if (iter.hasNext()) return true;
      if (!iter.hasNext() && !finished) {
        // need to block this until receiving new data
        while (true) {
          if (iter.hasNext()) {
            return true;
          }
        }
      }
      else {
        return false;
      }
    }

    @Override
    public Object next() {
      Object element = iter.next();
      LOG.error("outputting elements {}", element);
      if (element instanceof Finishmark) {
        finished = true;
      }
      return element;
    }
  }
    */
}
