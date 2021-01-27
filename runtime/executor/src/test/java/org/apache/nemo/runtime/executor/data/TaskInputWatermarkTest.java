package org.apache.nemo.runtime.executor.data;

import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.runtime.executor.common.DataFetcher;
import org.apache.nemo.runtime.executor.common.TaskInputWatermarkManager;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public final class TaskInputWatermarkTest {

  @Test
  public void testTaskInputWatermark() {
    final TaskInputWatermarkManager watermarkManager = new TaskInputWatermarkManager();

    final DataFetcher d1 = mock(DataFetcher.class);
    final int parallelism1 = 5;

    final DataFetcher d2 = mock(DataFetcher.class);
    final int parallelism2 = 3;

    watermarkManager.addDataFetcher(d1, parallelism1);
    watermarkManager.addDataFetcher(d2, parallelism2);

    // watermark 0
    // datafetcher1 watermark1
    assertFalse(watermarkManager.updateWatermark(d1, 0, 1).isPresent());
    assertFalse(watermarkManager.updateWatermark(d1, 1, 2).isPresent());
    assertFalse(watermarkManager.updateWatermark(d1, 2, 3).isPresent());
    assertFalse(watermarkManager.updateWatermark(d1, 3, 4).isPresent());
    assertFalse(watermarkManager.updateWatermark(d1, 4, 3).isPresent());

    // datafetcher2 watermark 2
    assertFalse(watermarkManager.updateWatermark(d2, 0, 2).isPresent());
    assertFalse(watermarkManager.updateWatermark(d2, 1, 3).isPresent());

    // emit watermark 1
    final Optional<Watermark> w = watermarkManager.updateWatermark(d2, 2, 4);

    assertTrue(w.isPresent());
    assertEquals(1L, w.get().getTimestamp());

    // watermark 2
    final Optional<Watermark> w2 = watermarkManager.updateWatermark(d1, 0, 4);

    assertTrue(w2.isPresent());
    assertEquals(2L, w2.get().getTimestamp());


    assertFalse(watermarkManager.updateWatermark(d1, 1, 3).isPresent());

    // watermark 3
    final Optional<Watermark> w3 = watermarkManager.updateWatermark(d2, 0, 4);

    assertTrue(w3.isPresent());
    assertEquals(3L, w3.get().getTimestamp());

  }
}
