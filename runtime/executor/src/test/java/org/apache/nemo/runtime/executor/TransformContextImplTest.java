package org.apache.nemo.runtime.executor;

import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.runtime.executor.data.BroadcastManagerWorker;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@link TransformContextImpl}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({BroadcastManagerWorker.class})
public class TransformContextImplTest {
  private Transform.Context context;

  @Before
  public void setUp() {
    final BroadcastManagerWorker broadcastManagerWorker = mock(BroadcastManagerWorker.class);
    when(broadcastManagerWorker.get("a")).thenReturn("b");
    this.context = new TransformContextImpl(broadcastManagerWorker);
  }

  @Test
  public void testContextImpl() {
    assertEquals("b", this.context.getBroadcastVariable("a"));

    final String sampleText = "test_text";

    assertFalse(this.context.getSerializedData().isPresent());

    this.context.setSerializedData(sampleText);
    assertTrue(this.context.getSerializedData().isPresent());
    assertEquals(sampleText, this.context.getSerializedData().get());
  }
}
