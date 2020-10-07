package org.apache.nemo.compiler.frontend.beam.transform.coders;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.coder.FSTSingleton;
import org.apache.nemo.compiler.frontend.beam.transform.InMemoryTimerInternalsFactory;
import org.apache.nemo.compiler.frontend.beam.transform.NemoTimerInternals;
import org.joda.time.Instant;
import org.nustaq.serialization.FSTConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

public final class InMemoryTimerInternalsFactoryCoder<K> extends Coder<InMemoryTimerInternalsFactory<K>> {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryTimerInternalsFactoryCoder.class.getName());

  private final Coder<K> keyCoder;
  private final Coder windowCoder;
  private final TimerInternals.TimerDataCoder timerCoder;

  public InMemoryTimerInternalsFactoryCoder(final Coder<K> keyCoder,
                                            final Coder windowCoder) {
    this.keyCoder = keyCoder;
    this.windowCoder = windowCoder;
    this.timerCoder = TimerInternals.TimerDataCoder.of(windowCoder);
  }

  @Override
  public void encode(InMemoryTimerInternalsFactory<K> value, OutputStream outStream) throws CoderException, IOException {

    final DataOutputStream dos = new DataOutputStream(outStream);

    encodeNavigableSet(value.watermarkTimers, dos);
    encodeNavigableSet(value.processingTimers, dos);
    encodeNavigableSet(value.synchronizedProcessingTimers, dos);

    //LOG.info("Encoded navigable sets");

    dos.writeLong(value.inputWatermarkTime.getMillis());
    dos.writeLong(value.processingTime.getMillis());
    dos.writeLong(value.synchronizedProcessingTime.getMillis());

    LOG.info("Encoding timer key size: {}", value.timerInternalsMap.size());
    encodeTimerInternalsMap(value.timerInternalsMap, dos);
  }

  @Override
  public InMemoryTimerInternalsFactory<K> decode(InputStream inStream) throws CoderException, IOException {

    final Comparator<Pair<K, TimerInternals.TimerData>> comparator = (o1, o2) -> {
      final int comp = o1.right().compareTo(o2.right());
      if (comp == 0) {
        if (o1.left() == null) {
          return 0;
        } else {
          return o1.left().toString().compareTo(o2.left().toString());
        }
      } else {
        return comp;
      }
    };

    final DataInputStream dis = new DataInputStream(inStream);

    final NavigableSet<Pair<K, TimerInternals.TimerData>> watermarkTimers = decodeNavigableSet(dis, comparator);
    final NavigableSet<Pair<K, TimerInternals.TimerData>> processingTimers = decodeNavigableSet(dis, comparator);
    final NavigableSet<Pair<K, TimerInternals.TimerData>> synchronizedProcessingTimers = decodeNavigableSet(dis, comparator);

    final Instant inputWatermarkTime = new Instant(dis.readLong());
    final Instant processingTime = new Instant(dis.readLong());
    final Instant synchronizedProcessingTime = new Instant(dis.readLong());

    final Map<K, NemoTimerInternals> timerInternalsMap;
    try {
      timerInternalsMap = decodeTimerInternalsMap(
        watermarkTimers, processingTimers, synchronizedProcessingTimers, dis);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    return new InMemoryTimerInternalsFactory<>(
      watermarkTimers,
      processingTimers,
      synchronizedProcessingTimers,
      inputWatermarkTime,
      processingTime,
      synchronizedProcessingTime,
      timerInternalsMap);
  }

  private void encodeNavigableSet(final NavigableSet<Pair<K, TimerInternals.TimerData>> set,
                                  final DataOutputStream dos) throws IOException {
    dos.writeInt(set.size());

    for (final Pair<K, TimerInternals.TimerData> data : set) {
      keyCoder.encode(data.left(), dos);
      //LOG.info("Encode key");
      final TimerInternals.TimerData timerData = data.right();
      timerCoder.encode(timerData, dos);
      //LOG.info("Encode timer");
    }
  }

  private NavigableSet<Pair<K, TimerInternals.TimerData>> decodeNavigableSet(
    final DataInputStream is,
    final Comparator<Pair<K, TimerInternals.TimerData>> comparator) throws IOException {
    final int size = is.readInt();
    final NavigableSet<Pair<K, TimerInternals.TimerData>> set = new TreeSet<>(comparator);

    for (int i = 0; i < size; i++) {
      final K key = keyCoder.decode(is);
      final TimerInternals.TimerData timerData = timerCoder.decode(is);
      set.add(Pair.of(key, timerData));
    }

    return set;
  }

  private void encodeTimerInternalsMap(final Map<K, NemoTimerInternals> timerInternalsMap,
                                       final DataOutputStream dos) throws IOException {
    dos.writeInt(timerInternalsMap.size());
    final FSTConfiguration conf = FSTSingleton.getInstance();

    for (final Map.Entry<K, NemoTimerInternals> entry : timerInternalsMap.entrySet()) {
      final K key = entry.getKey();
      keyCoder.encode(key, dos);

      final NemoTimerInternals nemoTimerInternals = entry.getValue();
      final Table<StateNamespace, String, TimerInternals.TimerData> existingTimers = nemoTimerInternals.existingTimers;
      final Instant inputWatermarkTime = nemoTimerInternals.currentInputWatermarkTime();
      final Instant processingTime = nemoTimerInternals.currentProcessingTime();
      final Instant synchronizedProcessingTime = nemoTimerInternals.currentSynchronizedProcessingTime();
      final Instant outputWatermarkTime = nemoTimerInternals.currentOutputWatermarkTime();


      encodeTable(existingTimers, dos);

      conf.encodeToStream(dos, inputWatermarkTime);
      conf.encodeToStream(dos, processingTime);
      conf.encodeToStream(dos, synchronizedProcessingTime);
      conf.encodeToStream(dos, outputWatermarkTime);

      //LOG.info("Serialize instances");
    }
  }

  private Map<K, NemoTimerInternals> decodeTimerInternalsMap(
    final NavigableSet<Pair<K, TimerInternals.TimerData>> watermarkTimers,
    final NavigableSet<Pair<K, TimerInternals.TimerData>> processingTimers,
    final NavigableSet<Pair<K, TimerInternals.TimerData>> synchronizedProcessingTimers,
    final DataInputStream dis) throws Exception {

    final int size = dis.readInt();
    final Map<K, NemoTimerInternals> map = new HashMap<>();
    final FSTConfiguration conf = FSTSingleton.getInstance();

    for (int i = 0; i < size; i++) {
      final K key = keyCoder.decode(dis);
      final Table<StateNamespace, String, TimerInternals.TimerData> existingTimers = decodeTable(dis);
      final Instant inputWatermarkTime = (Instant) conf.decodeFromStream(dis);
      final Instant processingTime = (Instant) conf.decodeFromStream(dis);
      final Instant synchronizedProcessingTime = (Instant) conf.decodeFromStream(dis);
      final Instant outputWatermarkTime = (Instant) conf.decodeFromStream(dis);

      final NemoTimerInternals nemoTimerInternals =
        new NemoTimerInternals(key, watermarkTimers, processingTimers, synchronizedProcessingTimers,
          existingTimers, inputWatermarkTime, processingTime, synchronizedProcessingTime, outputWatermarkTime);

      map.put(key, nemoTimerInternals);
    }

    return map;
  }

  private void encodeTable(final Table<StateNamespace, String, TimerInternals.TimerData> existingTimers,
                           final DataOutputStream dos) throws IOException {
    dos.writeInt(existingTimers.size());

    //LOG.info("Start encode table");

    for (final Table.Cell<StateNamespace, String, TimerInternals.TimerData> cell : existingTimers.cellSet()) {
      dos.writeUTF(cell.getRowKey().stringKey());
      dos.writeUTF(cell.getColumnKey());
      timerCoder.encode(cell.getValue(), dos);
    }

    //LOG.info("End encode table");
  }

  private Table<StateNamespace, String, TimerInternals.TimerData> decodeTable(final DataInputStream dis) throws IOException {
    final int size = dis.readInt();
    final Table<StateNamespace, String, TimerInternals.TimerData> table = HashBasedTable.create();

    for (int i = 0; i < size; i++) {
      final StateNamespace stateNamespace = StateNamespaces.fromString(dis.readUTF(), windowCoder);
      final String s = dis.readUTF();
      final TimerInternals.TimerData timerData = timerCoder.decode(dis);

      table.put(stateNamespace, s, timerData);
    }

    return table;
  }


  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return null;
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {

  }

}
