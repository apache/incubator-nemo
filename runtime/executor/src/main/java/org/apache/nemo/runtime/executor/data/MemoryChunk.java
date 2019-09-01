/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.runtime.executor.data;

import net.jcip.annotations.NotThreadSafe;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;


/**
 * This class represents chunk of memory residing in off-heap region
 * managed by {@link MemoryPoolAssigner}, which is backed by {@link ByteBuffer}.
 */
@NotThreadSafe
public class MemoryChunk {

  // UNSAFE is used for random access and manipulation of the ByteBuffer.
  @SuppressWarnings("restriction") // to suppress warnings that are invoked whenever we use UNSAFE.
  protected static final sun.misc.Unsafe UNSAFE = getUnsafe();
  @SuppressWarnings("restriction")
  protected static final long BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
  private static final boolean LITTLE_ENDIAN = (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN);
  private static final int BYTE_SIZE = 1;
  private static final int SHORT_SIZE = 2;
  private static final int CHAR_SIZE = 2;
  private static final int INT_SIZE = 4;
  private static final int LONG_SIZE = 8;
  private final ByteBuffer buffer;
  // Since using UNSAFE does not automatically track the address and limit, it should be accessed
  // through address for data write and get, and addressLimit for sanity checks on the buffer use.
  private long address;
  private final long addressLimit;
  private final int size;

  /**
   * Creates a new memory chunk that represents the off-heap memory at the absolute address.
   * This class supports random access and manipulation of the data in the {@code ByteBuffer} using UNSAFE.
   * For sequential access, ByteBuffer of this class can be accessed and manipulated.
   *
   * @param offHeapAddress the address of the off-heap memory, {@link ByteBuffer}, of this MemoryChunk
   * @param buffer         the off-heap memory of this MemoryChunk
   */
  MemoryChunk(final long offHeapAddress, final ByteBuffer buffer) {
    if (offHeapAddress <= 0) {
      throw new IllegalArgumentException("negative pointer or size");
    }
    if (offHeapAddress >= Long.MAX_VALUE - Integer.MAX_VALUE) {
      throw new IllegalArgumentException("MemoryChunk initialized with too large address");
    }
    this.buffer = buffer;
    this.size = buffer.capacity();
    this.address = offHeapAddress;
    this.addressLimit = this.address + this.size;
  }

  /**
   * Creates a new memory chunk that represents the off-heap memory at the absolute address.
   *
   * @param buffer  the off-heap memory of this MemoryChunk
   */
  MemoryChunk(final ByteBuffer buffer) {
    this(getAddress(buffer), buffer);
  }

  /**
   * Gets the {@link ByteBuffer} from this MemoryChunk.
   *
   * @return  {@link ByteBuffer}
   */
  public ByteBuffer getBuffer() {
    if (address > addressLimit) {
      throw new IllegalStateException("MemoryChunk has been freed");
    }
    return buffer;
  }

  /**
   * Makes the duplicated instance of this MemoryChunk.
   *
   * @return the MemoryChunk with the same content of the caller instance
   */
  public MemoryChunk duplicate() {
    return new MemoryChunk(buffer.duplicate());
  }

  /**
   * Frees this MemoryChunk. No further operation possible after calling this method.
   */
  public void release() {
    buffer.clear();
    address = addressLimit + 1;
  }

  /**
   * Reads the byte at the given index.
   *
   * @param index from which the byte will be read
   * @return the byte at the given position
   */
  @SuppressWarnings("restriction")
  public final byte get(final int index) {
    final long pos = address + index;
    if (checkIndex(index, pos, BYTE_SIZE)) {
      return UNSAFE.getByte(pos);
    } else if (address > addressLimit) {
      throw new IllegalStateException("MemoryChunk has been freed");
    } else {
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * Writes the given byte into this buffer at the given index.
   *
   * @param index The position at which the byte will be written.
   * @param b     The byte value to be written.
   */
  @SuppressWarnings("restriction")
  public final void put(final int index, final byte b) {
    final long pos = address + index;
    if (checkIndex(index, pos, BYTE_SIZE)) {
      UNSAFE.putByte(pos, b);
    } else if (address > addressLimit) {
      throw new IllegalStateException("MemoryChunk has been freed");
    } else {
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * Copies the data of the MemoryChunk from the specified position to target byte array.
   *
   * @param index The position at which the first byte will be read.
   * @param dst The memory into which the memory will be copied.
   */
  public final void get(final int index, final byte[] dst) {
    get(index, dst, 0, dst.length);
  }

  /**
   * Copies all the data from the source byte array into the MemoryChunk
   * beginning at the specified position.
   *
   * @param index the position in MemoryChunk to start copying the data.
   * @param src the source byte array that holds the data to copy.
   */
  public final void put(final int index, final byte[] src) {
    put(index, src, 0, src.length);
  }

  /**
   * Bulk get method using nk.the specified index in the MemoryChunk.
   *
   * @param index the index in the MemoryChunk to start copying the data.
   * @param dst the target byte array to copy the data from MemoryChunk.
   * @param offset the offset in the destination byte array.
   * @param length the number of bytes to be copied.
   */
  @SuppressWarnings("restriction")
  public final void get(final int index, final byte[] dst, final int offset, final int length) {
    if ((offset | length | (offset + length) | (dst.length - (offset + length))) < 0) {
      throw new IndexOutOfBoundsException();
    }
    final long pos = address + index;
    if (checkIndex(index, pos, length)) {
      final long arrayAddress = BYTE_ARRAY_BASE_OFFSET + offset;
      UNSAFE.copyMemory(null, pos, dst, arrayAddress, length);
    } else if (address > addressLimit) {
      throw new IllegalStateException("MemoryChunk has been freed");
    } else {
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * Bulk put method using the specified index in the MemoryChunk.
   *
   * @param index the index in the MemoryChunk to start copying the data.
   * @param src the source byte array that holds the data to be copied to MemoryChunk.
   * @param offset the offset in the source byte array.
   * @param length the number of bytes to be copied.
   */
  @SuppressWarnings("restriction")
  public final void put(final int index, final byte[] src, final int offset, final int length) {
    if ((offset | length | (offset + length) | (src.length - (offset + length))) < 0) {
      throw new IndexOutOfBoundsException();
    }
    final long pos = address + index;
    if (checkIndex(index, pos, length)) {
      final long arrayAddress = BYTE_ARRAY_BASE_OFFSET + offset;
      UNSAFE.copyMemory(src, arrayAddress, null, pos, length);
    } else if (address > addressLimit) {
      throw new IllegalStateException("MemoryChunk has been freed");
    } else {
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * Reads a char value from the given position.
   *
   * @param index The position from which the memory will be read.
   * @return The char value at the given position.
   *
   * @throws IndexOutOfBoundsException If the index is negative, or larger then the chunk size minus CHAR_SIZE.
   */
  @SuppressWarnings("restriction")
  public final char getChar(final int index) {
    final long pos = address + index;
    if (checkIndex(index, pos, CHAR_SIZE)) {
      if (LITTLE_ENDIAN) {
        return UNSAFE.getChar(pos);
      } else {
        return Character.reverseBytes(UNSAFE.getChar(pos));
      }
    } else if (address > addressLimit) {
      throw new IllegalStateException("This MemoryChunk has been freed.");
    } else {
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * Writes a char value to the given position.
   *
   * @param index The position at which the memory will be written.
   * @param value The char value to be written.
   *
   * @throws IndexOutOfBoundsException If the index is negative, or larger then the chunk size minus CHAR_SIZE.
   */
  @SuppressWarnings("restriction")
  public final void putChar(final int index, final char value) {
    final long pos = address + index;
    if (checkIndex(index, pos, CHAR_SIZE)) {
      if (LITTLE_ENDIAN) {
        UNSAFE.putChar(pos, value);
      } else {
        UNSAFE.putChar(pos, Character.reverseBytes(value));
      }
    } else if (address > addressLimit) {
      throw new IllegalStateException("MemoryChunk has been freed");
    } else {
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * Reads a short integer value from the given position, composing them into a short value
   * according to the current byte order.
   *
   * @param index The position from which the memory will be read.
   * @return The short value at the given position.
   *
   * @throws IndexOutOfBoundsException If the index is negative, or larger then the chunk size minus SHORT_SIZE.
   */
  @SuppressWarnings("restriction")
  public final short getShort(final int index) {
    final long pos = address + index;
    if (checkIndex(index, pos, SHORT_SIZE)) {
      if (LITTLE_ENDIAN) {
        return UNSAFE.getShort(pos);
      } else {
        return Short.reverseBytes(UNSAFE.getShort(pos));
      }
    } else if (address > addressLimit) {
      throw new IllegalStateException("MemoryChunk has been freed");
    } else {
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * Writes the given short value into this buffer at the given position, using
   * the native byte order of the system.
   *
   * @param index The position at which the value will be written.
   * @param value The short value to be written.
   *
   * @throws IndexOutOfBoundsException If the index is negative, or larger then the chunk size minus SHORT_SIZE.
   */
  @SuppressWarnings("restriction")
  public final void putShort(final int index, final short value) {
    final long pos = address + index;
    if (checkIndex(index, pos, SHORT_SIZE)) {
      if (LITTLE_ENDIAN) {
        UNSAFE.putShort(pos, value);
      } else {
        UNSAFE.putShort(pos, Short.reverseBytes(value));
      }
    } else if (address > addressLimit) {
      throw new IllegalStateException("MemoryChunk has been freed");
    } else {
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * Reads an int value from the given position, in the system's native byte order.
   *
   * @param index The position from which the value will be read.
   * @return The int value at the given position.
   *
   * @throws IndexOutOfBoundsException If the index is negative, or larger then the chunk size minus INT_SIZE.
   */
  public final int getInt(final int index) {
    final long pos = address + index;
    if (checkIndex(index, pos, INT_SIZE)) {
      if (LITTLE_ENDIAN) {
        return UNSAFE.getInt(pos);
      } else {
        return Integer.reverseBytes(UNSAFE.getInt(pos));
      }
    } else if (address > addressLimit) {
      throw new IllegalStateException("MemoryChunk has been freed");
    } else {
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * Writes the given int value to the given position in the system's native byte order.
   *
   * @param index The position at which the value will be written.
   * @param value The int value to be written.
   *
   * @throws IndexOutOfBoundsException If the index is negative, or larger then the chunk size minus INT_SIZE.
   */
  public final void putInt(final int index, final int value) {
    final long pos = address + index;
    if (checkIndex(index, pos, INT_SIZE)) {
      if (LITTLE_ENDIAN) {
        UNSAFE.putInt(pos, value);
      } else {
        UNSAFE.putInt(pos, Integer.reverseBytes(value));
      }
    } else if (address > addressLimit) {
      throw new IllegalStateException("MemoryChunk has been freed");
    } else {
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * Reads a long value from the given position.
   *
   * @param index The position from which the value will be read.
   * @return The long value at the given position.
   *
   * @throws IndexOutOfBoundsException If the index is negative, or larger then the chunk size minus LONG_SIZE.
   */
  public final long getLong(final int index) {
    final long pos = address + index;
    if (checkIndex(index, pos, LONG_SIZE)) {
      if (LITTLE_ENDIAN) {
        return UNSAFE.getLong(pos);
      } else {
        return Long.reverseBytes(UNSAFE.getLong(pos));
      }
    } else if (address > addressLimit) {
      throw new IllegalStateException("MemoryChunk has been freed");
    } else {
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * Writes the given long value to the given position in the system's native byte order.
   *
   * @param index The position at which the value will be written.
   * @param value The long value to be written.
   *
   * @throws IndexOutOfBoundsException If the index is negative, or larger then the chunk size minus LONG_SIZE.
   */
  public final void putLong(final int index, final long value) {
    final long pos = address + index;
    if (checkIndex(index, pos, LONG_SIZE)) {
      if (LITTLE_ENDIAN) {
        UNSAFE.putLong(pos, value);
      } else {
        UNSAFE.putLong(pos, Long.reverseBytes(value));
      }
    } else if (address > addressLimit) {
      throw new IllegalStateException("MemoryChunk has been freed");
    } else {
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * Reads a float value from the given position, in the system's native byte order.
   *
   * @param index The position from which the value will be read.
   * @return The float value at the given position.
   *
   * @throws IndexOutOfBoundsException If the index is negative, or larger then the chunk size minus size of float.
   */
  public final float getFloat(final int index) {
    return Float.intBitsToFloat(getInt(index));
  }

  /**
   * Writes the given float value to the given position in the system's native byte order.
   *
   * @param index The position at which the value will be written.
   * @param value The float value to be written.
   *
   * @throws IndexOutOfBoundsException If the index is negative, or larger then the chunk size minus size of float.
   */
  public final void putFloat(final int index, final float value) {
    putInt(index, Float.floatToRawIntBits(value));
  }

  /**
   * Reads a double value from the given position, in the system's native byte order.
   *
   * @param index The position from which the value will be read.
   * @return The double value at the given position.
   *
   * @throws IndexOutOfBoundsException If the index is negative, or larger then the chunk size minus size of double.
   */
  public final double getDouble(final int index) {
    return Double.longBitsToDouble(getLong(index));
  }

  /**
   * Writes the given double value to the given position in the system's native byte order.
   *
   * @param index The position at which the memory will be written.
   * @param value The double value to be written.
   *
   * @throws IndexOutOfBoundsException If the index is negative, or larger then the chunk size minus size of double.
   */
  public final void putDouble(final int index, final double value) {
    putLong(index, Double.doubleToRawLongBits(value));
  }

  private boolean checkIndex(final int index, final long pos, final int typeSize) {
    return (index >= 0 && pos <= (addressLimit - typeSize));
  }

  @SuppressWarnings("restriction")
  private static sun.misc.Unsafe getUnsafe() {
    try {
      Field unsafeField = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
      unsafeField.setAccessible(true);
      return (sun.misc.Unsafe) unsafeField.get(null);
    } catch (Exception e) {
      throw new RuntimeException("Error while trying to access the sun.misc.Unsafe handle.");
    }
  }

  private static final Field ADDRESS_FIELD;

  static {
    try {
      ADDRESS_FIELD = java.nio.Buffer.class.getDeclaredField("address");
      ADDRESS_FIELD.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException("Cannot initialize MemoryChunk: off-heap memory is incompatible with this JVM.");
    }
  }

  private static long getAddress(final ByteBuffer buffer) {
    if (buffer == null) {
      throw new NullPointerException("Buffer null");
    }
    if (!buffer.isDirect()) {
      throw new IllegalArgumentException("Cannot initialize from non-direct ByteBuffer.");
    }
    try {
      return (Long) ADDRESS_FIELD.get(buffer);
    } catch (Exception e) {
      throw new RuntimeException("Could not access ByteBuffer address.");
    }
  }
}
