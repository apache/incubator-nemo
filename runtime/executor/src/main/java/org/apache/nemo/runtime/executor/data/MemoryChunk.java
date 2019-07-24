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

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;


/**
 * This class represents chunk of memory residing in off-heap region
 * managed by {@link MemoryPoolAssigner}, which is backed by {@link ByteBuffer}.
 */
public class MemoryChunk {

  @SuppressWarnings("restriction")
  protected static final sun.misc.Unsafe UNSAFE = getUnsafe();
  @SuppressWarnings("restriction")
  protected static final long BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
  private static final boolean LITTLE_ENDIAN = (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN);
  private final ByteBuffer buffer;
  private long address;
  private final long addressLimit;
  private boolean isFree;
  private final int size;
  private final boolean sequential;

  /**
   * Creates a new memory chunk that represents the off-heap memory at the absolute address.
   * This class can be created in two modes: sequential access mode or random access mode.
   * Sequential access mode supports convenient sequential access of {@link ByteBuffer}.
   * Random access mode supports random access and manipulation of the data in the {@code ByteBuffer} using UNSAFE.
   * No automatic tracking of position, limit, capacity, etc. of {@code ByteBuffer} for random access mode.
   *
   * @param offHeapAddress the address of the off-heap memory, {@link ByteBuffer}, of this MemoryChunk
   * @param buffer         the off-heap memory of this MemoryChunk
   * @param sequential     whether this MemoryChunk is in sequential mode or not.
   */
  MemoryChunk(final long offHeapAddress, final ByteBuffer buffer, final boolean sequential) {
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
    this.isFree = false;
    this.sequential = sequential;
  }

  /**
   * Creates a new memory chunk that represents the off-heap memory at the absolute address.
   *
   * @param buffer  the off-heap memory of this MemoryChunk
   * @param sequential  whether this MemoryChunk is in sequential mode or not.
   */
  MemoryChunk(final ByteBuffer buffer, final boolean sequential) {
    this(getAddress(buffer), buffer, sequential);
  }

  /**
   * Gets the {@link ByteBuffer} from this MemoryChunk.
   *
   * @return  {@link ByteBuffer}
   */
  public ByteBuffer getBuffer() {
    return buffer;
  }

  /**
   * Gets the remaining number of bytes in the {@link ByteBuffer} of this MemoryChunk.
   * This is supported for sequential MemoryChunk.
   *
   * @return  the number of remaining bytes
   * @throws IllegalAccessException if remaining() not supported by this MemoryChunk.
   */
  public int remaining() throws IllegalAccessException {
    if (sequential) {
      return buffer.remaining();
    } else {
      throw new IllegalAccessException("remaining() only allowed for sequential MemoryChunk");
    }
  }

  /**
   * Gets the current position of the {@link ByteBuffer} of this MemoryChunk.
   *
   * @return the position
   * @throws IllegalAccessException if position() not supported by this MemoryChunk.
   */
  public int position() throws IllegalAccessException {
    if (sequential) {
      return buffer.position();
    } else {
      throw new IllegalAccessException("position() only allowed for sequential MemoryChunk");
    }
  }

  /**
   * Makes the duplicated instance of this MemoryChunk.
   *
   * @return the MemoryChunk with the same content of the caller instance
   */
  public MemoryChunk duplicate() {
    return new MemoryChunk(buffer.duplicate(), sequential);
  }

  /**
   * Frees this MemoryChunk. No further operation possible after calling this method.
   */
  public void free() {
    isFree = true;
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
    if (index >= 0 && pos < addressLimit) {
      return UNSAFE.getByte(pos);
    } else if (isFree) {
      throw new IllegalStateException("MemoryChunk has been freed");
    } else {
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * Reads the byte at the current position of the {@link ByteBuffer}.
   *
   * @return the byte value
   * @throws IllegalAccessException if called by random access mode MemoryChunk.
   */
  public final byte get() throws IllegalAccessException {
    if (!sequential) {
      throw new IllegalAccessException("Not allowed for non-sequential MemoryChunk.");
    } else if (isFree) {
      throw new IllegalStateException("MemoryChunk has been freed");
    }
    return buffer.get();
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
    if (index >= 0 && pos < addressLimit) {
      UNSAFE.putByte(pos, b);
    } else if (isFree) {
      throw new IllegalStateException("MemoryChunk has been freed");
    } else {
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * Writes the given byte into the current position of the {@link ByteBuffer}.
   *
   * @param b the byte value to be written.
   * @throws IllegalAccessException if called by random access mode MemoryChunk.
   */
  public final void put(final byte b) throws IllegalAccessException {
    if (!sequential) {
      throw new IllegalAccessException("Not allowed for non-sequential MemoryChunk.");
    } else if (isFree) {
      throw new IllegalStateException("MemoryChunk has been freed");
    }
    buffer.put(b);
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
   * Copies the data of the MemoryChunk from the current position of the {@link ByteBuffer} to target byte array.
   *
   * @param dst the target byte array to copy the data from MemoryChunk.
   * @throws IllegalAccessException if called by random access mode MemoryChunk.
   */
  public final void get(final byte[] dst) throws IllegalAccessException {
    if (!sequential) {
      throw new IllegalAccessException("Not allowed for non-sequential MemoryChunk.");
    } else if (isFree) {
      throw new IllegalStateException("MemoryChunk has been freed");
    }
    buffer.get(dst);
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
   * Copies all the data from the source byte array into the MemoryChunk
   * beginning at the current position of the {@link ByteBuffer}.
   *
   * @param src the source byte array that holds the data to copy.
   * @throws IllegalAccessException if called by non-sequential MemoryChunk.
   */
  public final void put(final byte[] src) throws IllegalAccessException {
    if (!sequential) {
      throw new IllegalAccessException("Not allowed for non-sequential MemoryChunk.");
    } else if (isFree) {
      throw new IllegalStateException("MemoryChunk has been freed");
    }
    buffer.put(src);
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
    if (index >= 0 && pos <= addressLimit - length) {
      final long arrayAddress = BYTE_ARRAY_BASE_OFFSET + offset;
      UNSAFE.copyMemory(null, pos, dst, arrayAddress, length);
    } else if (isFree) {
      throw new IllegalStateException("MemoryChunk has been freed");
    } else {
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * Bulk get method using the current position of the {@link ByteBuffer}.
   *
   * @param dst the target byte array to copy the data from MemoryChunk.
   * @param offset the offset in the destination byte array.
   * @param length the number of bytes to be copied.
   * @throws IllegalAccessException if called by non-sequential MemoryChunk.
   */
  public final void get(final byte[] dst, final int offset, final int length) throws IllegalAccessException {
    if (!sequential) {
      throw new IllegalAccessException("Not allowed for non-sequential MemoryChunk.");
    } else if (isFree) {
      throw new IllegalStateException("MemoryChunk has been freed");
    }
    buffer.get(dst, offset, length);
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
    if (index >= 0 && pos <= addressLimit - length) {
      final long arrayAddress = BYTE_ARRAY_BASE_OFFSET + offset;
      UNSAFE.copyMemory(src, arrayAddress, null, pos, length);
    } else if (isFree) {
      throw new IllegalStateException("MemoryChunk has been freed");
    } else {
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * Bulk put method using the current position of the {@link ByteBuffer}.
   *
   * @param src the source byte array that holds the data to be copied to MemoryChunk.
   * @param offset  the offset in the source byte array.
   * @param length  the number of bytes to be copied.
   * @throws IllegalAccessException if called by non-sequential MemoryChunk.
   */
  public final void put(final byte[] src, final int offset, final int length) throws IllegalAccessException {
    if (!sequential) {
      throw new IllegalAccessException("Not allowed for non-sequential MemoryChunk");
    } else if (isFree) {
      throw new IllegalStateException("MemoryChunk has been freed");
    }
    buffer.put(src, offset, length);
  }

  /**
   * Reads a char value from the given position.
   *
   * @param index The position from which the memory will be read.
   * @return The char value at the given position.
   *
   * @throws IndexOutOfBoundsException If the index is negative, or larger then the chunk size minus 2.
   */
  @SuppressWarnings("restriction")
  public final char getChar(final int index) {
    final long pos = address + index;
    if (index >= 0 && pos <= addressLimit - 2) {
      if (LITTLE_ENDIAN) {
        return UNSAFE.getChar(pos);
      } else {
        return Character.reverseBytes(UNSAFE.getChar(pos));
      }
    } else if (isFree) {
      throw new IllegalStateException("This MemoryChunk has been freed.");
    } else {
      // index is in fact invalid
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * Reads a char value from the current position of the {@link ByteBuffer}.
   *
   * @return The char value at the current position.
   * @throws IllegalAccessException if called by non-sequential MemoryChunk.
   */
  public final char getChar() throws IllegalAccessException {
    if (!sequential) {
      throw new IllegalAccessException("Not allowed for non-sequential MemoryChunk");
    } else if (isFree) {
      throw new IllegalStateException("This MemoryChunk has been freed");
    }
    return buffer.getChar();
  }

  /**
   * Writes a char value to the given position.
   *
   * @param index The position at which the memory will be written.
   * @param value The char value to be written.
   *
   * @throws IndexOutOfBoundsException If the index is negative, or larger then the chunk size minus 2.
   */
  @SuppressWarnings("restriction")
  public final void putChar(final int index, final char value) {
    final long pos = address + index;
    if (index >= 0 && pos <= addressLimit - 2) {
      if (LITTLE_ENDIAN) {
        UNSAFE.putChar(pos, value);
      } else {
        UNSAFE.putChar(pos, Character.reverseBytes(value));
      }
    } else if (isFree) {
      throw new IllegalStateException("MemoryChunk has been freed");
    } else {
      // index is in fact invalid
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * Writes a char value to the current position of the {@link ByteBuffer}.
   *
   * @param value to be copied to the MemoryChunk.
   * @throws IllegalAccessException if called by non-sequential MemoryChunk.
   */
  public final void putChar(final char value) throws IllegalAccessException {
    if (!sequential) {
      throw new IllegalAccessException("Not allowed for non-sequential MemoryChunk");
    } else if (isFree) {
      throw new IllegalStateException("This MemoryChunk has been freed");
    }
    buffer.putChar(value);
  }

  /**
   * Reads a short integer value from the given position, composing them into a short value
   * according to the current byte order.
   *
   * @param index The position from which the memory will be read.
   * @return The short value at the given position.
   *
   * @throws IndexOutOfBoundsException If the index is negative, or larger then the chunk size minus 2.
   */
  public final short getShort(final int index) {
    final long pos = address + index;
    if (index >= 0 && pos <= addressLimit - 2) {
      if (LITTLE_ENDIAN) {
        return UNSAFE.getShort(pos);
      } else {
        return Short.reverseBytes(UNSAFE.getShort(pos));
      }
    } else if (isFree) {
      throw new IllegalStateException("MemoryChunk has been freed");
    } else {
      // index is in fact invalid
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * Reads a short integer value from the current position of the {@link ByteBuffer}.
   *
   * @return The char value at the current position.
   * @throws IllegalAccessException if called by non-sequential MemoryChunk.
   */
  public final short getShort() throws IllegalAccessException {
    if (!sequential) {
      throw new IllegalAccessException("Not allowed for non-sequential MemoryChunk");
    } else if (isFree) {
      throw new IllegalStateException("This MemoryChunk has been freed");
    }
    return buffer.getShort();
  }

  /**
   * Writes the given short value into this buffer at the given position, using
   * the native byte order of the system.
   *
   * @param index The position at which the value will be written.
   * @param value The short value to be written.
   *
   * @throws IndexOutOfBoundsException If the index is negative, or larger then the chunk size minus 2.
   */
  public final void putShort(final int index, final short value) {
    final long pos = address + index;
    if (index >= 0 && pos <= addressLimit - 2) {
      if (LITTLE_ENDIAN) {
        UNSAFE.putShort(pos, value);
      } else {
        UNSAFE.putShort(pos, Short.reverseBytes(value));
      }
    } else if (isFree) {
      throw new IllegalStateException("MemoryChunk has been freed");
    } else {
      // index is in fact invalid
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * Writes a char value to the current position of the {@link ByteBuffer}.
   *
   * @param value to be copied to the MemoryChunk.
   * @throws IllegalAccessException if called by non-sequential MemoryChunk.
   */
  public final void putShort(final short value) throws IllegalAccessException {
    if (!sequential) {
      throw new IllegalAccessException("Not allowed for non-sequential MemoryChunk");
    } else if (isFree) {
      throw new IllegalStateException("This MemoryChunk has been freed");
    }
    buffer.putShort(value);
  }

  /**
   * Reads an int value from the given position, in the system's native byte order.
   *
   * @param index The position from which the value will be read.
   * @return The int value at the given position.
   *
   * @throws IndexOutOfBoundsException If the index is negative, or larger then the chunk size minus 4.
   */
  public final int getInt(final int index) {
    final long pos = address + index;
    if (index >= 0 && pos <= addressLimit - 4) {
      if (LITTLE_ENDIAN) {
        return UNSAFE.getInt(pos);
      } else {
        return Integer.reverseBytes(UNSAFE.getInt(pos));
      }
    } else if (isFree) {
      throw new IllegalStateException("MemoryChunk has been freed");
    } else {
      // index is in fact invalid
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * Reads an int value from the current position of the {@link ByteBuffer}.
   *
   * @return The char value at the current position.
   * @throws IllegalAccessException if called by non-sequential MemoryChunk.
   */
  public final int getInt() throws IllegalAccessException {
    if (!sequential) {
      throw new IllegalAccessException("Not allowed for non-sequential MemoryChunk");
    } else if (isFree) {
      throw new IllegalStateException("This MemoryChunk has been freed");
    }
    return buffer.getInt();
  }

  /**
   * Writes the given int value to the given position in the system's native byte order.
   *
   * @param index The position at which the value will be written.
   * @param value The int value to be written.
   *
   * @throws IndexOutOfBoundsException If the index is negative, or larger then the chunk size minus 4.
   */
  public final void putInt(final int index, final int value) {
    final long pos = address + index;
    if (index >= 0 && pos <= addressLimit - 4) {
      if (LITTLE_ENDIAN) {
        UNSAFE.putInt(pos, value);
      } else {
        UNSAFE.putInt(pos, Integer.reverseBytes(value));
      }
    } else if (isFree) {
      throw new IllegalStateException("MemoryChunk has been freed");
    } else {
      // index is in fact invalid
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * Writes an int value to the current position of the {@link ByteBuffer}.
   *
   * @param value to be copied to the MemoryChunk.
   * @throws IllegalAccessException if called by non-sequential MemoryChunk.
   */
  public final void putInt(final int value) throws IllegalAccessException {
    if (!sequential) {
      throw new IllegalAccessException("Not allowed for non-sequential MemoryChunk");
    } else if (isFree) {
      throw new IllegalStateException("This MemoryChunk has been freed");
    }
    buffer.putInt(value);
  }

  /**
   * Reads a long value from the given position.
   *
   * @param index The position from which the value will be read.
   * @return The long value at the given position.
   *
   * @throws IndexOutOfBoundsException If the index is negative, or larger then the chunk size minus 8.
   */
  public final long getLong(final int index) {
    final long pos = address + index;
    if (index >= 0 && pos <= addressLimit - 8) {
      if (LITTLE_ENDIAN) {
        return UNSAFE.getLong(pos);
      } else {
        return Long.reverseBytes(UNSAFE.getLong(pos));
      }
    } else if (isFree) {
      throw new IllegalStateException("MemoryChunk has been freed");
    } else {
      // index is in fact invalid
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * Reads a long value from the current position of the {@link ByteBuffer}.
   *
   * @return The char value at the current position.
   * @throws IllegalAccessException if called by non-sequential MemoryChunk.
   */
  public final long getLong() throws IllegalAccessException {
    if (!sequential) {
      throw new IllegalAccessException("Not allowed for non-sequential MemoryChunk");
    } else if (isFree) {
      throw new IllegalStateException("This MemoryChunk has been freed");
    }
    return buffer.getLong();
  }

  /**
   * Writes the given long value to the given position in the system's native byte order.
   *
   * @param index The position at which the value will be written.
   * @param value The long value to be written.
   *
   * @throws IndexOutOfBoundsException If the index is negative, or larger then the chunk size minus 8.
   */
  public final void putLong(final int index, final long value) {
    final long pos = address + index;
    if (index >= 0 && pos <= addressLimit - 8) {
      if (LITTLE_ENDIAN) {
        UNSAFE.putLong(pos, value);
      } else {
        UNSAFE.putLong(pos, Long.reverseBytes(value));
      }
    } else if (address > addressLimit) {
      throw new IllegalStateException("MemoryChunk has been freed");
    } else {
      // index is in fact invalid
      throw new IndexOutOfBoundsException();
    }
  }

  /**
   * Writes a long value to the current position of the {@link ByteBuffer}.
   *
   * @param value to be copied to the MemoryChunk.
   * @throws IllegalAccessException if called by non-sequential MemoryChunk.
   */
  public final void putLong(final long value) throws IllegalAccessException {
    if (!sequential) {
      throw new IllegalAccessException("Not allowed for non-sequential MemoryChunk");
    } else if (isFree) {
      throw new IllegalStateException("This MemoryChunk has been freed");
    }
    buffer.putLong(value);
  }

  /**
   * Reads a float value from the given position, in the system's native byte order.
   *
   * @param index The position from which the value will be read.
   * @return The float value at the given position.
   *
   * @throws IndexOutOfBoundsException If the index is negative, or larger then the chunk size minus 4.
   */
  public final float getFloat(final int index) {
    return Float.intBitsToFloat(getInt(index));
  }

  /**
   * Reads a float value from the current position of the {@link ByteBuffer}.
   *
   * @return The char value at the current position.
   * @throws IllegalAccessException if called by non-sequential MemoryChunk.
   */
  public final float getFloat() throws IllegalAccessException {
    if (!sequential) {
      throw new IllegalAccessException("Not allowed for non-sequential MemoryChunk");
    } else if (isFree) {
      throw new IllegalStateException("This MemoryChunk has been freed");
    }
    return buffer.getFloat();
  }

  /**
   * Writes the given float value to the given position in the system's native byte order.
   *
   * @param index The position at which the value will be written.
   * @param value The float value to be written.
   *
   * @throws IndexOutOfBoundsException If the index is negative, or larger then the chunk size minus 4.
   */
  public final void putFloat(final int index, final float value) {
    putInt(index, Float.floatToRawIntBits(value));
  }

  /**
   * Writes a float value to the current position of the {@link ByteBuffer}.
   *
   * @param value to be copied to the MemoryChunk.
   * @throws IllegalAccessException if called by non-sequential MemoryChunk.
   */
  public final void putFloat(final float value) throws IllegalAccessException {
    if (!sequential) {
      throw new IllegalAccessException("Not allowed for non-sequential MemoryChunk");
    } else if (isFree) {
      throw new IllegalStateException("This MemoryChunk has been freed");
    }
    buffer.putFloat(value);
  }

  /**
   * Reads a double value from the given position, in the system's native byte order.
   *
   * @param index The position from which the value will be read.
   * @return The double value at the given position.
   *
   * @throws IndexOutOfBoundsException If the index is negative, or larger then the chunk size minus 8.
   */
  public final double getDouble(final int index) {
    return Double.longBitsToDouble(getLong(index));
  }

  /**
   * Reads a double value from the current position of the {@link ByteBuffer}.
   *
   * @return The char value at the current position.
   * @throws IllegalAccessException if called by non-sequential MemoryChunk.
   */
  public final double getDouble() throws IllegalAccessException {
    if (!sequential) {
      throw new IllegalAccessException("Not allowed for non-sequential MemoryChunk");
    } else if (isFree) {
      throw new IllegalStateException("This MemoryChunk has been freed");
    }
    return buffer.getDouble();
  }

  /**
   * Writes the given double value to the given position in the system's native byte order.
   *
   * @param index The position at which the memory will be written.
   * @param value The double value to be written.
   *
   * @throws IndexOutOfBoundsException If the index is negative, or larger then the chunk size minus 8.
   */
  public final void putDouble(final int index, final double value) {
    putLong(index, Double.doubleToRawLongBits(value));
  }

  /**
   * Writes a double value to the current position of the {@link ByteBuffer}.
   *
   * @param value to be copied to the MemoryChunk.
   * @throws IllegalAccessException if called by non-sequential MemoryChunk.
   */
  public final void putDouble(final double value) throws IllegalAccessException {
    if (!sequential) {
      throw new IllegalAccessException("Not allowed for non-sequential MemoryChunk");
    } else if (isFree) {
      throw new IllegalStateException("This MemoryChunk has been freed");
    }
    buffer.putDouble(value);
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
