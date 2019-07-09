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
package org.apache.nemo.common;

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

  /**
   * Creates a new memory chunk that represents the off-heap memory at the absolute address.
   * Note that this class uses UNSAFE to directly manipulate the data in the {@link ByteBuffer} by the address,
   * which means that the position, limit, capacity, etc of {@link ByteBuffer} is not tracked automatically.
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
    this.isFree = false;
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
    return buffer;
  }

  /**
   * Returns the off-heap memory address of this MemoryChunk.
   *
   * @return absolute memory address outside the heap
   */
  public long getAddress() {
    return address;
  }

//  /**
//   * Gets the remaining number of bytes in the {@link ByteBuffer} of this MemoryChunk.
//   *
//   * @return  the number of remaining bytes
//   */
//  public int remaining() {
//    return buffer.remaining();
//  }

//  /**
//   * Gets the current position of the {@link ByteBuffer} of this MemoryChunk.
//   *
//   * @return the position
//   */
//  public int position() {
//    return buffer.position();
//  }

  /**
   * Makes the duplicated instance of this MemoryChunk.
   *
   * @return the MemoryChunk with the same content of the caller instance
   */
  public MemoryChunk duplicate() {
    return new MemoryChunk(buffer.duplicate());
  }

  /**
   * Frees this MemoryChunk. No further operation possible.
   */
  public void free() {
    isFree = true;
  }

  /**
   * Checks whether the MemoryChunk was freed.
   *
   * @return true, if the MemoryChunk has been freed, false otherwise.
   */
  public boolean isFree() {
    return isFree;
  }

  /**
   * Reads the byte at the given index.
   *
   * @param index from which the byte will be read
   * @return the byte at the given position
   */
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
   * Writes the given byte into this buffer at the given index.
   *
   * @param index The position at which the byte will be written.
   * @param b     The byte value to be written.
   */
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
   * Copies the data of the MemoryChunk from the specified position to target byte array.
   *
   * @param index The position at which the first byte will be read.
   * @param dst The memory into which the memory will be copied.
   */
  public final void get(final int index, final byte[] dst) {
    get(index, dst, 0, dst.length);
  }

  /**
   * Copies all the data from the source memory into the MemoryChunk
   * beginning at the specified position.
   *
   * @param index
   * @param src
   */
  public final void put(final int index, final byte[] src) {
    put(index, src, 0, src.length);
  }

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
   * Reads an int value from the given position, in the system's native byte order.
   * This method offers the best speed for integer reading and should be used
   * unless a specific byte order is required. In most cases, it suffices to know that the
   * byte order in which the value is written is the same as the one in which it is read
   * (such as transient storage in memory, or serialization for I/O and network), making this
   * method the preferable choice.
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
   * Writes the given int value to the given position in the system's native
   * byte order. This method offers the best speed for integer writing and should be used
   * unless a specific byte order is required. In most cases, it suffices to know that the
   * byte order in which the value is written is the same as the one in which it is read
   * (such as transient storage in memory, or serialization for I/O and network), making this
   * method the preferable choice.
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
