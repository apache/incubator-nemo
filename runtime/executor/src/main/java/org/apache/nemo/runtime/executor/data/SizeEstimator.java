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
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A class to estimate the size of Objects.
 * necessary for implementing caching and determining memory status
 */
public final class SizeEstimator {
  private static final Logger LOG = LoggerFactory.getLogger(SizeEstimator.class.getName());
  private SizeEstimator() {
    // not called
  }


  // Sizes of primitive types
  private static final int BOOLEAN_SIZE = 1;
  private static final int CHAR_SIZE = 2;
  private static final int SHORT_SIZE = 2;
  private static final int INT_SIZE = 4;
  private static final int LONG_SIZE = 8;
  private static final int FLOAT_SIZE = 4;
  private static final int BYTE_SIZE = 1;
  private static final int DOUBLE_SIZE = 8;
  private static final int ALIGN_SIZE = 8;
  private static boolean is64Bit = true;
  private static int objectSize = 16; // 12 bytes with 8 byte offset
  private static int pointerSize = 8;


  // cache of classInfos
  private static WeakHashMap<Class<?>, ClassInfo> classInfos = new WeakHashMap<Class<?>, ClassInfo>();


  public static void initialize() {
    String architecture = System.getProperty("os.arch");
    is64Bit = architecture.contains("64") || architecture.contains("s390x");
    objectSize = is64Bit ? 16 : 8;
    pointerSize = is64Bit ? 8 : 4;
    classInfos.clear();
  }

  public static long estimate(final Object obj) {
    return estimate(obj, new IdentityHashMap());
  }

  public static long estimate(final Object obj, final IdentityHashMap map) {
    SearchState state = new SearchState(map);
    state.enqueue(obj);
    while (!state.isFinished()) {
      visitSingleObject(state.dequeue(), state);
    }
    return state.size;
  }


  // Estimate the size of arrays larger than ARRAY_SIZE_FOR_SAMPLING by sampling.
  private static final int ARRAY_SIZE_FOR_SAMPLING = 400;
  private static final int ARRAY_SAMPLE_SIZE = 100; // should be lower than ARRAY_SIZE_FOR_SAMPLING

  private static void visitArray(final Object array, final Class<?> cls, final SearchState state) {
    Class<?> elementClass = cls.getComponentType();
    long length = Array.getLength(array);
    long arrSize = alignSize(objectSize + INT_SIZE); // 24 with 16 byte object size after alignment
    if (elementClass.isPrimitive()) {
      arrSize += alignSize(length * getPrimitiveSize(elementClass));
      state.size += arrSize;
    } else {
      arrSize += alignSize(length * pointerSize);
      state.size += arrSize;
      if (length <= ARRAY_SIZE_FOR_SAMPLING) {
        int arrayIndex = 0;
        while (arrayIndex < length) {
          Object selected = Array.get(array, arrayIndex);
          state.enqueue(selected);
          arrayIndex += 1;
        }
      } else {
        // get the size of a large array by sampling
        double sampledSize = 0.0;
        Random rand = new Random(42);
        Set<Integer> chosen = new HashSet<Integer>(ARRAY_SAMPLE_SIZE);
        int index = 0;
        for (int i = 0; i < ARRAY_SAMPLE_SIZE; i++) {
          index = rand.nextInt((int) length);
          while (chosen.contains(index)) {
            index = rand.nextInt((int) length);
          }
          chosen.add(index);
          Object element = Array.get(array, index); // randomly sampled element
          sampledSize += SizeEstimator.estimate(element, state.visited);
        }
        state.size += Double.valueOf(((length / (ARRAY_SAMPLE_SIZE * 1.0)) * sampledSize)).longValue();
      }
    }
  }


  private static void visitSingleObject(final Object obj, final SearchState state) {
    Class<?> cls = obj.getClass();
    if (cls.isArray()) {
      visitArray(obj, cls, state);
    } else if (!cls.getName().startsWith("java.lang.reflect")
      && !(obj instanceof ClassLoader || obj instanceof  Class)) {
      ClassInfo classInfo = getClassInfo(cls);
      state.size += classInfo.shellSize;
      for (Field field : classInfo.pointerFields) {
        try {
          state.enqueue(field.get(obj));
        } catch (IllegalArgumentException e) {
          // pass fields that can't be accessed with field.get(obj)
          continue;
        } catch (IllegalAccessException e) {
          // pass fields that can't be accessed with field.get(obj)
          continue;
        }
      }
    }
  }

  /**
   * A class to represent what state the search is currently in.
   */
  private static class SearchState {
    private IdentityHashMap visited;
    private long size = 0L;
    private Stack<Object> stack;

    SearchState(final IdentityHashMap map) {
      this.visited = map;
      this.stack = new Stack<>();
    }


    void enqueue(final Object obj) {
      if (obj != null && !visited.containsKey(obj)) {
        visited.put(obj, null);
        stack.add(obj);
      }
    }

    boolean isFinished() {
      return stack.isEmpty();
    }

    Object dequeue() {
      return stack.pop();
    }
  } // SearchState

  private static long getPrimitiveSize(final Class<?> cls) {
    if (cls == byte.class) {
      return BYTE_SIZE;
    } else if (cls == boolean.class) {
      return BOOLEAN_SIZE;
    } else if (cls == char.class) {
      return CHAR_SIZE;
    } else if (cls == short.class) {
      return SHORT_SIZE;
    } else if (cls == int.class) {
      return INT_SIZE;
    } else if (cls == long.class) {
      return LONG_SIZE;
    } else if (cls == float.class) {
      return FLOAT_SIZE;
    } else if (cls == double.class) {
      return DOUBLE_SIZE;
    } else {
      throw new IllegalArgumentException(
        "Non-primitive class " + cls + " passed to primitiveSize()");
    }
  }

  /**
   * A class for ClassInfo, which contains the class overhead size and members the class has.
   */
  private static class ClassInfo {
    ClassInfo(final long shellSize, final List pointerFields) {
      this.shellSize = shellSize;
      this.pointerFields = pointerFields;
    }
    private long shellSize;
    private List<Field> pointerFields;
  }

  /**
   * Get the cached info or compute the ClassInfo for a given class.
   * @return the computed classInfo
   */
  private static ClassInfo getClassInfo(final Class<?> cls) {
    // base case
    if (cls == Object.class) {
      ClassInfo info = new ClassInfo(8L, new ArrayList<Field>());
      classInfos.put(cls, info);
      return info;
    }
    ClassInfo info = classInfos.get(cls);
    if (info != null) {
      return info;
    }
    Class<?> superClass = cls.getSuperclass();
    ClassInfo parent = getClassInfo(superClass);
    long shellSize = parent.shellSize;
    List<Field> pointerFields = parent.pointerFields;

    // iterate through the fields of this class and gather information.
    for (Field field : cls.getDeclaredFields()) {
      if (!java.lang.reflect.Modifier.isStatic(field.getModifiers())) {
        Class<?> fieldClass = field.getType();
        // handle primitive members
        if (fieldClass.isPrimitive()) {
          shellSize += getPrimitiveSize(fieldClass);
        } else { // handle non-primitive references
            field.setAccessible(true); // Enable future get()'s on this field
            shellSize += pointerSize;
            pointerFields.add(field);
        }
      }
    }
    // cache the newly computed ClassInfo
    shellSize = alignSize(shellSize);
    ClassInfo newInfo = new ClassInfo(shellSize, pointerFields);
    classInfos.put(cls, newInfo);
    return newInfo;
  }

  private static long alignSize(final long size) {
    return (size + ALIGN_SIZE - 1) & ~(ALIGN_SIZE - 1);
  }
}






















