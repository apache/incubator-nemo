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
package edu.snu.nemo.common.exception;

/**
 * JsonParseException.
 * Thrown when the cause for the json parsing failure.
 */
public final class JsonParseException extends RuntimeException {
  /**
   * JsonParseException.
   * @param cause cause
   */
  public JsonParseException(final Throwable cause) {
    super(cause);
  }
}
