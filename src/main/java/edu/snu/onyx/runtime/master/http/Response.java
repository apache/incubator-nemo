/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.onyx.runtime.master.http;

/**
 * Encapsulates the response that the Onyx Master sends back to the HTTP client.
 * It includes the status code and message.
 */
public final class Response {
  /**
   * 200 OK : The request succeeded normally.
   */
  private static final int SC_OK = 200;

  /**
   * 400 BAD REQUEST : The request is syntactically incorrect.
   */
  private static final int SC_BAD_REQUEST = 400;

  /**
   * 403 FORBIDDEN : Syntactically okay but refused to process.
   */
  private static final int SC_FORBIDDEN = 403;

  /**
   * 404 NOT FOUND :  The resource is not available.
   */
  private static final int SC_NOT_FOUND = 404;


  /**
   * @param message the message to include in the response.
   * @return a response with OK status.
   */
  public static Response ok(final String message) {
    return new Response(SC_OK, message);
  }

  /**
   * @param message the message to include in the response.
   * @return a response with BAD_REQUEST status.
   */
  public static Response badRequest(final String message) {
    return new Response(SC_BAD_REQUEST, message);
  }

  /**
   * @param message the message to include in the response.
   * @return a response with FORBIDDEN status.
   */
  public static Response forbidden(final String message) {
    return new Response(SC_FORBIDDEN, message);
  }

  /**
   * @param message the message to include in the response.
   * @return a response with NOT FOUND status.
   */
  public static Response notFound(final String message) {
    return new Response(SC_NOT_FOUND, message);
  }

  /**
   * @return {@code true} if the response is OK.
   */
  public boolean isOK() {
    return this.status == SC_OK;
  }

  /**
   * Status code of the request based on RFC 2068.
   */
  private int status;

  /**
   * Message to send.
   */
  private String message;

  /**
   * @param status the status code
   * @param message the message to include in the response.
   */
  private Response(final int status, final String message) {
    this.status = status;
    this.message = message;
  }

  /**
   * @return the status code of this response.
   */
  int getStatus() {
    return status;
  }

  /**
   * @return the message of this response.
   */
  String getMessage() {
    return message;
  }
}
