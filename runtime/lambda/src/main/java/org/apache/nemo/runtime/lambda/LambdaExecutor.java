package org.apache.nemo.runtime.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import java.util.Base64;
import java.util.Map;

public class LambdaExecutor  implements RequestHandler<Map<String,String>, Context> {
  @Override
  public Context handleRequest(Map<String, String>input, Context context) {
    System.out.println("Handle event");
    System.out.println(input.get("d"));
    byte [] byteEncoded = Base64.getDecoder().decode(input.get("d"));
    System.out.println(byteEncoded.toString());
    return null;
  }
}
