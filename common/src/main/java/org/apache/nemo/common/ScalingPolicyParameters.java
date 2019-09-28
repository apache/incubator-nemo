package org.apache.nemo.common;

public final class ScalingPolicyParameters {

  // lambda vCPU
  public static int LAMBDA_VCPU = 1;
  public static int VM_VCPU = 4;

  // lambda cpu processing time 보정값. 이 값으로 나눠줌. lambda cpu가 1.5배 성능 안좋다는 소리.
  public static double LAMBDA_CPU_PROC_TIME_RATIO = 1.5;

  // vm과 lambda cpu load 차이. core 갯수 * 보정값
  public static double VM_LAMBDA_CPU_LOAD_RATIO =  (VM_VCPU / LAMBDA_VCPU) * LAMBDA_CPU_PROC_TIME_RATIO;

  public static double RELAY_OVERHEAD = 15;

  public static double CPU_HIGH_THRESHOLD = 0.8;

  public static double CPU_LOW_THRESHOLD = 0.6;

  public static int CONSECUTIVE = 3;

  public static int SLACK_TIME = 25;
}
