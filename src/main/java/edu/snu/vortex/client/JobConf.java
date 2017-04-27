package edu.snu.vortex.client;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.OptionalParameter;
import org.apache.reef.tang.formats.RequiredParameter;

/**
 * Vortex Job Configurations.
 */
public final class JobConf extends ConfigurationModuleBuilder {

  //////////////////////////////// User Configurations

  /**
   * User Main Class Name.
   */
  @NamedParameter(doc = "User Main Class Name", short_name = "user_main")
  public final class UserMainClass implements Name<String> {
  }

  /**
   * User Main Arguments.
   */
  @NamedParameter(doc = "User Main Arguments", short_name = "user_args")
  public final class UserMainArguments implements Name<String> {
  }

  /**
   * Directory to store JSON representation of intermediate DAGs.
   */
  @NamedParameter(doc = "Directory to store intermediate DAGs", short_name = "dag_dir", default_value = "./target/dag")
  public final class DAGDirectory implements Name<String> {
  }

  static final RequiredParameter<String> USER_MAIN_CLASS = new RequiredParameter<>();
  static final RequiredParameter<String> USER_MAIN_ARGS = new RequiredParameter<>();
  static final OptionalParameter<String> DAG_DIRECTORY = new OptionalParameter<>();

  //////////////////////////////// Compiler Configurations

  /**
   * Name of the optimization policy.
   */
  @NamedParameter(doc = "Name of the optimization policy", short_name = "optimization_policy", default_value = "none")
  public final class OptimizationPolicy implements Name<String> {
  }

  static final OptionalParameter<String> OPTIMIZATION_POLICY = new OptionalParameter<>();

  //////////////////////////////// Runtime Configurations



  //////////////////////////////// Configuration Module

  public static final ConfigurationModule CONF = new JobConf()
      .bindNamedParameter(UserMainClass.class, USER_MAIN_CLASS)
      .bindNamedParameter(UserMainArguments.class, USER_MAIN_ARGS)
      .bindNamedParameter(DAGDirectory.class, DAG_DIRECTORY)
      .bindNamedParameter(OptimizationPolicy.class, OPTIMIZATION_POLICY)
      .build();
}
