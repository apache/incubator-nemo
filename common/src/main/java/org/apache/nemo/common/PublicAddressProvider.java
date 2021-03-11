package org.apache.nemo.common;

import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.wake.remote.address.LocalAddressProvider;

import javax.inject.Inject;

public class PublicAddressProvider implements LocalAddressProvider {

  @Inject
  private PublicAddressProvider() {

  }

  @Override
  public String getLocalAddress() {
    return NetworkUtils.getPublicIP();
  }

  @Override
  public Configuration getConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bind(LocalAddressProvider.class, PublicAddressProvider.class)
        .build();
  }
}
