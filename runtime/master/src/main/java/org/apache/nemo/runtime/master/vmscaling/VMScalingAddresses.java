package org.apache.nemo.runtime.master.vmscaling;

import org.apache.nemo.conf.EvalConf;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.*;

public class VMScalingAddresses {
  private static final Logger LOG = LoggerFactory.getLogger(VMScalingAddresses.class.getName());

  public final List<String> vmAddresses;
  public final List<String> vmIds;

  @Inject
  private VMScalingAddresses(@Parameter(EvalConf.VMAddresses.class) final String text) {
    final String[] lines = text.split("\n");
    this.vmAddresses = new ArrayList<>(lines.length);
    this.vmIds = new ArrayList<>(lines.length);

    for (int i = 0; i < lines.length; i++) {
      final String[] addressAndId = lines[i].split(",");
      vmAddresses.add(addressAndId[0]);
      vmIds.add(addressAndId[1]);
    }

    LOG.info("vm addresses: {}", vmAddresses);
    LOG.info("vm ids: {}", vmIds);

    check();
  }

  private List<String> findDuplicateAddress(final Set<String> s, final List<String> l) {
    final List<String> dupL = new ArrayList<>(l);

    for (final String ss : s) {
      dupL.remove(ss);
    }

    return dupL;
  }

  private void check() {
    final Set<String> addressSet = new HashSet<>(vmAddresses);

    if (addressSet.size() != vmAddresses.size()) {
      throw new RuntimeException("duplicate address! " + findDuplicateAddress(addressSet, vmAddresses));
    }

    final Set<String> idSet = new HashSet<>(vmIds);

    if (idSet.size() != vmIds.size()) {
      throw new RuntimeException("duplicate id! " + findDuplicateAddress(idSet, vmIds));
    }
  }

//  public static final List<String> VM_ADDRESSES =
//    Arrays.asList(
//      "172.31.16.202",
//      "172.31.2.186",
//      "172.31.13.187",
//      "172.31.3.48",
//      "172.31.3.113", // 5
//      "172.31.13.141",
//      "172.31.5.110",
//      "172.31.13.9",
//      "172.31.4.203",
//      "172.31.12.135", // 10
//      "172.31.1.233",
//      "172.31.0.98",
//      "172.31.4.38",
//      "172.31.3.33",
//      "172.31.9.161", // 15
//      "172.31.4.69",
//      "172.31.4.18",
//      "172.31.11.43",
//      "172.31.1.23",
//      "172.31.1.145"); // 20
//
//  public static final List<String> INSTANCE_IDS =
//    Arrays.asList(
//      "i-0a0681abc25631732",
//      "i-0355631fa32d1bf6e",
//      "i-05cfa803958240451",
//      "i-0539a2f7a75bfa280",
//      "i-09ebb0d8dcaa2e904", // 5
//      "i-0a1615145a3a5a7d3",
//      "i-0cacdab34cd857c4d",
//      "i-0e91bb164a5d19ace",
//      "i-0e86392c8840ba048",
//      "i-0179b604fcfb54f73", // 10
//      "i-073095cb72d981ebf",
//      "i-0b0133bb14b42cea8",
//      "i-0431b4057de1ace6c",
//      "i-09aaf2462e0ba1981",
//      "i-09e106210c8fb40a3",  // 15
//      "i-09856752c48d6240f",
//      "i-097a4ce80c02587fd",
//      "i-0cbabd6b58da213b5",
//      "i-01ae46c47e9cb385a",
//      "i-0c4dd20932b88b46a" // 20
//    );
}
