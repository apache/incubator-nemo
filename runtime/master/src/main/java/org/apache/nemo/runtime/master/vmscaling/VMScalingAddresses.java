package org.apache.nemo.runtime.master.vmscaling;

import java.util.*;

public class VMScalingAddresses {

  public static List<String> findDuplicateAddress(final Set<String> s, final List<String> l) {
    final List<String> dupL = new ArrayList<>(l);

    for (final String ss : s) {
      dupL.remove(ss);
    }

    return dupL;
  }

  public static void check() {
    final Set<String> addressSet = new HashSet<>(VM_ADDRESSES);

    if (addressSet.size() != VM_ADDRESSES.size()) {
      throw new RuntimeException("duplicate address! " + findDuplicateAddress(addressSet, VM_ADDRESSES));
    }

    final Set<String> idSet = new HashSet<>(INSTANCE_IDS);

    if (idSet.size() != INSTANCE_IDS.size()) {
      throw new RuntimeException("duplicate id! " + findDuplicateAddress(idSet, INSTANCE_IDS));
    }
  }

  public static final List<String> VM_ADDRESSES =
    Arrays.asList(
      "172.31.16.202");

  public static final List<String> INSTANCE_IDS =
    Arrays.asList(
      "i-0a0681abc25631732"
    );
}
