package org.apache.nemo.runtime.master;

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
      "172.31.16.202",
      "172.31.17.96",
      "172.31.18.133",
      "172.31.25.250",
      "172.31.21.223",
      "172.31.24.148",
      "172.31.21.179",
      "172.31.29.181",
      "172.31.16.30",
      "172.31.17.224", // 10
      "172.31.22.215",
      "172.31.29.211",
      "172.31.25.112",
      "172.31.22.223",
      "172.31.25.243", // 15
      "172.31.23.66",
      "172.31.31.168",
      "172.31.29.140",
      "172.31.28.86",
      "172.31.22.90" // 20





    );

  public static final List<String> INSTANCE_IDS =
    Arrays.asList(
      "i-0707e910d42ab99fb",
      "i-081f578c165a41a7a",
      "i-0d346bd15aed1a33f",
      "i-0756c588bf6b60a71",
      "i-09355b96aac481c5d", // 5
      "i-00fa3a41c31e9e4f6",
      "i-018f1381fea30978b",
      "i-02997163366d56887",
      "i-0483222f102563cd2",
      "i-091dfd4055e08ede5", // 10
      "i-09a34576e7a5d420e",
      "i-0a0eccbb4733c4aef",
      "i-0c282c626dc07d2a5",
      "i-0ee85d5aa94ebaf77",
      "i-0ff0dcc1a2e5fed4e", // 15
      "i-0af9fc4d951abcce0",
      "i-0e4d18164fd428e67",
      "i-0e769647960c94036",
      "i-03203eb727c3508ab",
      "i-06376c4e4c42bfde8" // 20
    );
}
