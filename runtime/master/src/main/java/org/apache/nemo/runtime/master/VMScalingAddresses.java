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
      "172.31.22.90", // 20
      "172.31.20.192",
      "172.31.24.109",
      "172.31.26.88",
      "172.31.16.40",
      "172.31.30.72", // 25
      "172.31.19.84",
      "172.31.27.199",
      "172.31.30.57",
      "172.31.21.109",
      "172.31.29.194", // 30
      "172.31.25.108",
      "172.31.26.192",
      "172.31.17.105",
      "172.31.26.99",
      "172.31.31.159", // 35
      "172.31.28.117",
      "172.31.16.136",
      "172.31.24.132",
      "172.31.20.157",
      "172.31.30.56", // 40
      "172.31.18.73",
      "172.31.29.73",
      "172.31.21.112",
      "172.31.17.237",
      "172.31.27.7", // 45
      "172.31.30.245",
      "172.31.23.105",
      "172.31.19.110",
      "172.31.25.88",
      "172.31.23.122" // 50
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
      "i-06376c4e4c42bfde8", // 20
      "i-0877e3782718a93c2",
      "i-08b140dfa6f06d74b",
      "i-0191031cf835f8c3e",
      "i-033d1bec152315821",
      "i-04a5021e6b610a95f", // 25
      "i-06fa13b487a107682",
      "i-0875e09843b83d423",
      "i-0910a737050d4a8e1",
      "i-0a209f83e0e3db31e",
      "i-0c0f4dd2bd5a8474a", // 30
      "i-0c835f88a0e5dc409",
      "i-0dd4034a3a43dc01a",
      "i-0e0a17593e089eb4d",
      "i-0e72cc4fea275918a",
      "i-0f0e52706b93442b4", // 35
      "i-0feb7bd46286dcad1",
      "i-0feb7bd46286dcad1",
      "i-01f910168d0778d95",
      "i-024cb99390579b98a",
      "i-02921d8a251252a32",
      "i-036ae17718c6179da", // 40
      "i-0420b4f3176b5456a",
      "i-06b1aa07582dfbaf7",
      "i-06e4640597622907f",
      "i-073c1d8cd05f1cab1",
      "i-0757cee352acdc2d1", // 45
      "i-0a8356da0a56ea0dd",
      "i-0cb85eb4966bca7e0",
      "i-0e7d879666ae2ec27",
      "i-0f45c0232b81e9a81",
      "i-0f55f264204b63aa6" // 50
      );
}
