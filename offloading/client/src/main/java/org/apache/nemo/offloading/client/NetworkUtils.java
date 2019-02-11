package org.apache.nemo.offloading.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.*;

public class NetworkUtils {
  private static final Logger LOG = LoggerFactory.getLogger(NetworkUtils.class.getName());


  public static String getPublicIP() {
    final URL whatismyip;
    try {
      whatismyip = new URL("http://checkip.amazonaws.com");
    } catch (MalformedURLException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    final BufferedReader in;
    try {
      in = new BufferedReader(new InputStreamReader(
        whatismyip.openStream()));
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    String ip = null; //you get the IP as a String
    try {
      ip = in.readLine();
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    return ip;
  }

  public static InetAddress getLocalHostLANAddress() throws UnknownHostException {
    try {
      InetAddress candidateAddress = null;
      // Iterate all NICs (network interface cards)...

//      for (Enumeration ifaces = NetworkInterface.getNetworkInterfaces(); ifaces.hasMoreElements();) {
//        NetworkInterface iface = (NetworkInterface) ifaces.nextElement();
//        // Iterate all IP addresses assigned to each card...
//        for (Enumeration inetAddrs = iface.getInetAddresses(); inetAddrs.hasMoreElements();) {
//          InetAddress inetAddr = (InetAddress) inetAddrs.nextElement();
//          LOG.info("Addr: {}, isLoopback: {}, isSiteLocal: {}, isMCGlobal: {}, isMCLinkLocal: {}," +
//              "isLinkLocal: {}, isMCNodeLocal: {}, isMCSiteLocal: {}, isMultiCastAddr: {}, isReachable: {}",
//            inetAddr.getHostAddress(), inetAddr.isLoopbackAddress(), inetAddr.isSiteLocalAddress(),
//            inetAddr.isMCGlobal(), inetAddr.isMCLinkLocal(), inetAddr.isLinkLocalAddress(),
//            inetAddr.isMCNodeLocal(), inetAddr.isMCSiteLocal(), inetAddr.isMulticastAddress(),
//            inetAddr.isReachable(1000));
//
//          /*
//          if (!inetAddr.isLoopbackAddress()) {
//
//            if (inetAddr.isSiteLocalAddress()) {
//              // Found non-loopback site-local address. Return it immediately...
//              return inetAddr;
//            }
//            else if (candidateAddress == null) {
//              // Found non-loopback address, but not necessarily site-local.
//              // Store it as a candidate to be returned if site-local address is not subsequently found...
//              candidateAddress = inetAddr;
//              // Note that we don't repeatedly assign non-loopback non-site-local addresses as candidates,
//              // only the first. For subsequent iterations, candidate will be non-null.
//            }
//          }
//          */
//        }
//      }
//

      if (candidateAddress != null) {
        // We did not find a site-local address, but we found some other non-loopback address.
        // Server might have a non-site-local address assigned to its NIC (or it might be running
        // IPv6 which deprecates the "site-local" concept).
        // Return this non-loopback candidate address...
        return candidateAddress;
      }
      // At this point, we did not find a non-loopback address.
      // Fall back to returning whatever InetAddress.getLocalHost() returns...
      InetAddress jdkSuppliedAddress = InetAddress.getLocalHost();
      if (jdkSuppliedAddress == null) {
        throw new UnknownHostException("The JDK InetAddress.getLocalHost() method unexpectedly returned null.");
      }
      return jdkSuppliedAddress;
    }
    catch (Exception e) {
      UnknownHostException unknownHostException = new UnknownHostException("Failed to determine LAN address: " + e);
      unknownHostException.initCause(e);
      throw unknownHostException;
    }
  }
}
