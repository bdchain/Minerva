package org.apache.drill.exec.store.ipfs;

import io.ipfs.api.IPFS;
import io.ipfs.multiaddr.MultiAddress;
import io.ipfs.multihash.Multihash;

import java.io.IOException;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.commons.net.util.SubnetUtils;


public class IPFSHelper {

  public static String IPFS_NULL_OBJECT = "QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n";

  private ExecutorService executorService;
  private IPFS client;
  private Multihash myID;
  private List<MultiAddress> myAddrs;

  public IPFSHelper(IPFS ipfs) {
    executorService = Executors.newCachedThreadPool();
    this.client = ipfs;
    try {
      Map res = client.id();
      myID = Multihash.fromBase58((String)res.get("ID"));
      myAddrs = ((List<String>) res.get("Addresses"))
          .stream()
          .map(addr -> new MultiAddress(addr))
          .collect(Collectors.toList());

    } catch (IOException e) {
      //TODO handle exception
      myID = null;
      myAddrs = Collections.emptyList();
    }
  }

  public List<Multihash> findprovsTimeout(Multihash id, int maxPeers, int timeout) throws IOException {
    List<String> providers;
    providers = client.dht.findprovsListTimeout(id, maxPeers, timeout);

    List<Multihash> ret = providers.stream().map(str -> Multihash.fromBase58(str)).collect(Collectors.toList());
    return ret;
  }

  public List<MultiAddress> findpeerTimeout(Multihash peerId, int timeout) throws IOException {
    if(peerId.equals(myID)) {
      return myAddrs;
    }

    List<String> addrs = client.dht.findpeerListTimeout(peerId, timeout);

    List<MultiAddress>
        ret = addrs
        .stream()
        .filter(addr -> !addr.equals(""))
        .map(str -> new MultiAddress(str)).collect(Collectors.toList());
    return ret;
  }

  public static String pickPeerHost(List<MultiAddress> peerAddrs) {
    SubnetUtils subnet1 = new SubnetUtils("10.0.0.0/8");
    SubnetUtils subnet2 = new SubnetUtils("172.16.0.0/12");
    for (MultiAddress addr : peerAddrs) {
      String host = addr.getHost();
      if(subnet1.getInfo().isInRange(host) || subnet2.getInfo().isInRange(host)) {
        return addr.getHost();
      }
    }
    return null;
  }
}
