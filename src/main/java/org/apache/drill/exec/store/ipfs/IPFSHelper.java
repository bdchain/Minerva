package org.apache.drill.exec.store.ipfs;

import io.ipfs.api.IPFS;
import io.ipfs.api.MerkleNode;
import io.ipfs.multiaddr.MultiAddress;
import io.ipfs.multihash.Multihash;

import java.io.IOException;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.bouncycastle.util.Strings;



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

  public static Optional<String> pickPeerHost(List<MultiAddress> peerAddrs) {
    String localAddr = null;
    for (MultiAddress addr : peerAddrs) {
      String host = addr.getHost();
      try {
        InetAddress inetAddress = InetAddress.getByName(host);
        if (inetAddress.isLoopbackAddress()) {
          continue;
        }
        if (inetAddress.isSiteLocalAddress() || inetAddress.isLinkLocalAddress()) {
          //FIXME we don't know which local address can be reached; maybe check with InetAddress.isReachable?
          localAddr = host;
        } else {
          return Optional.of(host);
        }
      } catch (UnknownHostException e) {
        continue;
      }
    }

    return Optional.ofNullable(localAddr);
  }

  public Optional<String> getPeerDrillHostname(Multihash peerId) {
    return getPeerData(peerId, "drill-hostname").map(Strings::fromByteArray);
  }

  public boolean isDrillReady(Multihash peerId) {
    return getPeerData(peerId, "drill-ready").isPresent();
  }

  private Optional<byte[]> getPeerData(Multihash peerId, String key) {
    try {
      Optional<String> optionalPath = client.name.resolve(peerId, 30);
      if (!optionalPath.isPresent()) {
        return Optional.empty();
      }
      String path = optionalPath.get().substring(6); // path starts with /ipfs/Qm...

      List<MerkleNode> links = client.object.get(Multihash.fromBase58(path)).links;

      return links.stream()
          .filter(l -> l.name.equals(Optional.of(key)))
          .findFirst()
          .map(l -> {
            try {
              return client.object.data(l.hash);
            } catch (IOException e) {
              return null;
            }
          });
    } catch (IOException e) {
      return Optional.empty();
    }
  }
}
