package org.apache.drill.exec.store.ipfs;

import io.ipfs.api.IPFS;
import io.ipfs.api.MerkleNode;
import io.ipfs.multiaddr.MultiAddress;
import io.ipfs.multihash.Multihash;
import org.apache.drill.common.exceptions.UserException;
import org.bouncycastle.util.Strings;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;



public class IPFSHelper {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IPFSHelper.class);

  public static final String IPFS_NULL_OBJECT_HASH = "QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n";
  public static final Multihash IPFS_NULL_OBJECT = Multihash.fromBase58(IPFS_NULL_OBJECT_HASH);

  private ExecutorService executorService;
  private IPFS client;
  private Multihash myID;
  private List<MultiAddress> myAddrs;
  private IPFSPeer myself;
  private int maxPeersPerLeaf;
  private int timeout;

  public IPFSHelper(IPFS ipfs, ExecutorService executorService) {
    this(ipfs);
    this.executorService = executorService;
  }

  public IPFSHelper(IPFS ipfs) {
    executorService = Executors.newSingleThreadExecutor();
    this.client = ipfs;
    try {
      Map res = client.id();
      myID = Multihash.fromBase58((String)res.get("ID"));
      myAddrs = ((List<String>) res.get("Addresses"))
          .stream()
          .map(addr -> new MultiAddress(addr))
          .collect(Collectors.toList());
      myself = new IPFSPeer(this, myID);
    } catch (IOException e) {
      //TODO handle exception
      myID = null;
      myAddrs = Collections.emptyList();
      myself = null;
    }
  }

  public void setExecutorService(ExecutorService executorService) {
    this.executorService = executorService;
  }

  public void setTimeout(int timeout) {
    this.timeout = timeout;
  }

  public void setMaxPeersPerLeaf(int maxPeersPerLeaf) {
    this.maxPeersPerLeaf = maxPeersPerLeaf;
  }

  public IPFS getClient() {
    return client;
  }

  public Multihash getMyID() {
    return myID;
  }

  public IPFSPeer getSelf() {
    return myself;
  }

  public List<Multihash> findprovsTimeout(Multihash id) throws IOException {
    List<String> providers;
    if (executorService != null) {
      providers = client.dht.findprovsListTimeout(id, maxPeersPerLeaf, timeout, executorService);
    } else {
      throw new NullPointerException("executor is null");
    }

    List<Multihash> ret = providers.stream().map(str -> Multihash.fromBase58(str)).collect(Collectors.toList());
    return ret;
  }

  public List<MultiAddress> findpeerTimeout(Multihash peerId) throws IOException {
    if(peerId.equals(myID)) {
      return myAddrs;
    }

    List<String> addrs;
    if (executorService != null) {
      addrs = client.dht.findpeerListTimeout(peerId, timeout, executorService);
    } else {
      throw new NullPointerException("executor is null");
    }
    List<MultiAddress>
        ret = addrs
        .stream()
        .filter(addr -> !addr.equals(""))
        .map(str -> new MultiAddress(str)).collect(Collectors.toList());
    return ret;
  }

  @FunctionalInterface
  public interface ThrowingFunction<T, R, E extends Exception>{
    R apply(final T in) throws E;
  }

  public <T, R, E extends Exception> R timed(ThrowingFunction<T, R, E> op, T in, int timeout) throws TimeoutException, E {
    Callable<R> task = () -> op.apply(in);
    Future<R> res = executorService.submit(task);
    try {
      return res.get(timeout, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      throw (E) e.getCause();
    } catch (CancellationException | InterruptedException e) {
      throw UserException.executionError(e).build(logger);
    }
  }

  public <T, R, E extends Exception> R timedFailure(ThrowingFunction<T, R, E> op, T in, int timeout) throws E {
    Callable<R> task = () -> op.apply(in);
    Future<R> res = executorService.submit(task);
    try {
      return res.get(timeout, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      throw (E) e.getCause();
    } catch (TimeoutException e) {
      throw UserException.executionError(e).message("IPFS operation timed out").build(logger);
    } catch (CancellationException | InterruptedException e) {
      throw UserException.executionError(e).build(logger);
    }
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
    try {
      return getPeerData(peerId, "drill-ready").isPresent();
    } catch (RuntimeException e) {
      return false;
    }
  }

  public Optional<Multihash> getIPNSDataHash(Multihash peerId) {
    Optional<List<MerkleNode>> links = getPeerLinks(peerId);
    if (!links.isPresent()) {
      return Optional.empty();
    }

    return links.get().stream()
        .filter(l -> l.name.equals(Optional.of("drill-data")))
        .findFirst()
        .map(l -> l.hash);
  }


  private Optional<byte[]> getPeerData(Multihash peerId, String key) {
    Optional<List<MerkleNode>> links = getPeerLinks(peerId);
    if (!links.isPresent()) {
      return Optional.empty();
    }

    return links.get().stream()
        .filter(l -> l.name.equals(Optional.of(key)))
        .findFirst()
        .map(l -> {
          try {
            return client.object.data(l.hash);
          } catch (IOException e) {
            return null;
          }
        });
  }

  private Optional<List<MerkleNode>> getPeerLinks(Multihash peerId) {
    try {
      Optional<String> optionalPath = client.name.resolve(peerId, 30);
      if (!optionalPath.isPresent()) {
        return Optional.empty();
      }
      String path = optionalPath.get().substring(6); // path starts with /ipfs/Qm...

      List<MerkleNode> links = client.object.get(Multihash.fromBase58(path)).links;
      if (links.size() < 1) {
        return Optional.empty();
      } else {
        return Optional.of(links);
      }
    } catch (IOException e) {
      return Optional.empty();
    }
  }
}
