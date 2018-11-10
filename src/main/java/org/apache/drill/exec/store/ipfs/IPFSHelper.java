package org.apache.drill.exec.store.ipfs;

import io.ipfs.api.IPFS;
import io.ipfs.multiaddr.MultiAddress;
import io.ipfs.multihash.Multihash;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


public class IPFSHelper {
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

  public List<Multihash> findprovsTimeout(Multihash id, int maxPeers, int timeout) {
    FutureTask<List<String>> task = new FutureTask<>(new Callable<List<String>>() {
      @Override
      public List<String> call() throws Exception {
        return client.dht.findprovsList(id, maxPeers);
      }
    });
    executorService.execute(task);

    List<String> providers;
    try {
      providers = task.get(timeout, TimeUnit.SECONDS);

    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      task.cancel(true);
      providers = Collections.emptyList();
    }

    List<Multihash> ret = providers.stream().map(str -> Multihash.fromBase58(str)).collect(Collectors.toList());
    return ret;
  }

  public List<MultiAddress> findpeerTimeout(Multihash peerId, int timeout) {
    if(peerId.equals(myID)) {
      return myAddrs;
    }
    FutureTask<List<String>> task = new FutureTask<>(new Callable<List<String>>() {
      @Override
      public List<String> call() throws Exception {
        return client.dht.findpeerList(peerId);
      }
    });
    executorService.execute(task);

    List<String> addrs;
    try {
      addrs = task.get(timeout, TimeUnit.SECONDS);

    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      task.cancel(true);
      addrs = Collections.emptyList();
    }

    List<MultiAddress> ret = addrs.stream().map(str -> new MultiAddress(str)).collect(Collectors.toList());
    return ret;
  }
}
