package org.apache.drill.exec.store.ipfs;

import io.ipfs.api.IPFS;

public class IPFSContext {
  private IPFS ipfsClient;
  private IPFSStoragePluginConfig storagePluginConfig;
  private IPFSStoragePlugin storagePlugin;

  public IPFSContext(IPFSStoragePluginConfig config, IPFSStoragePlugin plugin, IPFS client) {
    this.ipfsClient = client;
    this.storagePlugin = plugin;
    this.storagePluginConfig = config;
  }


  public IPFS getIPFSClient() {
    return ipfsClient;
  }

  public IPFSHelper getIPFSHelper() {
    return new IPFSHelper(ipfsClient);
  }

  public IPFSStoragePlugin getStoragePlugin() {
    return storagePlugin;
  }

  public IPFSStoragePluginConfig getStoragePluginConfig() {
    return storagePluginConfig;
  }

}

