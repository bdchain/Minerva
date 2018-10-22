package org.apache.drill.exec.store.ipfs;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.ipfs.multihash.Multihash;

public class IPFSScanSpec {
  private Multihash targetHash;

  @JsonCreator
  public IPFSScanSpec(@JsonProperty("targetHash") String targetHash)  {
    this.targetHash = Multihash.fromBase58(targetHash);
  }

  public IPFSScanSpec (Multihash targetHash) {
    this.targetHash = targetHash;
  }

  public Multihash getTargetHash() {
    return targetHash;
  }

  @Override
  public String toString() {
    return "IPFSScanSpec [table=" + targetHash + " ]";
  }
}
