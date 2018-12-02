package org.apache.drill.exec.store.ipfs;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class IPFSScanSpec {
  private String targetHash;

  @JsonCreator
  public IPFSScanSpec (@JsonProperty("targetHash") String targetHash) {
    this.targetHash = targetHash;
  }

  @JsonProperty
  public String getTargetHash() {
    return targetHash;
  }

  @Override
  public String toString() {
    return "IPFSScanSpec [table=" + targetHash + " ]";
  }
}
