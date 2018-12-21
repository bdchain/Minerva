package org.apache.drill.exec.store.ipfs;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.security.InvalidParameterException;

public class IPFSScanSpec {
  private String targetHash;
  public enum Prefix {
    IPFS("ipfs"), IPNS("ipns");

    private String name;
    Prefix(String prefix) {
      this.name = prefix;
    }

    @Override
    public String toString() {
      return this.name;
    }

    public static Prefix of(String what) {
      switch (what) {
        case "ipfs" :
          return IPFS;
        case "ipns":
          return IPNS;
        default:
          throw new InvalidParameterException("Prefix not allowed: " + what);
      }
    }
  }
  private Prefix prefix;

  @JsonCreator
  public IPFSScanSpec (@JsonProperty("targetHash") String targetHash,
                       @JsonProperty("name") Prefix prefix) {
    this.targetHash = targetHash;
    this.prefix = prefix;
  }

  @JsonProperty
  public String getTargetHash() {
    return targetHash;
  }

  @JsonProperty
  public Prefix getPrefix() {
    return prefix;
  }

  @Override
  public String toString() {
    return "IPFSScanSpec [table=/" + prefix + "/" + targetHash + " ]";
  }
}
