package org.apache.drill.exec.store.ipfs;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import io.ipfs.multihash.Multihash;
import org.apache.drill.common.exceptions.UserException;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IPFSScanSpec {
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
          throw new InvalidParameterException("Unsupported prefix: " + what);
      }
    }
  }

  public enum Format {
    JSON("json"), CSV("csv");

    @JsonProperty("format")
    private String name;
    Format(String prefix) {
      this.name = prefix;
    }

    @Override
    public String toString() {
      return this.name;
    }

    public static Format of(String what) {
      switch (what) {
        case "json" :
          return JSON;
        case "csv":
          return CSV;
        default:
          throw new InvalidParameterException("Unsupported prefix: " + what);
      }
    }
  }

  public static Set<String> formats = ImmutableSet.of("json", "csv");
  private Prefix prefix;
  private String path;
  private Format formatExtension;

  @JsonCreator
  public IPFSScanSpec (@JsonProperty("path") String path) {
    //FIXME: IPFS hashes are actually Base58 encoded, so "0" "O" "I" "l" are not valid
    //also CIDs can be encoded with different encodings, not necessarily Base58
    Pattern tableNamePattern = Pattern.compile("^/(ipfs|ipns)/([A-Za-z0-9]{46}(/[^#]+)*)#(\\w+)$");
    Matcher matcher = tableNamePattern.matcher(path);
    if (!matcher.matches()) {
      throw UserException.validationError().message("Invalid IPFS path in query string. Use paths of pattern `^/(ipfs|ipns)/([A-Za-z0-9]{46}(/[^#]+)*)#(\\w+)$`").build();
    } else {
      String prefix = matcher.group(1);
      String hashPath = matcher.group(2);
      String formatExtension = matcher.group(4);

      this.path = hashPath;
      this.prefix = Prefix.of(prefix);
      this.formatExtension = Format.of(formatExtension);
    }
  }

  @JsonProperty
  public Multihash getTargetHash(IPFSHelper helper) {
    try {
      Map<String, String> result = (Map<String, String>) helper.getClient().resolve(prefix.toString(), path, true);
      String topHashString;
      if (result.containsKey("Path")) {
        topHashString = result.get("Path");
      } else {
        throw UserException.validationError().message("Non-existent IPFS path: {}", toString()).build(IPFSHelper.logger);
      }
      topHashString = result.get("Path");
      // returns in form of /ipfs/Qma...
      Multihash topHash = Multihash.fromBase58(topHashString.split("/")[2]);
      return topHash;
    } catch (IOException e) {
      throw UserException.executionError(e).message("Unable to resolve IPFS path; is it a valid IPFS path?").build(IPFSHelper.logger);
    }
  }

  @JsonProperty
  public Prefix getPrefix() {
    return prefix;
  }

  @JsonProperty
  public Format getFormatExtension() {
    return formatExtension;
  }

  @Override
  public String toString() {
    return "IPFSScanSpec [/" + prefix + "/" + path + "#" + formatExtension + " ]";
  }
}
