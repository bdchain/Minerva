package org.apache.drill.exec.store.ipfs;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableSet;
import io.ipfs.multihash.Multihash;
import org.apache.drill.common.exceptions.ExecutionSetupException;
/*import org.apache.drill.common.expression.SchemaPath;*/
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.store.StoragePluginRegistry;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

@JsonTypeName("ipfs-sub-scan")
public class IPFSSubScan extends AbstractBase implements SubScan {
  private static int IPFS_SUB_SCAN_VALUE = 19155;
  private final IPFSStoragePlugin ipfsStoragePlugin;
  private final List<IPFSSubScanSpec> ipfsSubScanSpecList;
  private final List<SchemaPath> columns;


  @JsonCreator
  public IPFSSubScan(@JacksonInject StoragePluginRegistry registry,
                     @JsonProperty("ipfsStoragePluginConfig") IPFSStoragePluginConfig ipfsStoragePluginConfig,
                     @JsonProperty("ipfsSubScanSpecList") LinkedList<IPFSSubScanSpec> ipfsSubScanSpecList,
                     @JsonProperty("columns") List<SchemaPath> columns
                     ) throws ExecutionSetupException {
    super((String) null);
    ipfsStoragePlugin = (IPFSStoragePlugin) registry.getPlugin(ipfsStoragePluginConfig);
    this.ipfsSubScanSpecList = ipfsSubScanSpecList;
    this.columns = columns;
  }

  public IPFSSubScan(IPFSStoragePlugin ipfsStoragePlugin, List<IPFSSubScanSpec> ipfsSubScanSpecList, List<SchemaPath> columns) {
    super((String) null);
    this.ipfsStoragePlugin = ipfsStoragePlugin;
    this.ipfsSubScanSpecList = ipfsSubScanSpecList;
    this.columns = columns;
  }

  @JsonIgnore
  public IPFSStoragePlugin getIPFSStoragePlugin() {
    return ipfsStoragePlugin;
  }

  public IPFSStoragePluginConfig getIPFSStoragePluginConfig() {
    return ipfsStoragePlugin.getConfig();
  }

  public List<SchemaPath> getColumns() {
    return columns;
  }

  public List<IPFSSubScanSpec> getIPFSSubScanSpecList() {
    return ipfsSubScanSpecList;
  }

  @Override
  public <T, X, E extends Throwable> T accept(
      PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return ImmutableSet.<PhysicalOperator>of().iterator();
  }

  @Override
  public int getOperatorType() {
    return IPFS_SUB_SCAN_VALUE;
  }

  @Override
  public boolean isExecutable() {
    return false;
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    return new IPFSSubScan(ipfsStoragePlugin, ipfsSubScanSpecList, columns);
  }

  public static class IPFSSubScanSpec {
    private final Multihash targetHash;

    @JsonCreator
    public IPFSSubScanSpec(@JsonProperty("targetHash") String targetHash) {
      this.targetHash = Multihash.fromBase58(targetHash);
    }

    public IPFSSubScanSpec(Multihash targetHash) {
      this.targetHash = targetHash;
    }

    public Multihash getTargetHash() {
      return targetHash;
    }
  }
}
