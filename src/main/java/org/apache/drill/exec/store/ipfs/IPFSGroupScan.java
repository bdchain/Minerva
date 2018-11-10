package org.apache.drill.exec.store.ipfs;


import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.ipfs.api.MerkleNode;
import io.ipfs.multihash.Multihash;
import io.ipfs.multiaddr.MultiAddress;
import io.ipfs.api.IPFS;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.ipfs.IPFSSubScan.IPFSSubScanSpec;
import org.apache.drill.exec.store.schedule.AffinityCreator;
import org.apache.drill.exec.store.schedule.AssignmentCreator;
import org.apache.drill.exec.store.schedule.EndpointByteMap;
import org.apache.drill.exec.store.schedule.EndpointByteMapImpl;
import org.apache.drill.exec.store.schedule.CompleteWork;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Collection;
import java.util.ArrayList;
import java.util.stream.Collectors;

@JsonTypeName("ipfs-scan")
public class IPFSGroupScan extends AbstractGroupScan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IPFSGroupScan.class);
  private IPFSStoragePlugin ipfsStoragePlugin;
  private IPFSScanSpec ipfsScanSpec;
  private List<SchemaPath> columns;

  private static long DEFAULT_NODE_SIZE = 1000l;

  private static int MAX_IPFS_NODES = 1;
  private static int IPFS_TIMEOUT = 5;

  private ListMultimap<Integer, IPFSWork> assignments;
  private List<IPFSWork> ipfsWorkList = Lists.newArrayList();
  private List<EndpointAffinity> affinities;

  @JsonCreator
  public IPFSGroupScan(@JsonProperty("ipfsScanSpec") IPFSScanSpec ipfsScanSpec,
                       @JsonProperty("ipfsStoragePluginConfig") IPFSStoragePluginConfig ipfsStoragePluginConfig,
                       @JsonProperty("columns") List<SchemaPath> columns,
                       @JacksonInject StoragePluginRegistry pluginRegistry) throws IOException, ExecutionSetupException {
    this((IPFSStoragePlugin) pluginRegistry.getPlugin(ipfsStoragePluginConfig), ipfsScanSpec, columns);
  }

  public IPFSGroupScan(IPFSStoragePlugin ipfsStoragePlugin, IPFSScanSpec ipfsScanSpec, List<SchemaPath> columns) {
    super((String) null);
    this.ipfsStoragePlugin = ipfsStoragePlugin;
    this.ipfsScanSpec = ipfsScanSpec;
    logger.debug("GroupScan constructor called with columns {}", columns);
    this.columns = columns == null || columns.size() == 0? ALL_COLUMNS : columns;
    init();
  }
  /*
   * Picks out only 10.0.0.0/8 range address
   */
  private String pickPeerHost(List<MultiAddress> peerAddrs) {
    for (MultiAddress addr : peerAddrs) {
      //if (addr.isTCPIP()) {
        if(addr.getHost().startsWith("10.")) {
          return addr.getHost();
        }
      //}
    }
    return null;
  }

  private void init() {
    Multihash topHash = ipfsScanSpec.getTargetHash();
    //FIXME we here assume the topHash is available on foreman, which is _this_ endpoint
    //need revise this assumption

    Collection<DrillbitEndpoint> endpoints = ipfsStoragePlugin.getContext().getBits();
    Map<String,DrillbitEndpoint> endpointMap = Maps.newHashMap();
    for (DrillbitEndpoint endpoint : endpoints) {
      endpointMap.put(endpoint.getAddress(), endpoint);
    }
    IPFS ipfs = ipfsStoragePlugin.getIPFSClient();
    IPFSHelper ipfsHelper = new IPFSHelper(ipfs);
    try {
      // split topHash into several child leaves
      MerkleNode topNode = ipfs.object.links(topHash);
      List<Multihash> leaves = Collections.emptyList();
      if(topNode.links.size() > 0) {
        // this is a meta node containing leaf hashes
        logger.debug("{} is a meta node", topHash);
        leaves = topNode.links.stream().map(x -> x.hash).collect(Collectors.toList());
        //FIXME do something useful with leaf size, e.g. hint Drill about operation costs
      }
      else {
        //this is a simple node directly owing data
        logger.debug("{} is a simple node", topHash);
        leaves = new ArrayList<>();
        leaves.add(topHash);
      }
      logger.debug("Iterating on {} leaves...", leaves.size());
      for(Multihash leaf : leaves) {
        IPFSWork work = new IPFSWork(leaf);
        List<Multihash> providers = ipfsHelper.findprovsTimeout(leaf, MAX_IPFS_NODES, IPFS_TIMEOUT);
        logger.debug("Got {} providers for {} from IPFS", providers.size(), leaf);
        for(Multihash provider : providers) {
          List<MultiAddress> peerAddrs = null;
          try {
            peerAddrs = ipfsHelper.findpeerTimeout(provider, IPFS_TIMEOUT);
          }
          catch (Exception e) {
            continue;
          }
          String peerHost = pickPeerHost(peerAddrs);
          logger.debug("Got peer host {} for leaf {}", peerHost, leaf);
          DrillbitEndpoint ep = endpointMap.get(peerHost);
          if(ep != null) {
            logger.debug("added endpoint {} to work", ep);
            work.getByteMap().add(ep, DEFAULT_NODE_SIZE);
          }

        }
        ipfsWorkList.add(work);
      }

      logger.debug("init1: ipfsWorkList.size() = {}", ipfsWorkList.size());

    }catch (Exception e) {
      logger.debug("exception in init");
      throw new RuntimeException(e);
    }

    logger.debug("init2: ipfsWorkList.size() = {}", ipfsWorkList.size());
  }

  private IPFSGroupScan(IPFSGroupScan that) {
    super(that);
    this.ipfsStoragePlugin = that.ipfsStoragePlugin;
    this.ipfsScanSpec = that.ipfsScanSpec;
    this.assignments = that.assignments;
    this.ipfsWorkList = that.ipfsWorkList;
    this.columns = that.columns;
  }

  @JsonProperty
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @JsonIgnore
  public IPFSStoragePlugin getStoragePlugin() {
    return ipfsStoragePlugin;
  }

  @JsonProperty
  public IPFSScanSpec getIPFSScanSpec() {
    return ipfsScanSpec;
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    if (affinities == null) {
      affinities = AffinityCreator.getAffinityMap(ipfsWorkList);
    }
    return affinities;
  }

  @Override
  public int getMaxParallelizationWidth() {
    //FIXME replace 0 with something more meaningful
    return 0;
    /*return ipfsWorkList.size();*/
  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> incomingEndpoints) {
    logger.debug("ipfsWorkList.size() = {}", ipfsWorkList.size());

    assignments = AssignmentCreator.getMappings(incomingEndpoints, ipfsWorkList);
    logger.debug("assignments keys:" + assignments.keys().toString());
  }

  @Override
  public IPFSSubScan getSpecificScan(int minorFragmentId) {
    logger.debug(String.format("getSpecificScan: minorFragmentId = %d", minorFragmentId));
    List<IPFSWork> workList = assignments.get(minorFragmentId);
    logger.debug("workList == null: " + (workList == null? "true": "false"));
    logger.debug(String.format("workList.size(): %d", workList.size()));

    List<IPFSSubScanSpec> scanSpecList = Lists.newArrayList();

    for (IPFSWork work : workList) {
      scanSpecList.add(new IPFSSubScanSpec(work.getPartialRootHash()));
    }

    return new IPFSSubScan(ipfsStoragePlugin, scanSpecList, columns);
  }

  @Override
  public ScanStats getScanStats() {
    //FIXME why 100000 * size?
    long recordCount = 100000 * ipfsWorkList.size();
    return new ScanStats(ScanStats.GroupScanProperty.NO_EXACT_ROW_COUNT, recordCount, 1, recordCount);
  }

  @Override
  public IPFSGroupScan clone(List<SchemaPath> columns){
    logger.debug("IPFSGroupScan clone {}", columns);
    IPFSGroupScan cloned = new IPFSGroupScan(this);
    cloned.columns = columns;
    return cloned;
  }

  @Override
  @JsonIgnore
  public boolean canPushdownProjects(List<SchemaPath> columns) {
    //FIXME what does this mean?
    return true;
  }

  @Override
  @JsonIgnore
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    logger.debug("getNewWithChildren called");
    return new IPFSGroupScan(this);
  }

  @JsonIgnore
  public Multihash getTargetHash() {
    return getIPFSScanSpec().getTargetHash();
  }


  @Override
  public String getDigest() {
    return toString();
  }

  @Override
  public String toString() {
    return "IPFSGroupScan [IPFSScanSpec=" + ipfsScanSpec + ", columns=" + columns + "]";
  }

  private class IPFSWork implements CompleteWork {
    private EndpointByteMapImpl byteMap = new EndpointByteMapImpl();
    private Multihash partialRoot;


    public IPFSWork(Multihash root) {
      this.partialRoot = root;
    }

    public Multihash getPartialRootHash() {return partialRoot;}

    @Override
    public long getTotalBytes() {
      return DEFAULT_NODE_SIZE;
    }

    @Override
    public EndpointByteMap getByteMap() {
      return byteMap;
    }

    @Override
    public int compareTo(CompleteWork o) {
      return 0;
    }

    @Override
    public String toString() {
      return "IPFSWork [root = " + partialRoot.toString() + "]";
    }
  }
}
