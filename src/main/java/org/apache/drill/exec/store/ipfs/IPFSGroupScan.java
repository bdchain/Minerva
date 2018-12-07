package org.apache.drill.exec.store.ipfs;


import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
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
import org.apache.drill.exec.store.schedule.AffinityCreator;
import org.apache.drill.exec.store.schedule.EndpointByteMap;
import org.apache.drill.exec.store.schedule.EndpointByteMapImpl;
import org.apache.drill.exec.store.schedule.CompleteWork;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
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

  private Map<Integer, List<IPFSWork>> assignments;
  private List<IPFSWork> ipfsWorkList = Lists.newArrayList();
  private Map<String, List<IPFSWork>> endpointWorksMap;
  private List<EndpointAffinity> affinities;

  private static Map<String, String> staticIPEndpointMap = ImmutableMap.of(
    "172.17.16.4", "qcloud2",
    "172.17.16.5", "qcloud3",
    "172.17.16.12", "qcloud1"
    );

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

  private void init() {
    Multihash topHash = Multihash.fromBase58(ipfsScanSpec.getTargetHash());

    Collection<DrillbitEndpoint> endpoints = ipfsStoragePlugin.getContext().getBits();
    Map<String,DrillbitEndpoint> endpointMap = Maps.newHashMap();
    for (DrillbitEndpoint endpoint : endpoints) {
      endpointMap.put(endpoint.getAddress(), endpoint);
    }
    IPFS ipfs = ipfsStoragePlugin.getIPFSClient();
    IPFSHelper ipfsHelper = new IPFSHelper(ipfs);
    endpointWorksMap = new HashMap<>();

    try {
      // split topHash into several child leaves
      MerkleNode topNode = ipfs.object.links(topHash);
      List<Multihash> leaves = Collections.emptyList();
      //FIXME make this recursively expand all meta nodes until all nodes in leaves are simple nodes
      if(topNode.links.size() > 0) {
        // this is a meta node containing leaf hashes
        logger.debug("{} is a meta node", topHash);
        leaves = topNode.links.stream().map(x -> x.hash).collect(Collectors.toList());
        //FIXME do something useful with leaf size, e.g. hint Drill about operation costs
      }
      else {
        //this is a simple node directly owning data
        logger.debug("{} is a simple node", topHash);
        leaves = new ArrayList<>();
        leaves.add(topHash);
      }
      logger.debug("Iterating on {} leaves...", leaves.size());
      for(Multihash leaf : leaves) {
        IPFSWork work = new IPFSWork(leaf.toBase58());
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
          String peerHost = IPFSHelper.pickPeerHost(peerAddrs);
          logger.debug("Got peer host {} for leaf {}", peerHost, leaf);
          DrillbitEndpoint ep = endpointMap.get(staticIPEndpointMap.get(peerHost));
          if(ep != null) {
            logger.debug("added endpoint {} to work {}", ep, work);
            work.getByteMap().add(ep, DEFAULT_NODE_SIZE);
            work.setOnEndpoint(ep);

            if(endpointWorksMap.containsKey(ep.getAddress())) {
              endpointWorksMap.get(ep.getAddress()).add(work);
            } else {
              List<IPFSWork> ipfsWorks = Lists.newArrayList();
              ipfsWorks.add(work);
              endpointWorksMap.put(ep.getAddress(), ipfsWorks);
            }
          } else {
            //TODO what if no corresponding endpoint is available in the cluster?
            //doing nothing will simply cause data to be missing in the result set
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
    this.endpointWorksMap = that.endpointWorksMap;
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
    DrillbitEndpoint myself = ipfsStoragePlugin.getContext().getEndpoint();
    int width;
    if (endpointWorksMap.containsKey(myself.getAddress())) {
      // the foreman is also going to be a minor fragment worker under a UnionExchange operator
      width = endpointWorksMap.size();
    } else {
      // the foreman does not hold data, so we have to force parallelization
      // to make sure there is a UnionExchange operator
      width = endpointWorksMap.size() + 1;
    }
    logger.debug("getMaxParallelizationWidth: {}", width);
    return width;
  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> incomingEndpoints) {
    logger.debug("ipfsWorkList.size() = {}", ipfsWorkList.size());
    logger.debug("endpointWorksMap: {}", endpointWorksMap);
    assignments = new HashMap<>();
    for (int fragmentId=0; fragmentId < incomingEndpoints.size(); fragmentId++) {
      DrillbitEndpoint ep = incomingEndpoints.get(fragmentId);
      logger.debug("processing ep {}", ep.getAddress());
      if (endpointWorksMap.containsKey(ep.getAddress())) {
        // if this ep has works, add them to the corresponding fragment
        assignments.put(fragmentId, endpointWorksMap.get(ep.getAddress()));
        logger.debug("assigned fragement {} to endpoint {}", fragmentId, ep.getAddress());
      } else {
        // add a dummy work to this fragment, e.g. when the foreman does not have data, just
        // make it read an empty object
        assignments.put(fragmentId, Lists.newArrayList(new IPFSWork(IPFSHelper.IPFS_NULL_OBJECT)));
        logger.debug("assigned dummy work to endpoint {}", ep.getAddress());
      }
    }
  }

  @Override
  public IPFSSubScan getSpecificScan(int minorFragmentId) {
    logger.debug(String.format("getSpecificScan: minorFragmentId = %d", minorFragmentId));
    List<IPFSWork> workList = assignments.get(minorFragmentId);
    logger.debug("workList == null: " + (workList == null? "true": "false"));
    logger.debug(String.format("workList.size(): %d", workList.size()));

    List<String> scanSpecList = Lists.newArrayList();

    for (IPFSWork work : workList) {
      scanSpecList.add(work.getPartialRootHash());
    }

    return new IPFSSubScan(ipfsStoragePlugin, scanSpecList, columns);
  }

  @Override
  public ScanStats getScanStats() {
    //FIXME why 100000 * size?
    long recordCount = 100001 * endpointWorksMap.size();
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
  public String getTargetHash() {
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
    private String partialRoot;
    private DrillbitEndpoint onEndpoint = null;


    public IPFSWork(String root) {
      this.partialRoot = root;
    }

    public String getPartialRootHash() {return partialRoot;}

    public void setOnEndpoint(DrillbitEndpoint endpointAddress) {
      this.onEndpoint = endpointAddress;
    }

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
