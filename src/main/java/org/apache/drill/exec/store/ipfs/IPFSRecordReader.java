package org.apache.drill.exec.store.ipfs;

import io.ipfs.api.IPFS;
import io.ipfs.multihash.Multihash;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.easy.json.JSONRecordReader;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.List;

public class IPFSRecordReader extends AbstractRecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IPFSRecordReader.class);
  private FragmentContext context;
  private IPFSStoragePlugin plugin;
  private String subScanSpec;
  private JSONRecordReader jsonRecordReader;
  private List<SchemaPath> columnList;

  public IPFSRecordReader(FragmentContext context, IPFSStoragePlugin plugin, String scanSpec, List<SchemaPath> columns) {
    this.context = context;
    this.plugin = plugin;
    this.subScanSpec = scanSpec;
    this.columnList = columns;
    setColumns(columns);

  }

  @Override
  public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
    logger.debug("IPFSRecordReader setup, query {}", subScanSpec);
    Multihash rootHash = Multihash.fromBase58(subScanSpec);
    logger.debug("I am RecordReader {}", plugin.getContext().getEndpoint());
    logger.debug("rootHash={}", rootHash);
    IPFS client = plugin.getIPFSClient();
    String rootJson;
    byte[] rawDataBytes;
    try {
      rawDataBytes = client.object.data(rootHash);
    }
    catch (IOException e) {
      throw new ExecutionSetupException(e);
    }
    if (subScanSpec.equals(IPFSHelper.IPFS_NULL_OBJECT)) {
      // An empty ipfs object, but an empty string will make Jackson ObjectMapper fail
      // so treat it specially
      rootJson = "[]";
    }
    ObjectMapper mapper = new ObjectMapper();
    JsonNode rootJsonNode;
    try {
      rootJsonNode = mapper.readTree(rawDataBytes);
    }
    catch (IOException e) {
      throw new ExecutionSetupException(e);
    }
    jsonRecordReader = new JSONRecordReader(this.context, rootJsonNode, (DrillFileSystem) null, columnList);
    jsonRecordReader.setup(context, output);


  }


  @Override
  public int next() {
    logger.debug("I am IPFSRecordReader {} calling next", plugin.getContext().getEndpoint());
    return jsonRecordReader.next();
  }

  @Override
  public void close() {
    try {
      jsonRecordReader.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
    logger.debug("IPFSRecordReader close");
  }
}
