package org.apache.drill.exec.store.ipfs;

import com.google.common.collect.Lists;
import io.ipfs.api.IPFS;
import io.ipfs.multihash.Multihash;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.complex.fn.JsonReader;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import com.google.common.base.Charsets;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class IPFSRecordReader extends AbstractRecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IPFSRecordReader.class);
  private FragmentContext context;
  private IPFSStoragePlugin plugin;
  private IPFSSubScan.IPFSSubScanSpec subScanSpec;
  private VectorContainerWriter writer;
  private JsonReader jsonReader;
  private Iterator<JsonNode> jsonNodeIterator;

  public IPFSRecordReader(FragmentContext context, IPFSStoragePlugin plugin, IPFSSubScan.IPFSSubScanSpec scanSpec, List<SchemaPath> columns) {
    this.context = context;
    this.plugin = plugin;
    this.subScanSpec = scanSpec;
    setColumns(columns);

  }

  @Override
  public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
    logger.debug("IPFSRecordReader setup, query {}", subScanSpec.getTargetHash().toString());
    this.writer = new VectorContainerWriter(output);
    JsonReader.Builder builder = new JsonReader.Builder(context.getManagedBuffer());
    this.jsonReader = builder.schemaPathColumns(Lists.newArrayList(getColumns()))
                             .allTextMode(true)
                             .enableNanInf(true)
                             .skipOuterList(true)
                             .build();
    Multihash rootHash = subScanSpec.getTargetHash();
    IPFS client = plugin.getIPFSClient();
    String rootJson;
    byte[] rawDataBytes;
    try {
      rawDataBytes = client.object.data(rootHash);
    }
    catch (IOException e) {
      throw new ExecutionSetupException(e);
    }
    rootJson = new String(rawDataBytes);
    ObjectMapper mapper = new ObjectMapper();
    try {
      JsonNode rootJsonNode = mapper.readTree(rootJson);
      this.jsonNodeIterator = rootJsonNode.getElements();
    }
    catch (IOException e) {
      throw new ExecutionSetupException(e);
    }


  }


  @Override
  public int next() {
    logger.debug("IPFSRecordReader next");
    if (jsonNodeIterator == null || !jsonNodeIterator.hasNext()) {
      return 0;
    }
    writer.allocate();
    writer.reset();
    int docCount = 0;
    try {
      while (docCount < BaseValueVector.INITIAL_VALUE_ALLOCATION && jsonNodeIterator.hasNext()) {
        JsonNode node = jsonNodeIterator.next();
        jsonReader.setSource(node.toString().getBytes(Charsets.UTF_8));
        writer.setPosition(docCount);
        jsonReader.write(writer);
        docCount ++;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    writer.setValueCount(docCount);
    return docCount;
  }

  @Override
  public void close() {
    logger.debug("IPFSRecordReader close");
  }
}
