package org.apache.drill.exec.store.ipfs;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import io.ipfs.api.IPFS;
import io.ipfs.api.MerkleNode;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.store.JSONOutputRecordWriter;
import org.apache.drill.exec.store.RecordWriter;
import org.apache.drill.exec.vector.complex.fn.ExtendedJsonOutput;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;

public class IPFSRecordWriter extends JSONOutputRecordWriter implements RecordWriter {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IPFSRecordWriter.class);
  private static final String LINE_FEED = String.format("%n");
  private static final int MAX_RECORDS_PER_LEAF = 1024;

  private IPFS ipfs;
  private final JsonFactory factory = new JsonFactory();
  private final String name;
  private ByteArrayOutputStream stream;
  private boolean fRecordStarted = false;

  public IPFSRecordWriter(OperatorContext context, IPFS client, String name) {
    this.ipfs = client;
    this.name = name;
    logger.debug("IPFS record writer construct, name: {}", name);
    try {
      init(null);
    } catch (IOException e) {
      //pass
    }
  }

  @Override
  public void init(Map<String, String> writerOptions) throws IOException {
    logger.debug("IPFS record writer init, writerOptions: {}", writerOptions);
    stream = new ByteArrayOutputStream();
    JsonGenerator generator = factory.createGenerator(stream).useDefaultPrettyPrinter().configure(JsonGenerator.Feature.QUOTE_NON_NUMERIC_NUMBERS, true);
    generator = generator.setPrettyPrinter(new MinimalPrettyPrinter(LINE_FEED));
    gen = new ExtendedJsonOutput(generator);
  }

  @Override
  public void updateSchema(VectorAccessible batch) throws IOException {

  }

  @Override
  public void checkForNewPartition(int index) {
    if (index > MAX_RECORDS_PER_LEAF) {
      // flush data to IPFS
    }
  }

  @Override
  public void startRecord() throws IOException {
    gen.writeStartObject();
    fRecordStarted = true;
  }

  @Override
  public void endRecord() throws IOException {
    gen.writeEndObject();
    fRecordStarted = false;
  }

  @Override
  public void abort() throws IOException {
    logger.debug("IPFS record writer abort");
    gen.flush();
    stream.reset();
  }

  /**
   * Will be called at the end of every schema write
   * Flush data to IPFS
   * @throws IOException
   */
  @Override
  public void cleanup() throws IOException {
    logger.debug("IPFS record writer cleanup");
    gen.flush();
    MerkleNode node = ipfs.object.patch(
      IPFSHelper.IPFS_NULL_OBJECT,
      "set-data",
      Optional.of(stream.toByteArray()),
      Optional.empty(),
      Optional.empty()
    );
    logger.debug("successfully written to IPFS, resulting node hash {}", node.hash);
    stream.close();
  }
}
