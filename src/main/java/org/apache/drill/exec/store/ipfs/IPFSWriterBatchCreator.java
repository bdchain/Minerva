package org.apache.drill.exec.store.ipfs;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.ExecutorFragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.WriterRecordBatch;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.RecordBatch;

import java.util.List;

public class IPFSWriterBatchCreator implements BatchCreator<IPFSWriter> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IPFSWriterBatchCreator.class);

  @Override
  public CloseableRecordBatch getBatch(ExecutorFragmentContext context, IPFSWriter config, List<RecordBatch> children)
      throws ExecutionSetupException {
    assert children != null && children.size() == 1;

    return new WriterRecordBatch(config, children.iterator().next(), context, new IPFSRecordWriter(
        null,
        config.getPlugin().getIPFSClient(),
        config.getName()));
  }
}
