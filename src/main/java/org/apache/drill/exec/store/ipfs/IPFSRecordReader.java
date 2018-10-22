package org.apache.drill.exec.store.ipfs;

import io.ipfs.api.IPFS;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.vector.ValueVector;

import java.util.List;
import java.util.Map;

public class IPFSRecordReader implements RecordReader {
  public IPFSRecordReader(IPFS ipfsClient, IPFSSubScan.IPFSSubScanSpec scanSpec, List<SchemaPath> columns) {



  }

  public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {

  }

  public void allocate(Map<String, ValueVector> vectorMap) throws OutOfMemoryException {

  }

  public int next() {
    return 1;
  }

  public void close() {

  }
}
