package org.apache.drill.exec.store.ipfs;


import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ipfs.api.IPFS;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;

import java.io.IOException;
import java.util.List;

public class IPFSStoragePlugin extends AbstractStoragePlugin {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IPFSStoragePlugin.class);

  private final IPFSStoragePluginConfig pluginConfig;
  private final IPFSSchemaFactory schemaFactory;
  private final IPFS ipfsClient;

  public IPFSStoragePlugin(IPFSStoragePluginConfig config, DrillbitContext context, String name) throws IOException {
    super(context, name);
    this.schemaFactory = new IPFSSchemaFactory(this, name);
    this.pluginConfig = config;
    this.ipfsClient = new IPFS(config.getHost(), config.getPort());
  }

  @Override
  public boolean supportsRead() {
    return true;
  }

  @Override
  public boolean supportsWrite() {
    return true;
  }

  @Override
  public IPFSGroupScan getPhysicalScan(String userName, JSONOptions selection) throws IOException {
    logger.debug("IPFSStoragePlugin before getPhysicalScan");
    IPFSScanSpec spec = selection.getListWith(new ObjectMapper(), new TypeReference<IPFSScanSpec>() {});
    logger.debug("IPFSStoragePlugin getPhysicalScan with selection {}", selection);
    return new IPFSGroupScan(this, spec, null);
  }

  @Override
  public IPFSGroupScan getPhysicalScan(String userName, JSONOptions selection, List<SchemaPath> columns) throws IOException {
    logger.debug("IPFSStoragePlugin before getPhysicalScan");
    IPFSScanSpec spec = selection.getListWith(new ObjectMapper(), new TypeReference<IPFSScanSpec>() {});
    logger.debug("IPFSStoragePlugin getPhysicalScan with selection {}, columns {}", selection, columns);
    return new IPFSGroupScan(this, spec, columns);
  }

  public IPFS getIPFSClient() {
    return ipfsClient;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    schemaFactory.registerSchemas(schemaConfig, parent);
  }

  @Override
  public IPFSStoragePluginConfig getConfig() {
    return pluginConfig;
  }
}
