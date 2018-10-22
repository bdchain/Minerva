package org.apache.drill.exec.store.ipfs;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.SchemaFactory;

import java.io.IOException;

public class IPFSSchemaFactory implements SchemaFactory{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IPFSSchemaFactory.class);

  final String schemaName;
  final IPFSStoragePlugin plugin;

  public IPFSSchemaFactory(IPFSStoragePlugin plugin, String name) throws IOException {
    this.plugin = plugin;
    this.schemaName = name;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    logger.debug("registerSchemas {}", schemaName);
    IPFSTables schema = new IPFSTables(schemaName);
    SchemaPlus hPlus = parent.add(schemaName, schema);
    schema.setHolder(hPlus);
  }

  class IPFSTables extends AbstractSchema {
    public IPFSTables (String name) {
      super(ImmutableList.<String>of(), name);
    }

    public void setHolder(SchemaPlus pulsOfThis) {
    }

    @Override
    public String getTypeName() {
      return IPFSStoragePluginConfig.NAME;
    }

    @Override
    public Table getTable(String tableName) {
      logger.debug("getTable in IPFSTables {}", tableName);
      IPFSScanSpec spec = new IPFSScanSpec(tableName);
      return new DynamicDrillTable(plugin, schemaName, spec);
    }
  }
}
