package org.apache.drill.exec.store.ipfs;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.SchemaFactory;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

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
    private Set<String> tableNames = Sets.newHashSet();
    private final ConcurrentMap<String, Table> tables = new ConcurrentSkipListMap<>(String::compareToIgnoreCase);
    public IPFSTables (String name) {
      super(ImmutableList.<String>of(), name);
      tableNames.add(name);
    }

    public void setHolder(SchemaPlus pulsOfThis) {
    }

    @Override
    public String getTypeName() {
      return IPFSStoragePluginConfig.NAME;
    }

    @Override
    public Set<String> getTableNames() {
      return tableNames;
    }

    @Override
    public Table getTable(String tableName) {
      logger.debug("getTable in IPFSTables {}", tableName);
      if (!tableName.startsWith("/")) {
        throw new InvalidParameterException("IPFS path must start with /");
      }
      String[] parts = tableName.substring(1).split("/");
      IPFSScanSpec spec = new IPFSScanSpec(parts[1], IPFSScanSpec.Prefix.of(parts[0]));
      return tables.computeIfAbsent(name,
          n -> new DynamicDrillTable(plugin, schemaName, spec));
    }
  }
}
