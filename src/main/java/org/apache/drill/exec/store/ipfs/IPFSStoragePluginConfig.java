package org.apache.drill.exec.store.ipfs;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfigBase;

import java.util.Map;

@JsonTypeName(IPFSStoragePluginConfig.NAME)
public class IPFSStoragePluginConfig extends StoragePluginConfigBase{
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IPFSStoragePluginConfig.class);

    public static final String NAME = "ipfs";

    private final String host;
    private final int port;

    @JsonProperty("max-nodes-per-leaf")
    private final int maxNodesPerLeaf;

    @JsonProperty("ipfs-timeout")
    private final int ipfsTimeout;

    @JsonProperty
    private final Map<String, FormatPluginConfig> formats;

    @JsonCreator
    public IPFSStoragePluginConfig(
        @JsonProperty("host") String host,
        @JsonProperty("port") int port,
        @JsonProperty("max-nodes-per-leaf") int maxNodesPerLeaf,
        @JsonProperty("ipfs-timeout") int ipfsTimeout,
        @JsonProperty("formats") Map<String, FormatPluginConfig> formats) {
        this.host = host;
        this.port = port;
        this.maxNodesPerLeaf = maxNodesPerLeaf;
        this.ipfsTimeout = ipfsTimeout;
        this.formats = formats;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getMaxNodesPerLeaf() {
        return maxNodesPerLeaf;
    }

    public int getIpfsTimeout() {
        return ipfsTimeout;
    }

    public Map<String, FormatPluginConfig> getFormats() {
        return formats;
    }

    @Override
    public int hashCode() {
        String host_port = String.format("%s:%d[%d,%d]", host, port, maxNodesPerLeaf, ipfsTimeout);
        final int prime = 31;
        int result = 1;
        result = prime * result + ((host_port == null) ? 0 : host_port.hashCode());
        result = prime * result + ((formats == null) ? 0 : formats.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        IPFSStoragePluginConfig other = (IPFSStoragePluginConfig) obj;
        if (formats == null) {
            if (other.formats != null) {
                return false;
            }
        } else if (!formats.equals(other.formats)) {
            return false;
        }
        if (host == null) {
            if (other.host != null) {
                return false;
            }
        } else if (!host.equals(other.host)
            || port != other.port
            || maxNodesPerLeaf != other.maxNodesPerLeaf
            || ipfsTimeout != other.ipfsTimeout) {
            return false;
        }
        return true;
    }
}
