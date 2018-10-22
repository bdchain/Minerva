package org.apache.drill.exec.store.ipfs;

import org.apache.drill.common.logical.StoragePluginConfigBase;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(IPFSStoragePluginConfig.NAME)
public class IPFSStoragePluginConfig extends StoragePluginConfigBase{
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IPFSStoragePluginConfig.class);

    public static final String NAME = "ipfs";

    private final String host;
    private final int port;

    @JsonCreator
    public IPFSStoragePluginConfig(@JsonProperty("host") String host, @JsonProperty("port") int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public int hashCode() {
        String host_port = String.format("%s:%d", host, port);
        final int prime = 31;
        int result = 1;
        result = prime * result + ((host_port == null) ? 0 : host_port.hashCode());
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
        if (host == null) {
            if (other.host != null) {
                return false;
            }
        } else if (!host.equals(other.host) || port != other.port) {
            return false;
        }
        return true;
    }
}
