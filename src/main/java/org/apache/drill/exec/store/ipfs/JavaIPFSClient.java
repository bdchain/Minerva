
import io.ipfs.multibase.*;

import io.ipfs.api.*;
import io.ipfs.api.cbor.*;
import io.ipfs.cid.*;
import io.ipfs.multihash.Multihash;
import io.ipfs.multiaddr.MultiAddress;

import java.util.*;
import java.io.*;
import java.net.*;

import com.google.common.base.Charsets;


public class JavaIPFSClient {

    public IpfsRecordReader getByHash(String target, FragmentContext context, HttpSubScan config) throws IOException {
        final IpfsRecordReader ipfsRecordReader = new IpfsRecordReader(context, config);
    	final IPFS ipfs = new IPFS(new MultiAddress("/ip4/127.0.0.1/tcp/5001"));

        Multihash pointer = Multihash.fromBase58(target);
        byte[] content = ipfs.get(pointer);
        ipfsRecordReader.loadFile(new String(content, Charsets.UTF_8), output);//TODO:赋值OutputMutator output

        //System.out.print("");
        return ipfsRecordReader;
    }

    public static void main(String[] a) throws Exception {
        IpfsRecordReader reader = new JavaIPFSClient().getByHash("QmPZ9gcCEpqKTo6aq61g2nXGUhM4iCL3ewB6LDXZCtioEB",
                fragmentContext, config);//TODO:赋值FragmentContext context, HttpSubScan config

        System.out.print(reader.next());
    }
}