

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.http.util.JsonConverter;
import org.apache.drill.exec.store.http.util.SimpleHttp;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.complex.fn.JsonReader;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class IpfsRecordReader extends AbstractRecordReader {
    private VectorContainerWriter writer;
    private JsonReader jsonReader;
    private FragmentContext fragmentContext;
    private Iterator<JsonNode> jsonIt;
    private HttpSubScan subScan;

    public IpfsRecordReader(FragmentContext context, HttpSubScan subScan){
        this.subScan = subScan;
        fragmentContext = context;
        Set<SchemaPath> transformed = Sets.newLinkedHashSet();
        transformed.add(STAR_COLUMN);
        setColumns(transformed);//TODO:不懂这几行的作用
    }

    public void loadFile(String content, OutputMutator output) {
        //Read from local position. Expired
        //String content = JsonConverter.stringFromFile(file);

        this.writer = new VectorContainerWriter(output);
        this.jsonReader = new JsonReader(fragmentContext.getManagedBuffer(),
                Lists.newArrayList(getColumns()), true, false, true);
        parseResult(content);
    }

    private void parseResult(String content) {
        /* String key = subScan.getStorageConfig().getResultKey();
        JsonNode root = key.length() == 0 ? JsonConverter.parse(content) :
                JsonConverter.parse(content, key);
        if (root != null) {
            logger.debug("response object count {}", root.size());
            jsonIt = root.elements();
         } */
        JsonNode root = JsonConverter.parse(content);
        jsonIt = root.elements();
    }

    @Override
    public int next() {
        if (jsonIt == null || !jsonIt.hasNext()) {
            return 0;
        }
        writer.allocate();
        writer.reset();
        int docCount = 0;
        try {
            while (docCount < BaseValueVector.INITIAL_VALUE_ALLOCATION && jsonIt.hasNext()) {
                JsonNode node = jsonIt.next();
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
    public void cleanup() {
        logger.debug("HttpRecordReader cleanup");
    }

}
