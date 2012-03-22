package storm.contrib.hbase;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.hbase.async.Bytes;
import org.hbase.async.HBaseClient;
import org.hbase.async.PutRequest;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * HBase bolt which maps input tuples into Puts
 *
 * @author Michael Rose <michael@fullcontact.com>
 */
public abstract class HbaseBolt implements IRichBolt {
    protected HBaseClient _client;
    protected final String _quorumSpec;
    protected final byte[] _table;
    protected OutputCollector _collector;

    protected HbaseBolt(String table, String quorumSpec) {
        this(Bytes.UTF8(table), quorumSpec);
    }

    protected HbaseBolt(byte[] table, String quorumSpec) {
        this._table = table;
        this._quorumSpec = quorumSpec;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        this._client = new HBaseClient(this._quorumSpec);
    }

    @Override
    public void cleanup() {
        try {
            _client.shutdown().joinUninterruptibly();
        } catch (Exception e) {
            // Likely to be gone soon anyways
        }
    }

}
