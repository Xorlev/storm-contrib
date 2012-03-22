package storm.contrib.hbase;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
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
    private HBaseClient _client;
    private final String _quorumSpec;
    private final byte[] _table;
    private final ConcurrentHashMap<MessageId, Tuple> _pendingWrites = new ConcurrentHashMap<MessageId, Tuple>();
    private OutputCollector _collector;

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
    public void execute(Tuple tuple) {
        final MessageId tupleMessageId = tuple.getMessageId();
        byte[] key = tuple.getBinary(0);
        byte[] family = tuple.getBinary(1);
        byte[] qualifier = tuple.getBinary(2);
        byte[] value = tuple.getBinary(3);

        // Generate Put and add to pending writes
        PutRequest putRequest = new PutRequest(_table, key, family, qualifier, value);
        _pendingWrites.put(tupleMessageId, tuple);

        // Get the deferred put
        Deferred put = _client.put(putRequest);

        // Add callbacks to the put to ack/fail + remove the tuples
        put.addCallbacks(
                new Callback<Object, Object>() {
                    public Object call(Object nullObject) {
                        _collector.ack(_pendingWrites.remove(tupleMessageId));
                        return nullObject;
                    }
                },
                new Callback<Object, Exception>() {
                    public Object call(Exception nullObject) {
                        _collector.fail(_pendingWrites.get(tupleMessageId));
                        return nullObject;
                    }
                }
            );
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
