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
public abstract class HbasePutBolt extends HbaseBolt {
    private final ConcurrentHashMap<MessageId, Tuple> _pendingWrites = new ConcurrentHashMap<MessageId, Tuple>();

    protected HbasePutBolt(String table, String quorumSpec) {
        super(table, quorumSpec);
    }

    protected HbasePutBolt(byte[] table, String quorumSpec) {
        super(table, quorumSpec);
    }

    @Override
    public void execute(Tuple tuple) {
        final MessageId tupleMessageId = tuple.getMessageId();

        // TODO: lets not suck and make this extensible
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
                    public Object call(Exception e) {
                        _collector.fail(_pendingWrites.get(tupleMessageId));
                        return e;
                    }
                }
            );
    }
}
