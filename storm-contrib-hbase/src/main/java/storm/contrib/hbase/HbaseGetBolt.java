package storm.contrib.hbase;

import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.hbase.async.GetRequest;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

/**
 * HBase bolt which maps input tuples into Puts
 *
 * @author Michael Rose <michael@fullcontact.com>
 */
public abstract class HbaseGetBolt extends HbaseBolt {
    private final ConcurrentHashMap<MessageId, Tuple> _pendingReads = new ConcurrentHashMap<MessageId, Tuple>();

    protected HbaseGetBolt(String table, String quorumSpec) {
        super(table, quorumSpec);
    }

    protected HbaseGetBolt(byte[] table, String quorumSpec) {
        super(table, quorumSpec);
    }

    @Override
    public void execute(Tuple tuple) {
        final MessageId tupleMessageId = tuple.getMessageId();

        // TODO: lets not suck and make this extensible
        byte[] key = tuple.getBinary(0);

        // Generate Get and add to pending writes
        GetRequest getRequest = new GetRequest(_table, key);
        _pendingReads.put(tupleMessageId, tuple);

        // Get the deferred put
        Deferred<ArrayList<KeyValue>> get = _client.get(getRequest);

        // Add callbacks to the put to ack/fail + remove the tuples
        get.addCallbacks(
                new Callback<ArrayList<KeyValue>, ArrayList<KeyValue>>() {
                    public ArrayList<KeyValue> call(ArrayList<KeyValue> cells) {
                        _collector.ack(_pendingReads.remove(tupleMessageId));
                        doEmit(cells);
                        return cells;
                    }
                },
                new Callback<Object, Exception>() {
                    public Object call(Exception e) {
                        _collector.fail(_pendingReads.get(tupleMessageId));
                        return e;
                    }
                }
            );
    }

    protected abstract void doEmit(ArrayList<KeyValue> cells);
}
