package net.acegik.examples;

import java.util.HashMap;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import net.acegik.jsondataflow.OpflowMessage;
import net.acegik.jsondataflow.OpflowPubsubHandler;
import net.acegik.jsondataflow.OpflowPubsubListener;

/**
 *
 * @author drupalex
 */
public class OpflowPubsubExample {

    public static void main(String[] argv) throws Exception {
        final Gson gson = new Gson();
        final JsonParser jsonParser = new JsonParser();
        final HashMap<String, Object> flowParams = new HashMap<String, Object>();
        flowParams.put("uri", "amqp://master:zaq123edcx@192.168.56.56?frameMax=0x1000");
        flowParams.put("exchangeName", "tdd-opflow-publisher");
        flowParams.put("exchangeType", "direct");
        flowParams.put("routingKey", "tdd-opflow-pubsub-public");
        flowParams.put("consumer.queueName", "tdd-opflow-subscriber#5");

        final OpflowPubsubHandler pubsub = new OpflowPubsubHandler(flowParams);
        pubsub.subscribe(new OpflowPubsubListener() {
            @Override
            public void processMessage(OpflowMessage message) throws IOException {
                JsonObject jsonObject = (JsonObject)jsonParser.parse(new String(message.getContent(), "UTF-8"));
                System.out.println(" [+] Received '" + jsonObject.toString() + "'");
            }
        });
    }
}
