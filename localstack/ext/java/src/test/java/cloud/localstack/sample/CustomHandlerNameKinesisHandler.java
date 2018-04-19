package cloud.localstack.sample;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;

public class CustomHandlerNameKinesisHandler {

    public Object customHandler(KinesisEvent event, Context context) {
        for (KinesisEvent.KinesisEventRecord rec : event.getRecords()) {
            String msg = new String(rec.getKinesis().getData().array());
            System.err.println("Kinesis record: " + msg);
        }
        return "{}";
    }
}
