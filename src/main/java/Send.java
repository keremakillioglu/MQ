import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static com.google.common.math.DoubleMath.mean;
import static com.google.common.math.Quantiles.percentiles;

public class Send {

    private final static String QUEUE_NAME = "hello"; // sender queue
    private final static String QUEUE_NAME2 = "aloha"; // acknowledgement queue
    private final static String HOST = "40.85.219.36";

    public static void main(String[] argv) throws Exception {
        // createPayload
        byte[] smallPayload = new byte[1];
        byte[] bigPayload = new byte[1000*1000];
        new Random().nextBytes(bigPayload);

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        factory.setPort(5672);
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            channel.queueDeclare(QUEUE_NAME2, false, false, false, null);

        List<Integer> smallExperiments = new ArrayList<>();
        List<Integer> bigExperiments = new ArrayList<>();
        for (int i = 1; i <= 30; i++) {
            Instant start = Instant.now();
            makeRequest(channel, smallPayload);
            Instant end = Instant.now();
            Duration timeElapsed = Duration.between(start, end);
            smallExperiments.add((int) timeElapsed.toMillis());

            start = Instant.now();
            makeRequest(channel, bigPayload);
            end = Instant.now();
            timeElapsed = Duration.between(start, end);
            bigExperiments.add((int) timeElapsed.toMillis());
        }

            String message = "Hello World!";
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
            System.out.println(" [x] Sent '" + message + "'");

            // necessary calculations
            Collections.sort(smallExperiments);
            double ninetyPercentile = percentiles().index(90).compute(smallExperiments);
            double tenPercentile = percentiles().index(10).compute(smallExperiments);
            double mean = mean(smallExperiments);

            // for reporting purposes
            System.out.println(smallExperiments);
            System.out.println("90 Percentile: "+ninetyPercentile);
            System.out.println("10 Percentile: "+tenPercentile);
            System.out.println("Mean: "+mean);

            // necessary calculations
            Collections.sort(bigExperiments);
            ninetyPercentile = percentiles().index(90).compute(bigExperiments);
            tenPercentile = percentiles().index(10).compute(bigExperiments);
            mean = mean(bigExperiments);

            // for reporting purposes
            System.out.println(bigExperiments);
            System.out.println("90 Percentile: "+ninetyPercentile);
            System.out.println("10 Percentile: "+tenPercentile);
            System.out.println("Mean: "+mean);

        }

    }

    static void makeRequest(Channel channel, byte[] payload) {
        try {
            channel.basicPublish("",QUEUE_NAME, null, payload);
            GetResponse response = null;

            while (response == null) {
                response = channel.basicGet(QUEUE_NAME2, true);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}