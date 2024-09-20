/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package demo;

import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.FlowEventArgs;
import com.solacesystems.jcsmp.FlowEventHandler;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPErrorResponseException;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProducerEventHandler;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.OperationNotSupportedException;
import com.solacesystems.jcsmp.ProducerEventArgs;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.SpringJCSMPFactory;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageProducer;

import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Component;

@SpringBootApplication
public class DemoApplication {

    private static final Logger logger = LoggerFactory.getLogger(DemoApplication.class);

    private static final String QUEUE_NAME = "demo-sample";
    private static XMLMessageProducer producer;
    private static FlowReceiver flowQueueReceiver;
    private static JCSMPSession session;
    public static boolean isShutdown = false;

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args).close();
    }

    private static void publish() throws JCSMPException {
        TextMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);  // preallocate
        message.setText("DemoSample");
        message.setDeliveryMode(DeliveryMode.PERSISTENT);  // required for Guaranteed

        String topicString = new StringBuilder("demo/sample").toString();
        Topic topic = JCSMPFactory.onlyInstance().createTopic(topicString);
        producer.send(message, topic);
    }

    private static void startConsumer() throws JCSMPException {
        // configure the queue API object locally
        final Queue queue = JCSMPFactory.onlyInstance().createQueue(QUEUE_NAME);
        // Create a Flow be able to bind to and consume messages from the Queue.
        final ConsumerFlowProperties flow_prop = new ConsumerFlowProperties();
        flow_prop.setEndpoint(queue);
        flow_prop.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);  // best practice
        flow_prop.setActiveFlowIndication(true);  // Flow events will advise when

       logger.info("Attempting to bind to queue '%s' on the broker.%n", QUEUE_NAME);
        try {
            // DemoMessageConsumer receives the messages from the queue
            flowQueueReceiver = session.createFlow(new DemoMessageConsumer(), flow_prop, null, new FlowEventHandler() {
                @Override
                public void handleEvent(Object source, FlowEventArgs event) {
                    // Flow events are usually: active, reconnecting (i.e. unbound), reconnected, active
                    logger.info("### Received a Flow event: " + event);
                    // try disabling and re-enabling the queue to see in action
                }
            });
        } catch (OperationNotSupportedException e) {  // not allowed to do this
            throw e;
        } catch (JCSMPErrorResponseException e) {  // something else went wrong: queue not exist, queue shutdown, etc.
            logger.error(e.toString());
            logger.error("%n*** Could not establish a connection to queue '%s': %s%n", QUEUE_NAME, e.getMessage());
            logger.error("Exiting.");
            return;
        }
        // tell the broker to start sending messages on this queue receiver
        flowQueueReceiver.start();
        // async queue receive working now, so time to wait until done...
        logger.info("Connected, and running. Press [ENTER] to quit.");
    }

    @Component
    public static class Runner implements CommandLineRunner {

        private static final Logger logger = LoggerFactory.getLogger(Runner.class);

        @Autowired private SpringJCSMPFactory solaceFactory;

        // Examples of other beans that can be used together to generate a customized SpringJCSMPFactory
        //@Autowired(required=false) private JCSMPProperties jcsmpProperties;

        public void run(String... strings) throws Exception {
            session = solaceFactory.createSession();
            startConsumer();

            producer = session.getMessageProducer(new DemoPublishEventHandler(), new JCSMPProducerEventHandler() {
            @Override
            public void handleEvent(ProducerEventArgs event) {
                // as of JCSMP v10.10, this event only occurs when republishing unACKed messages on an unknown flow (DR failover)
                logger.info("*** Received a producer event: " + event);
            }
            });

            ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
            Runnable task = () -> {
                try {
                    publish();
                } catch (JCSMPException e) {
                    e.printStackTrace();
                }
            };
            ses.scheduleAtFixedRate(task, 0, 1, TimeUnit.SECONDS); // ( task , initialDelay , period , unit )

            Scanner in = new Scanner(System.in);
            if (in.hasNextLine() || isShutdown) {
                logger.info("Closing the connection");
            }
            in.close();

            ses.shutdown();
            producer.close();
            flowQueueReceiver.stop();
            session.closeSession();  // will also close consumer object
        }
    }
}
