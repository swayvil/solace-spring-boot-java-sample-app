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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPTransportException;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLMessageListener;

public class DemoMessageConsumer implements XMLMessageListener {

    private static final Logger logger = LoggerFactory.getLogger(DemoMessageConsumer.class);

    @Override
    public void onReceive(BytesXMLMessage msg) {
        if (msg instanceof TextMessage) {
            logger.info("============= TextMessage received: {}", ((TextMessage) msg).getText());
        } else {
            logger.info("============= Message received.");
        }

        // if (msg.hasAttachment()) {
        //     byte[] attachment = new byte[msg.getAttachmentContentLength()];
        //     msg.readAttachmentBytes(attachment);
        //     String rxAttachment = new String(attachment);
        //     System.out.println(rxAttachment);
        // }

        if (msg.getRedelivered()) {  // useful check
            // this is the broker telling the consumer that this message has been sent and not ACKed before.
            // this can happen if an exception is thrown, or the broker restarts, or the netowrk disconnects
            // perhaps an error in processing? Should do extra checks to avoid duplicate processing
        }
        // Messages are removed from the broker queue when the ACK is received.
        // Therefore, DO NOT ACK until all processing/storing of this message is complete.
        // NOTE that messages can be acknowledged from a different thread.
        msg.ackMessage();  // ACKs are asynchronous
    }

    @Override
        public void onException(JCSMPException e) {
            logger.error("### MessageListener's onException(): %s%n",e);
            if (e instanceof JCSMPTransportException) {  // unrecoverable, all reconnect attempts failed
                demo.DemoApplication.isShutdown = true;  // let's quit
            }
        }
}
