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
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;

/*
* A streaming producer can provide this callback handler to handle ack events.
*/
public class DemoPublishEventHandler implements JCSMPStreamingPublishCorrelatingEventHandler {
    private static final Logger logger = LoggerFactory.getLogger(DemoPublishEventHandler.class);

    @Override
    public void handleErrorEx(Object key, JCSMPException cause, long timestamp) {
        if (key != null) {  // NACK
            assert key instanceof BytesXMLMessage;
            logger.warn(String.format("NACK for Message %s - %s", key, cause));
            // probably want to do something here.  some error handling possibilities:
            //  - send the message again
            //  - send it somewhere else (error handling queue?)
            //  - log and continue
            //  - pause and retry (backoff) - maybe set a flag to slow down the publisher
        } else {  // not a NACK, but some other error (ACL violation, connection loss, message too big, ...)
            logger.warn("### Producer handleErrorEx() callback: %s%n", cause);
        }
    }

    @Override
    public void responseReceivedEx(Object key) {
        //logger.info("Producer received response for msg: " + key.toString());
    }
}
