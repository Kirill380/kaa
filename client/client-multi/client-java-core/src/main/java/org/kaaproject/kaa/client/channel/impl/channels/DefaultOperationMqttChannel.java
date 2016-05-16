/*
 * Copyright 2014-2016 CyberVision, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kaaproject.kaa.client.channel.impl.channels;

import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.kaaproject.kaa.client.FailureListener;
import org.kaaproject.kaa.client.channel.*;
import org.kaaproject.kaa.client.channel.connectivity.ConnectivityChecker;
import org.kaaproject.kaa.client.channel.failover.FailoverDecision;
import org.kaaproject.kaa.client.channel.failover.FailoverManager;
import org.kaaproject.kaa.client.channel.failover.FailoverStatus;
import org.kaaproject.kaa.client.persistence.KaaClientState;
import org.kaaproject.kaa.common.TransportType;
import org.kaaproject.kaa.common.channels.protocols.mqtt.KaaMqttClient;
import org.kaaproject.kaa.common.channels.protocols.mqtt.listeners.ConnectionLostListener;
import org.kaaproject.kaa.common.channels.protocols.mqtt.listeners.MessageArrivedListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

//TODO add encryption
public class DefaultOperationMqttChannel implements KaaDataChannel {

    public static final Logger LOG = LoggerFactory.getLogger(DefaultOperationMqttChannel.class);

    private static final Map<TransportType, ChannelDirection> SUPPORTED_TYPES = new HashMap<TransportType, ChannelDirection>();

    static {
        SUPPORTED_TYPES.put(TransportType.PROFILE, ChannelDirection.BIDIRECTIONAL);
        SUPPORTED_TYPES.put(TransportType.CONFIGURATION, ChannelDirection.BIDIRECTIONAL);
        SUPPORTED_TYPES.put(TransportType.NOTIFICATION, ChannelDirection.BIDIRECTIONAL);
        SUPPORTED_TYPES.put(TransportType.USER, ChannelDirection.BIDIRECTIONAL);
        SUPPORTED_TYPES.put(TransportType.EVENT, ChannelDirection.BIDIRECTIONAL);
        SUPPORTED_TYPES.put(TransportType.LOGGING, ChannelDirection.BIDIRECTIONAL);
    }

    private static final String CHANNEL_ID = "default_operation_mqtt_channel";

    private FailureListener failureListener;

    private IPTransportInfo currentServer;
    private final KaaClientState state;

    private ScheduledExecutorService executor;

    private volatile State channelState = State.CLOSED;

    private KaaDataDemultiplexer demultiplexer;
    private KaaDataMultiplexer multiplexer;


    private KaaMqttClient client;

    private final FailoverManager failoverManager;

    private volatile ConnectivityChecker connectivityChecker;

    private final Runnable openConnectionTask = new Runnable() {
        @Override
        public void run() {
            openConnection();
        }
    };


    private final MessageArrivedListener messageArrivedListener = new MessageArrivedListener() {
        @Override
        public void messageArrived(String topic, MqttMessage message) {
            LOG.info("KaaSync message  received for channel [{}]", getId());
            byte[] resultBody = message.getPayload();

            if (resultBody != null) {
                try {
                    demultiplexer.preProcess();
                    demultiplexer.processResponse(resultBody);
                    demultiplexer.postProcess();
                } catch (Exception e) {
                    LOG.error("Failed to process response for channel [{}]", getId(), e);
                }

                synchronized (DefaultOperationMqttChannel.this) {
                    channelState = State.OPENED;
                }
                failoverManager.onServerConnected(currentServer);
            }
        }
    };


    private final ConnectionLostListener connectionLostListener = new ConnectionLostListener() {
        @Override
        public void connectionLost(Throwable e) {
            LOG.error("Server error occurred: {}", e);
            onServerFailed();
        }
    };


    private volatile boolean isOpenConnectionScheduled;

    public DefaultOperationMqttChannel(KaaClientState state, FailoverManager failoverManager, FailureListener failureListener) {
        this.state = state;
        this.failoverManager = failoverManager;
        this.failureListener = failureListener;
    }


    private void sendKaaSyncRequest(Map<TransportType, ChannelDirection> types) throws Exception {
        LOG.debug("Sending KaaSync from channel [{}]", getId());
        byte[] body = multiplexer.compileRequest(types);
        client.sendRequest(body, "avro", "v1");
    }

    private void sendConnect() {
        LOG.debug("Sending Connect to channel [{}]", getId());
        try {
            client.connect();
        } catch (MqttException ex) {
            LOG.error("Connection for channel [{}] was rejected: {}", getId(), ex.getMessage());
            LOG.info("Cleaning client state");
            state.clean();
            if (ex.getReasonCode() == MqttException.REASON_CODE_FAILED_AUTHENTICATION) {
                onServerFailed(FailoverStatus.ENDPOINT_VERIFICATION_FAILED);
            } else {
                onServerFailed();
            }
        }
    }

    private synchronized void closeConnection() {
        LOG.info("Channel \"{}\": closing current connection", getId());
        try {
            client.disconnect();
        } catch (MqttException e) {
            LOG.error("Failed during disconnection {}", e);
        } finally {
            if (channelState != State.SHUTDOWN) {
                channelState = State.CLOSED;
            }
        }
    }


    private synchronized void openConnection() {
        if (channelState == State.PAUSE || channelState == State.SHUTDOWN) {
            LOG.info("Can't open connection, as channel is in the {} state", channelState);
            return;
        }
        try {
            LOG.info("Channel [{}]: opening connection to server {}", getId(), currentServer);
            isOpenConnectionScheduled = false;
            sendConnect();
        } catch (Exception e) {
            LOG.error("Failed to create a socket for server {}:{}. Stack trace: ", currentServer.getHost(), currentServer.getPort(), e);
            onServerFailed();
        }
    }

    private void onServerFailed() {
        this.onServerFailed(FailoverStatus.NO_CONNECTIVITY);
    }

    private void onServerFailed(FailoverStatus status) {
        LOG.info("[{}] has failed", getId());
        closeConnection();
        if (connectivityChecker != null && !connectivityChecker.checkConnectivity()) {
            LOG.warn("Loss of connectivity detected");

            FailoverDecision decision = failoverManager.onFailover(status);
            switch (decision.getAction()) {
                case NOOP:
                    LOG.warn("No operation is performed according to failover strategy decision");
                    break;
                case RETRY:
                    long retryPeriod = decision.getRetryPeriod();
                    LOG.warn("Attempt to reconnect will be made in {} ms " +
                            "according to failover strategy decision", retryPeriod);
                    scheduleOpenConnectionTask(retryPeriod);
                    break;
                case FAILURE:
                    LOG.warn("Calling failure listener according to failover strategy decision!");
                    failureListener.onFailure();
                    break;
                default:
                    break;
            }
        } else {
            failoverManager.onServerFailed(currentServer, status);
        }
    }

    private synchronized void scheduleOpenConnectionTask(long retryPeriod) {
        if (!isOpenConnectionScheduled) {
            if (executor != null) {
                LOG.info("Scheduling open connection task");
                executor.schedule(openConnectionTask, retryPeriod, TimeUnit.MILLISECONDS);
                isOpenConnectionScheduled = true;
            } else {
                LOG.info("Executor is null, can't schedule open connection task");
            }
        } else {
            LOG.info("Reconnect is already scheduled, ignoring the call");
        }
    }


    protected ScheduledExecutorService createExecutor() {
        LOG.info("Creating a new executor for channel [{}]", getId());
        return new ScheduledThreadPoolExecutor(1);
    }

    protected String getBroker() {
        return "tcp://" + currentServer.getHost() + ":" + currentServer.getPort();
//        return  "tcp://localhost:1884"; for testing on localhost
    }

    @Override
    public synchronized void sync(TransportType type) {
        sync(Collections.singleton(type));
    }

    @Override
    public synchronized void sync(Set<TransportType> types) {

        if (channelState == State.SHUTDOWN) {
            LOG.info("Can't sync. Channel [{}] is down", getId());
            return;
        }
        if (channelState == State.PAUSE) {
            LOG.info("Can't sync. Channel [{}] is paused", getId());
            return;
        }
        if (channelState != State.OPENED) {
            LOG.info("Can't sync. Channel [{}] is waiting for CONNACK message + KAASYNC message", getId());
            return;
        }
        if (multiplexer == null) {
            LOG.warn("Can't sync. Channel {} multiplexer is not set", getId());
            return;
        }
        if (demultiplexer == null) {
            LOG.warn("Can't sync. Channel {} demultiplexer is not set", getId());
            return;
        }
        if (currentServer == null || !client.isConnected()) {
            LOG.warn("Can't sync. Server is {}, MQTT client is {}", currentServer, client.isConnected() ? "connected" : "disconnected");
            return;
        }

        Map<TransportType, ChannelDirection> typeMap = new HashMap<>(getSupportedTransportTypes().size());

        for (TransportType type : types) {
            LOG.info("Processing sync {} for channel [{}]", type, getId());

            ChannelDirection direction = getSupportedTransportTypes().get(type);

            if (direction != null) {
                typeMap.put(type, direction);
            } else {
                LOG.error("Unsupported type {} for channel [{}]", type, getId());
            }

            for (Map.Entry<TransportType, ChannelDirection> typeIt : getSupportedTransportTypes().entrySet()) {
                if (!typeIt.getKey().equals(type)) {
                    typeMap.put(typeIt.getKey(), ChannelDirection.DOWN);
                }
            }
        }
        try {
            sendKaaSyncRequest(typeMap);
        } catch (Exception e) {
            LOG.error("Failed to sync channel [{}]", getId(), e);
        }
    }

    @Override
    public synchronized void syncAll() {
        if (channelState == State.SHUTDOWN) {
            LOG.info("Can't sync. Channel [{}] is down", getId());
            return;
        }
        if (channelState == State.PAUSE) {
            LOG.info("Can't sync. Channel [{}] is paused", getId());
            return;
        }
        if (channelState != State.OPENED) {
            LOG.info("Can't sync. Channel [{}] is waiting for CONNACK + KAASYNC message", getId());
            return;
        }

        LOG.info("Processing sync all for channel [{}]", getId());
        if (multiplexer != null && demultiplexer != null) {
            if (currentServer != null && client.isConnected()) {
                try {
                    sendKaaSyncRequest(getSupportedTransportTypes());
                } catch (Exception e) {
                    LOG.error("Failed to sync channel [{}]: {}", getId(), e);
                    onServerFailed();
                }
            } else {
                LOG.warn("Can't sync. Server is {}, socket is {}", currentServer, client.isConnected() ? "connected" : "disconnected");
            }
        }
    }

    @Override
    public void syncAck(TransportType type) {
        LOG.info("Adding sync acknowledgement for type {} as a regular sync for channel [{}]", type, getId());
        syncAck(Collections.singleton(type));
    }

    @Override
    public void syncAck(Set<TransportType> types) {
        synchronized (this) {
            if (channelState != State.OPENED) {
                LOG.info("First KaaSync message received and processed for channel [{}]", getId());
                channelState = State.OPENED;
                failoverManager.onServerConnected(currentServer);
                LOG.debug("There are pending requests for channel [{}] -> starting sync", getId());
                syncAll();
            } else {
                LOG.debug("Acknowledgment is pending for channel [{}] -> starting sync", getId());
                if (types.size() == 1) {
                    sync(types.iterator().next());
                } else {
                    syncAll();
                }
            }
        }
    }

    @Override
    public synchronized void setDemultiplexer(KaaDataDemultiplexer demultiplexer) {
        this.demultiplexer = demultiplexer;

    }

    @Override
    public synchronized void setMultiplexer(KaaDataMultiplexer multiplexer) {
        this.multiplexer = multiplexer;
    }


    @Override
    public synchronized void setServer(TransportConnectionInfo server) {
        LOG.info("Setting server [{}] for channel [{}]", server, getId());

        if (server == null) {
            LOG.warn("Server is null for Channel [{}].", getId());
            return;
        }

        if (channelState == State.SHUTDOWN) {
            LOG.info("Can't set server. Channel [{}] is down", getId());
            return;
        }


        IPTransportInfo oldServer = currentServer;
        this.currentServer = new IPTransportInfo(server);

        //create mqtt client to communicate with server
        this.client = new KaaMqttClient(getBroker(), state.getEndpointKeyHash().getKeyHash());
        client.onConnectionLost(connectionLostListener);
        client.onMessageArrived(messageArrivedListener);

        if (channelState != State.PAUSE) {
            if (executor == null) {
                executor = createExecutor();
            }
            if (oldServer == null
                    || !client.isConnected()
                    || !oldServer.getHost().equals(currentServer.getHost())
                    || oldServer.getPort() != currentServer.getPort()) {
                LOG.info("New server's: {} host or ip is different from the old {}, reconnecting", currentServer, oldServer);
                closeConnection();
                scheduleOpenConnectionTask(0);
            }
        } else {
            LOG.info("Can't start new session. Channel [{}] is paused", getId());
        }
    }

    @Override
    public TransportConnectionInfo getServer() {
        return currentServer;
    }

    @Override
    public void setConnectivityChecker(ConnectivityChecker checker) {
        connectivityChecker = checker;
    }

    @Override
    public synchronized void shutdown() {
        LOG.info("Shutting down...");
        channelState = State.SHUTDOWN;
        closeConnection();
        destroyExecutor();
    }

    @Override
    public synchronized void pause() {
        if (channelState != State.PAUSE) {
            LOG.info("Pausing...");
            channelState = State.PAUSE;
            closeConnection();
            destroyExecutor();
        }
    }

    private synchronized void destroyExecutor() {
        if (executor != null) {
            executor.shutdownNow();
            isOpenConnectionScheduled = false;
            executor = null;
        }
    }

    @Override
    public synchronized void resume() {
        if (channelState == State.PAUSE) {
            LOG.info("Resuming...");
            channelState = State.CLOSED;
            if (executor == null) {
                executor = createExecutor();
            }
            scheduleOpenConnectionTask(0);
        }
    }

    @Override
    public String getId() {
        return CHANNEL_ID;
    }

    @Override
    public TransportProtocolId getTransportProtocolId() {
        return TransportProtocolIdConstants.TCP_TRANSPORT_ID;
    }

    @Override
    public ServerType getServerType() {
        return ServerType.OPERATIONS;
    }

    @Override
    public Map<TransportType, ChannelDirection> getSupportedTransportTypes() {
        return SUPPORTED_TYPES;
    }

    private enum State {
        SHUTDOWN, PAUSE, CLOSED, OPENED
    }
}
