/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.plc4x.java.ads;

import org.apache.plc4x.java.ads.configuration.AdsConfiguration;
import org.apache.plc4x.java.ads.discovery.readwrite.*;
import org.apache.plc4x.java.ads.discovery.readwrite.AmsNetId;
import org.apache.plc4x.java.ads.discovery.readwrite.Constants;
import org.apache.plc4x.java.ads.model.AdsSubscriptionHandle;
import org.apache.plc4x.java.ads.readwrite.*;
import org.apache.plc4x.java.ads.tag.AdsTag;
import org.apache.plc4x.java.ads.tag.AdsTagHandler;
import org.apache.plc4x.java.ads.tag.DirectAdsStringTag;
import org.apache.plc4x.java.ads.tag.DirectAdsTag;
import org.apache.plc4x.java.ads.tag.SymbolicAdsTag;
import org.apache.plc4x.java.api.authentication.PlcUsernamePasswordAuthentication;
import org.apache.plc4x.java.api.exceptions.PlcConnectionException;
import org.apache.plc4x.java.api.exceptions.PlcException;
import org.apache.plc4x.java.api.exceptions.PlcInvalidTagException;
import org.apache.plc4x.java.api.exceptions.PlcRuntimeException;
import org.apache.plc4x.java.api.messages.*;
import org.apache.plc4x.java.api.model.*;
import org.apache.plc4x.java.api.types.ConnectionStateChangeType;
import org.apache.plc4x.java.api.types.PlcResponseCode;
import org.apache.plc4x.java.api.types.PlcSubscriptionType;
import org.apache.plc4x.java.api.types.PlcValueType;
import org.apache.plc4x.java.api.value.PlcValue;
import org.apache.plc4x.java.spi.buffers.api.ReadBuffer;
import org.apache.plc4x.java.spi.buffers.api.WithOption;
import org.apache.plc4x.java.spi.buffers.api.exceptions.BufferException;
import org.apache.plc4x.java.spi.buffers.bytebased.ReadBufferByteBased;
import org.apache.plc4x.java.spi.buffers.bytebased.WithByteBasedOption;
import org.apache.plc4x.java.spi.buffers.bytebased.WriteBufferByteBased;
import org.apache.plc4x.java.spi.drivers.ConnectionBase;
import org.apache.plc4x.java.spi.drivers.exceptions.MessageCodecException;
import org.apache.plc4x.java.spi.drivers.messages.*;
import org.apache.plc4x.java.spi.drivers.messages.items.DefaultPlcResponseItem;
import org.apache.plc4x.java.spi.drivers.messages.items.PlcResponseItem;
import org.apache.plc4x.java.spi.drivers.model.DefaultArrayInfo;
import org.apache.plc4x.java.spi.drivers.tags.PlcTagHandler;
import org.apache.plc4x.java.spi.transports.api.TransportInstance;
import org.apache.plc4x.java.spi.values.DefaultPlcValueHandler;
import org.apache.plc4x.java.spi.values.PlcList;
import org.apache.plc4x.java.spi.values.PlcSTRING;
import org.apache.plc4x.java.spi.values.PlcStruct;
import org.apache.plc4x.java.spi.values.PlcUDINT;
import org.apache.plc4x.java.spi.values.PlcValueHandler;
import org.apache.plc4x.java.utils.auditlog.api.AuditLog;
import org.apache.plc4x.java.utils.auditlog.api.AuditLogEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * ADS over TCP connection.
 *
 * <p>This is a direct port of the original Netty/SPI1-based AdsProtocolLogic to the SPI3
 * connection model — request/response correlation now happens via the invoke-id keyed
 * pendingRequests map, single-flight throttling via {@code executeThrottled} (max-concurrent = 1),
 * and unsolicited AdsDeviceNotificationRequest packets are dispatched directly to registered
 * subscription consumers.
 *
 * <p>Logic semantics (table loading, sum-up reads/writes, recursive subscribe/unsubscribe,
 * symbol/datatype invalidation on online-/symbol-version change, browse, route-setup) are
 * preserved from the original implementation, including its known TODOs around symbolic-tag
 * resolution.
 */
public class AdsTcpConnection extends ConnectionBase<AdsConfiguration> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AdsTcpConnection.class);

    private final AtomicLong invokeIdGenerator = new AtomicLong(1);
    private AdsTcpMessageCodec messageCodec;
    private final Map<Long, CompletableFuture<AmsTCPPacket>> pendingRequests = new ConcurrentHashMap<>();

    private final Map<DefaultPlcConsumerRegistration, Consumer<PlcSubscriptionEvent>> consumers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<SymbolicAdsTag, CompletableFuture<Void>> pendingResolutionRequests = new ConcurrentHashMap<>();

    private String adsVersion;
    private String deviceName;
    private int symbolVersion;
    private long onlineVersion;
    private final Map<String, AdsSymbolTableEntry> symbolTable = new HashMap<>();
    private final Map<String, AdsDataTypeTableEntry> dataTypeTable = new HashMap<>();
    private final ReentrantLock invalidationLock = new ReentrantLock();

    public AdsTcpConnection(AdsConfiguration configuration, TransportInstance<?> transportInstance, AuditLog auditLog) {
        super(configuration, transportInstance, auditLog);
    }

    @Override
    protected PlcTagHandler getTagHandler() {
        return new AdsTagHandler();
    }

    @Override
    protected PlcValueHandler getValueHandler() {
        return new DefaultPlcValueHandler();
    }

    @Override
    protected int getMaxConcurrentRequests() {
        return 1;
    }

    @Override
    public boolean isConnected() {
        return messageCodec != null && messageCodec.isOpen();
    }

    @Override
    protected void onConnect() throws PlcConnectionException {
        messageCodec = new AdsTcpMessageCodec(transportInstance, this::handleIncomingMessage);

        startReceiving(() -> {
            try {
                messageCodec.processIncomingData();
            } catch (MessageCodecException e) {
                LOGGER.error("Error processing incoming ADS data", e);
            }
        });

        // Step 1: optional AMS route setup if username/password authentication is provided.
        // (The original implementation triggered this from onConnect; we don't have access to
        //  authentication here as ConnectionBase doesn't propagate it. Skipping unless needed.)
        // If auth needed: see setupAmsRoute() — currently disabled because authentication is
        // not surfaced through the SPI3 ConnectionBase.

        // Step 2: when the configuration asks us to load the symbol/data-type tables, do so;
        //         otherwise mark the connection as connected immediately.
        if (!getConfiguration().isLoadSymbolAndDataTypeTables()) {
            fireConnectedAuditAndState();
            return;
        }

        try {
            doConnectHandshake().get(getConfiguration().getTimeoutRequest() * 5L, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            try {
                close();
            } catch (Exception ignored) {
                // ignore
            }
            throw new PlcConnectionException("Error during ADS connect handshake", e);
        }

        fireConnectedAuditAndState();
    }

    private void fireConnectedAuditAndState() {
        LOGGER.info("ADS TCP connection established");
        if (auditLog.isEnabled()) {
            auditLog.write(AuditLogEventType.CONNECT, "ADS TCP connection established");
        }
        fireConnectionStateChanged(ConnectionStateChangeType.CONNECTED, null);
    }

    private CompletableFuture<Void> doConnectHandshake() {
        // ReadDeviceInfo
        AmsPacket readDeviceInfoRequest = new AdsReadDeviceInfoRequest(
            getConfiguration().getTargetAmsNetId(), DefaultAmsPorts.RUNTIME_SYSTEM_01.getValue(),
            getConfiguration().getSourceAmsNetId(), 800, ReturnCode.OK, getInvokeId());

        return sendAmsRequest(readDeviceInfoRequest, AdsReadDeviceInfoResponse.class).thenCompose(readDeviceInfoResponse -> {
            if (readDeviceInfoResponse.getResult() != ReturnCode.OK) {
                return CompletableFuture.failedFuture(new PlcConnectionException(
                    "Error reading device info. Got: " + readDeviceInfoResponse.getResult()));
            }
            adsVersion = String.format("%d.%d.%d", readDeviceInfoResponse.getMajorVersion(),
                readDeviceInfoResponse.getMinorVersion(), readDeviceInfoResponse.getVersion());
            deviceName = new String(readDeviceInfoResponse.getDevice()).trim();

            // Read online version (sym-by-name "TwinCAT_SystemInfoVarList._AppInfo.OnlineChangeCnt")
            AmsPacket readOnlineVersionRequest = new AdsReadWriteRequest(
                getConfiguration().getTargetAmsNetId(), DefaultAmsPorts.RUNTIME_SYSTEM_01.getValue(),
                getConfiguration().getSourceAmsNetId(), 800, ReturnCode.OK, getInvokeId(),
                ReservedIndexGroups.ADSIGRP_SYM_VALBYNAME.getValue(), 0L, 4L, null,
                "TwinCAT_SystemInfoVarList._AppInfo.OnlineChangeCnt".getBytes(StandardCharsets.UTF_8));
            return sendAmsRequest(readOnlineVersionRequest, AdsReadWriteResponse.class);
        }).thenCompose(readOnlineVersionResponse -> {
            if (readOnlineVersionResponse.getResult() != ReturnCode.OK) {
                return CompletableFuture.failedFuture(new PlcConnectionException(
                    "Error reading online version number. Got: " + readOnlineVersionResponse.getResult()));
            }
            try {
                ReadBuffer rb = new ReadBufferByteBased(readOnlineVersionResponse.getData());
                onlineVersion = rb.readUnsignedLong(32, WithOption.WithUnsignedIntegerEncoding("unsigned-binary"));
            } catch (BufferException e) {
                return CompletableFuture.failedFuture(new PlcConnectionException(
                    "Error parsing online version number response.", e));
            }
            // Read symbol version (Group 0xF008, Offset 0, length 1)
            AmsPacket readSymbolVersionRequest = new AdsReadRequest(
                getConfiguration().getTargetAmsNetId(), DefaultAmsPorts.RUNTIME_SYSTEM_01.getValue(),
                getConfiguration().getSourceAmsNetId(), 800, ReturnCode.OK, getInvokeId(),
                ReservedIndexGroups.ADSIGRP_SYM_VERSION.getValue(), 0L, 1L);
            return sendAmsRequest(readSymbolVersionRequest, AdsReadResponse.class);
        }).thenCompose(readSymbolVersionResponse -> {
            if (readSymbolVersionResponse.getResult() != ReturnCode.OK) {
                return CompletableFuture.failedFuture(new PlcConnectionException(
                    "Error reading symbol version number. Got: " + readSymbolVersionResponse.getResult()));
            }
            try {
                ReadBuffer rb = new ReadBufferByteBased(readSymbolVersionResponse.getData());
                symbolVersion = rb.readUnsignedInt(8, WithOption.WithUnsignedIntegerEncoding("unsigned-binary"));
            } catch (BufferException e) {
                return CompletableFuture.failedFuture(new PlcConnectionException(
                    "Error parsing symbol version number response.", e));
            }
            LOGGER.debug("Fetching sizes of symbol and datatype table sizes.");
            return readSymbolTableAndDatatypeTable();
        });
    }

    private CompletableFuture<Void> readSymbolTableAndDatatypeTable() {
        // Read sizes
        AmsPacket sizesRequest = new AdsReadRequest(
            getConfiguration().getTargetAmsNetId(), getConfiguration().getTargetAmsPort(),
            getConfiguration().getSourceAmsNetId(), getConfiguration().getSourceAmsPort(), ReturnCode.OK, getInvokeId(),
            ReservedIndexGroups.ADSIGRP_SYMBOL_AND_DATA_TYPE_SIZES.getValue(), 0L, 24L);

        return sendAmsRequest(sizesRequest, AdsReadResponse.class).thenCompose(sizesResponse -> {
            if (sizesResponse.getResult() != ReturnCode.OK) {
                return CompletableFuture.failedFuture(new PlcException(
                    "Reading data type and symbol table sizes failed: " + sizesResponse.getResult()));
            }
            final AdsTableSizes sizes;
            try {
                ReadBuffer readBuffer = new ReadBufferByteBased(sizesResponse.getData());
                sizes = AdsTableSizes.staticParse(readBuffer);
            } catch (BufferException e) {
                return CompletableFuture.failedFuture(new PlcException("Error loading the table sizes", e));
            }
            LOGGER.debug("PLC contains {} symbols and {} data-types", sizes.getSymbolCount(), sizes.getDataTypeCount());

            // Read data type table
            AmsPacket dataTypeRequest = new AdsReadRequest(
                getConfiguration().getTargetAmsNetId(), getConfiguration().getTargetAmsPort(),
                getConfiguration().getSourceAmsNetId(), getConfiguration().getSourceAmsPort(), ReturnCode.OK, getInvokeId(),
                ReservedIndexGroups.ADSIGRP_DATA_TYPE_TABLE_UPLOAD.getValue(), 0L, sizes.getDataTypeLength());
            return sendAmsRequest(dataTypeRequest, AdsReadResponse.class).thenCompose(dataTypeResponse -> {
                if (dataTypeResponse.getResult() != ReturnCode.OK) {
                    return CompletableFuture.failedFuture(new PlcException(
                        "Reading data type table failed: " + dataTypeResponse.getResult()));
                }
                ReadBuffer rb = new ReadBufferByteBased(dataTypeResponse.getData());
                for (int i = 0; i < sizes.getDataTypeCount(); i++) {
                    try {
                        AdsDataTypeTableEntry entry = AdsDataTypeTableEntry.staticParse(rb);
                        dataTypeTable.put(entry.getMainName(), entry);
                    } catch (BufferException e) {
                        return CompletableFuture.failedFuture(new RuntimeException(e));
                    }
                }

                // Read symbol table
                AmsPacket symbolRequest = new AdsReadRequest(
                    getConfiguration().getTargetAmsNetId(), getConfiguration().getTargetAmsPort(),
                    getConfiguration().getSourceAmsNetId(), getConfiguration().getSourceAmsPort(), ReturnCode.OK, getInvokeId(),
                    ReservedIndexGroups.ADSIGRP_SYM_UPLOAD.getValue(), 0L, sizes.getSymbolLength());
                return sendAmsRequest(symbolRequest, AdsReadResponse.class).thenCompose(symbolResponse -> {
                    if (symbolResponse.getResult() != ReturnCode.OK) {
                        return CompletableFuture.failedFuture(new PlcException(
                            "Reading symbol table failed: " + symbolResponse.getResult()));
                    }
                    ReadBuffer rb2 = new ReadBufferByteBased(symbolResponse.getData());
                    for (int i = 0; i < sizes.getSymbolCount(); i++) {
                        try {
                            AdsSymbolTableEntry entry = AdsSymbolTableEntry.staticParse(rb2);
                            symbolTable.put(entry.getName(), entry);
                        } catch (BufferException e) {
                            return CompletableFuture.failedFuture(new RuntimeException(e));
                        }
                    }

                    // Subscribe to online + symbol version invalidation events.
                    LinkedHashMap<String, PlcSubscriptionTag> subTags = new LinkedHashMap<>();
                    subTags.put("onlineVersion", new DefaultPlcSubscriptionTag(
                        PlcSubscriptionType.CHANGE_OF_STATE,
                        new SymbolicAdsTag("TwinCAT_SystemInfoVarList._AppInfo.OnlineChangeCnt",
                            PlcValueType.UDINT, Collections.emptyList()),
                        java.time.Duration.ofMillis(1000)));
                    subTags.put("symbolVersion", new DefaultPlcSubscriptionTag(
                        PlcSubscriptionType.CHANGE_OF_STATE,
                        new DirectAdsTag(0xF008, 0x0000, "USINT", 1),
                        java.time.Duration.ofMillis(1000)));

                    Consumer<PlcSubscriptionEvent> consumer = ev -> {
                        for (String tagName : ev.getTagNames()) {
                            switch (tagName) {
                                case "onlineVersion": {
                                    long newVersion = ev.getPlcValue("onlineVersion").getLong();
                                    if (onlineVersion != newVersion) {
                                        if (invalidationLock.tryLock()) {
                                            LOGGER.info("Detected change of the 'online-version', invalidating data type and symbol information.");
                                            readSymbolTableAndDatatypeTable().whenComplete((u, t) -> {
                                                if (t != null) LOGGER.error("Error reloading data type and symbol data", t);
                                                invalidationLock.unlock();
                                            });
                                        }
                                    }
                                    break;
                                }
                                case "symbolVersion": {
                                    int newVersion = ev.getPlcValue("symbolVersion").getInteger();
                                    if (symbolVersion != newVersion) {
                                        if (invalidationLock.tryLock()) {
                                            LOGGER.info("Detected change of the 'symbol-version', invalidating data type and symbol information.");
                                            readSymbolTableAndDatatypeTable().whenComplete((u, t) -> {
                                                if (t != null) LOGGER.error("Error reloading data type and symbol data", t);
                                                invalidationLock.unlock();
                                            });
                                        }
                                    }
                                    break;
                                }
                            }
                        }
                    };

                    DefaultPlcSubscriptionRequest bootstrapReq = new DefaultPlcSubscriptionRequest(
                        this, subTags, consumer, Collections.emptyMap());
                    return subscribe(bootstrapReq).thenAccept(resp -> {
                        // Wire up the global consumer to the returned handles.
                        registerConsumer(consumer, resp.getSubscriptionHandles());
                    });
                });
            });
        });
    }

    @Override
    public void close() throws Exception {
        stopReceiving();
        if (messageCodec != null) {
            messageCodec.close();
        }
        pendingRequests.values().forEach(future ->
            future.completeExceptionally(new PlcRuntimeException("Connection closed")));
        pendingRequests.clear();
        super.close();
        LOGGER.info("ADS TCP connection closed");
        fireConnectionStateChanged(ConnectionStateChangeType.DISCONNECTED, null);
    }

    @Override
    protected void onTransportDisconnected(Throwable cause) {
        super.onTransportDisconnected(cause);
        fireConnectionStateChanged(ConnectionStateChangeType.CONNECTION_LOST,
            cause != null ? cause.getMessage() : "Connection closed by remote");

        PlcRuntimeException exception = new PlcRuntimeException(
            cause != null ? "Connection lost: " + cause.getMessage() : "Connection closed by remote", cause);
        if (!pendingRequests.isEmpty()) {
            LOGGER.warn("Failing {} pending requests due to transport disconnect", pendingRequests.size());
            pendingRequests.values().forEach(f -> f.completeExceptionally(exception));
            pendingRequests.clear();
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Request/response correlation
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    private long getInvokeId() {
        long invokeId = invokeIdGenerator.getAndIncrement();
        if (invokeIdGenerator.get() == 0xFFFFFFFFL) {
            invokeIdGenerator.set(1);
        }
        return invokeId;
    }

    private void handleIncomingMessage(AmsTCPPacket packet) {
        AmsPacket userdata = packet.getUserdata();

        // Unsolicited device-notification: dispatch to consumers.
        if (userdata instanceof AdsDeviceNotificationRequest notification) {
            try {
                dispatchDeviceNotification(notification);
            } catch (Exception e) {
                LOGGER.error("Error dispatching ADS device notification", e);
            }
            return;
        }

        long invokeId = userdata.getInvokeId();
        if (auditLog.isEnabled()) {
            auditLog.write(AuditLogEventType.INCOMING_MESSAGE, "Received ADS response, invokeId=" + invokeId);
        }
        CompletableFuture<AmsTCPPacket> future = pendingRequests.remove(invokeId);
        if (future != null) {
            future.complete(packet);
        } else {
            LOGGER.warn("Received response for unknown invoke ID: {}", invokeId);
        }
    }

    private CompletableFuture<AmsTCPPacket> sendAmsTCPPacket(AmsTCPPacket request, long invokeId) {
        CompletableFuture<AmsTCPPacket> response = new CompletableFuture<>();
        pendingRequests.put(invokeId, response);
        try {
            if (auditLog.isEnabled()) {
                auditLog.write(AuditLogEventType.OUTGOING_MESSAGE, "Sending ADS request, invokeId=" + invokeId);
            }
            messageCodec.send(request);
        } catch (MessageCodecException e) {
            pendingRequests.remove(invokeId);
            response.completeExceptionally(new PlcRuntimeException("Failed to send request", e));
            return response;
        }
        long timeoutMs = getConfiguration().getTimeoutRequest();
        response.orTimeout(timeoutMs, TimeUnit.MILLISECONDS).whenComplete((r, e) -> {
            if (e instanceof TimeoutException) {
                pendingRequests.remove(invokeId);
            }
        });
        return response;
    }

    @SuppressWarnings("unchecked")
    private <T extends AmsPacket> CompletableFuture<T> sendAmsRequest(AmsPacket request, Class<T> expectedResponseType) {
        long invokeId = request.getInvokeId();
        AmsTCPPacket tcpPacket = new AmsTCPPacket(request);
        return executeThrottled(() -> sendAmsTCPPacket(tcpPacket, invokeId).thenApply(resp -> {
            AmsPacket userdata = resp.getUserdata();
            if (!expectedResponseType.isInstance(userdata)) {
                throw new PlcRuntimeException("Unexpected response type: " + userdata.getClass().getSimpleName()
                    + " (expected " + expectedResponseType.getSimpleName() + ")");
            }
            return (T) userdata;
        }));
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Ping
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Override
    protected CompletableFuture<PlcPingResponse> onPing(PlcPingRequest pingRequest) {
        AmsPacket pingPacket = new AdsReadDeviceInfoRequest(
            getConfiguration().getTargetAmsNetId(), DefaultAmsPorts.RUNTIME_SYSTEM_01.getValue(),
            getConfiguration().getSourceAmsNetId(), 800, ReturnCode.OK, getInvokeId());
        return sendAmsRequest(pingPacket, AdsReadDeviceInfoResponse.class)
            .thenApply(r -> (PlcPingResponse) new DefaultPlcPingResponse(pingRequest, PlcResponseCode.OK));
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Read
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Override
    protected CompletableFuture<PlcReadResponse> onRead(PlcReadRequest readRequest) {
        return getDirectAddresses(readRequest.getTags()).thenCompose(resolvedTags -> {
            if (resolvedTags == null) {
                return CompletableFuture.failedFuture(new PlcException("Tags are null"));
            }
            return executeRead(readRequest, resolvedTags);
        });
    }

    private CompletableFuture<PlcReadResponse> executeRead(PlcReadRequest readRequest, Map<AdsTag, DirectAdsTag> resolvedTags) {
        if (resolvedTags.size() == 1) {
            AdsTag adsTag = (AdsTag) readRequest.getTags().getFirst();
            DirectAdsTag directAdsTag = resolvedTags.get(adsTag);
            return singleRead(readRequest, directAdsTag);
        } else {
            // TODO: Check if the version of the remote station is at least TwinCAT v2.11 Build >= 1550 otherwise split up into single item requests.
            return multiRead(readRequest, resolvedTags);
        }
    }

    private CompletableFuture<PlcReadResponse> singleRead(PlcReadRequest readRequest, DirectAdsTag directAdsTag) {
        if (directAdsTag == null) {
            return CompletableFuture.completedFuture(new DefaultPlcReadResponse(readRequest, Collections.singletonMap(
                readRequest.getTagNames().iterator().next(),
                new DefaultPlcResponseItem<>(PlcResponseCode.NOT_FOUND, null))));
        }
        Optional<AdsDataTypeTableEntry> dataTypeOpt = getDataTypeTableEntry(directAdsTag.getPlcDataType());
        if (dataTypeOpt.isEmpty()) {
            return CompletableFuture.failedFuture(new PlcRuntimeException("couldn't find datatype: " + directAdsTag.getPlcDataType()));
        }
        long size = dataTypeOpt.get().getSize();

        AmsPacket request = new AdsReadRequest(
            getConfiguration().getTargetAmsNetId(), getConfiguration().getTargetAmsPort(),
            getConfiguration().getSourceAmsNetId(), getConfiguration().getSourceAmsPort(), ReturnCode.OK, getInvokeId(),
            directAdsTag.getIndexGroup(), directAdsTag.getIndexOffset(), size * directAdsTag.getNumberOfElements());

        return sendAmsRequest(request, AdsReadResponse.class).thenApply(response -> {
            if (response.getResult() == ReturnCode.OK) {
                return convertToPlc4xReadResponse(readRequest,
                    Map.of((AdsTag) readRequest.getTags().get(0), directAdsTag), response);
            }
            throw new PlcRuntimeException("Unexpected return code " + response.getResult());
        });
    }

    private CompletableFuture<PlcReadResponse> multiRead(PlcReadRequest readRequest, Map<AdsTag, DirectAdsTag> resolvedTags) {
        List<AdsTag> successfullyResolvedTags = readRequest.getTagNames().stream()
            .map(name -> (AdsTag) readRequest.getTag(name))
            .filter(adsTag -> resolvedTags.get(adsTag) != null)
            .collect(Collectors.toList());

        long expectedResponseDataSize = successfullyResolvedTags.stream().mapToLong(adsTag -> {
            DirectAdsTag d = resolvedTags.get(adsTag);
            Optional<AdsDataTypeTableEntry> dt = getDataTypeTableEntry(d.getPlcDataType());
            if (dt.isEmpty()) {
                LOGGER.warn("couldn't find datatype: {}", d.getPlcDataType());
                return 0;
            }
            return 4 + (dt.get().getSize() * d.getNumberOfElements());
        }).sum();

        AmsPacket request = new AdsReadWriteRequest(
            getConfiguration().getTargetAmsNetId(), getConfiguration().getTargetAmsPort(),
            getConfiguration().getSourceAmsNetId(), getConfiguration().getSourceAmsPort(), ReturnCode.OK, getInvokeId(),
            ReservedIndexGroups.ADSIGRP_MULTIPLE_READ.getValue(), (long) successfullyResolvedTags.size(),
            expectedResponseDataSize,
            successfullyResolvedTags.stream().map(t -> {
                DirectAdsTag d = resolvedTags.get(t);
                Optional<AdsDataTypeTableEntry> dt = getDataTypeTableEntry(d.getPlcDataType());
                long size = dt.map(AdsDataTypeTableEntry::getSize).orElse(0L);
                return new AdsMultiRequestItemRead(d.getIndexGroup(), d.getIndexOffset(), size * d.getNumberOfElements());
            }).collect(Collectors.toList()),
            null);

        return sendAmsRequest(request, AdsReadWriteResponse.class).thenApply(response -> {
            if (response.getResult() == ReturnCode.OK) {
                return convertToPlc4xReadResponse(readRequest, resolvedTags, response);
            } else if (response.getResult() == ReturnCode.ADSERR_DEVICE_INVALIDSIZE) {
                throw new PlcRuntimeException("The parameter size was not correct (Internal error)");
            } else {
                throw new PlcRuntimeException("Unexpected result " + response.getResult());
            }
        });
    }

    private PlcReadResponse convertToPlc4xReadResponse(PlcReadRequest readRequest, Map<AdsTag, DirectAdsTag> resolvedTags, AmsPacket adsData) {
        ReadBuffer readBuffer = null;
        Map<String, PlcResponseCode> responseCodes = new HashMap<>();

        if (adsData instanceof AdsReadResponse adsReadResponse) {
            readBuffer = new ReadBufferByteBased(adsReadResponse.getData(), WithOption.WithUnsignedIntegerEncoding("unsigned-binary"), WithOption.WithSignedIntegerEncoding("twos-complement"), WithByteBasedOption.WithByteOrder("LITTLE_ENDIAN"));
            responseCodes.put(readRequest.getTagNames().getFirst(), parsePlcResponseCode(adsReadResponse.getResult()));
        } else if (adsData instanceof AdsReadWriteResponse adsReadWriteResponse) {
            readBuffer = new ReadBufferByteBased(adsReadWriteResponse.getData(), WithOption.WithUnsignedIntegerEncoding("unsigned-binary"), WithOption.WithSignedIntegerEncoding("twos-complement"), WithByteBasedOption.WithByteOrder("LITTLE_ENDIAN"));
            for (String tagName : readRequest.getTagNames()) {
                try {
                    PlcTag tag = readRequest.getTag(tagName);
                    if (resolvedTags.get((AdsTag) tag) != null) {
                        ReturnCode result = ReturnCode.enumForValue(readBuffer.readUnsignedLong(32));
                        responseCodes.put(tagName, parsePlcResponseCode(result));
                    } else {
                        responseCodes.put(tagName, PlcResponseCode.INVALID_ADDRESS);
                    }
                } catch (BufferException e) {
                    responseCodes.put(tagName, PlcResponseCode.INTERNAL_ERROR);
                }
            }
        }

        if (readBuffer == null) {
            return null;
        }
        Map<String, PlcResponseItem<PlcValue>> values = new LinkedHashMap<>();
        for (String tagName : readRequest.getTagNames()) {
            if (responseCodes.get(tagName) != PlcResponseCode.OK) {
                values.put(tagName, new DefaultPlcResponseItem<>(responseCodes.get(tagName), null));
            } else {
                DirectAdsTag d = resolvedTags.get((AdsTag) readRequest.getTag(tagName));
                values.put(tagName, parseResponseItem(d, readBuffer));
            }
        }
        return new DefaultPlcReadResponse(readRequest, values);
    }

    private PlcResponseCode parsePlcResponseCode(ReturnCode adsResult) {
        if (adsResult == ReturnCode.OK) return PlcResponseCode.OK;
        if (adsResult == ReturnCode.ADSERR_DEVICE_SYMBOLNOTFOUND) return PlcResponseCode.INVALID_ADDRESS;
        return PlcResponseCode.INTERNAL_ERROR;
    }

    private PlcResponseItem<PlcValue> parseResponseItem(DirectAdsTag tag, ReadBuffer readBuffer) {
        try {
            Optional<AdsDataTypeTableEntry> dataTypeOpt = getDataTypeTableEntry(tag.getPlcDataType());
            if (dataTypeOpt.isEmpty()) {
                return new DefaultPlcResponseItem<>(PlcResponseCode.INTERNAL_ERROR, null);
            }
            AdsDataTypeTableEntry dataType = dataTypeOpt.get();
            PlcValueType plcValueType = getPlcValueTypeForAdsDataType(dataType);

            int strLen = 0;
            if (tag instanceof DirectAdsStringTag s) {
                strLen = s.getStringLength();
            }
            final int stringLength = strLen;
            if (tag.getNumberOfElements() == 1) {
                ReadBufferByteBased rb = (ReadBufferByteBased) readBuffer;
                int remainingBytes = (rb.getBytes().length) - (rb.getPositionInBits() / 8);
                int singleStringLength = Math.min(remainingBytes - 1, stringLength);
                return new DefaultPlcResponseItem<>(PlcResponseCode.OK,
                    parsePlcValue(plcValueType, dataType, singleStringLength, readBuffer));
            } else {
                PlcValue[] resultItems = IntStream.range(0, tag.getNumberOfElements()).mapToObj(i -> {
                    try {
                        return parsePlcValue(plcValueType, dataType, stringLength, readBuffer);
                    } catch (BufferException e) {
                        LOGGER.warn("Error parsing tag item of type: '{}' (at position {}})", tag.getPlcDataType(), i, e);
                        return null;
                    }
                }).toArray(PlcValue[]::new);
                return new DefaultPlcResponseItem<>(PlcResponseCode.OK, DefaultPlcValueHandler.of(tag, resultItems));
            }
        } catch (Exception e) {
            LOGGER.warn(String.format("Error parsing tag item of type: '%s'", tag.getPlcDataType()), e);
            return new DefaultPlcResponseItem<>(PlcResponseCode.INTERNAL_ERROR, null);
        }
    }

    private PlcValue parsePlcValue(PlcValueType plcValueType, AdsDataTypeTableEntry adsDataTypeTableEntry, int stringLength, ReadBuffer readBuffer) throws BufferException {
        switch (plcValueType) {
            case Struct:
                Map<String, PlcValue> properties = new HashMap<>();
                int startPos = (readBuffer.getPositionInBits() / 8);
                int curPos = 0;
                for (AdsDataTypeTableEntry child : adsDataTypeTableEntry.getChildren()) {
                    if (child.getOffset() > curPos) {
                        long skipBytes = child.getOffset() - curPos;
                        // Just advance the cursor — readBits is the raw bit-level read that
                        // doesn't depend on a signed/unsigned encoding being configured.
                        readBuffer.readBits((int) skipBytes * 8);
                    }
                    String propertyName = child.getMainName();
                    Optional<AdsDataTypeTableEntry> opt = getDataTypeTableEntry(child.getSecondaryName());
                    if (opt.isEmpty()) {
                        throw new BufferException(String.format("couldn't find datatype: %s", child.getSecondaryName()));
                    }
                    AdsDataTypeTableEntry propertyType = opt.get();
                    PlcValueType propertyPlcType = getPlcValueTypeForAdsDataType(propertyType);
                    int strLen = 0;
                    if ((propertyPlcType == PlcValueType.STRING) || (propertyPlcType == PlcValueType.WSTRING)) {
                        String n = propertyType.getMainName();
                        strLen = Integer.parseInt(n.substring(n.indexOf("(") + 1, n.indexOf(")")));
                    }
                    properties.put(propertyName, parsePlcValue(propertyPlcType, propertyType, strLen, readBuffer));
                    curPos = (readBuffer.getPositionInBits() / 8) - startPos;
                }
                return new PlcStruct(properties);
            case List:
                return parseArrayLevel(adsDataTypeTableEntry, adsDataTypeTableEntry.getArrayInfo(), readBuffer);
            default:
                return DataItem.staticParse(readBuffer, plcValueType, stringLength);
        }
    }

    private PlcValue parseArrayLevel(AdsDataTypeTableEntry adsDataTypeTableEntry, List<AdsDataTypeArrayInfo> arrayLayers, ReadBuffer readBuffer) throws BufferException {
        if (arrayLayers.isEmpty()) {
            String dataTypeName = adsDataTypeTableEntry.getMainName();
            dataTypeName = dataTypeName.substring(dataTypeName.lastIndexOf(" OF ") + 4);
            int stringLength = 0;
            if (dataTypeName.startsWith("STRING(")) {
                stringLength = Integer.parseInt(dataTypeName.substring(7, dataTypeName.length() - 1));
            } else if (dataTypeName.startsWith("WSTRING(")) {
                stringLength = Integer.parseInt(dataTypeName.substring(8, dataTypeName.length() - 1));
            }
            Optional<AdsDataTypeTableEntry> opt = getDataTypeTableEntry(dataTypeName);
            if (opt.isEmpty()) {
                throw new BufferException(String.format("couldn't find datatype: %s", dataTypeName));
            }
            AdsDataTypeTableEntry elementType = opt.get();
            return parsePlcValue(getPlcValueTypeForAdsDataType(elementType), elementType, stringLength, readBuffer);
        }
        List<PlcValue> elements = new ArrayList<>();
        List<AdsDataTypeArrayInfo> arrayInfo = adsDataTypeTableEntry.getArrayInfo();
        AdsDataTypeArrayInfo firstLayer = arrayInfo.get(0);
        for (int i = 0; i < firstLayer.getNumElements(); i++) {
            elements.add(parseArrayLevel(adsDataTypeTableEntry, arrayInfo.subList(1, arrayInfo.size()), readBuffer));
        }
        return new PlcList(elements);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Write
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Override
    protected CompletableFuture<PlcWriteResponse> onWrite(PlcWriteRequest writeRequest) {
        return getDirectAddresses(writeRequest.getTags()).thenCompose(resolvedTags -> {
            if (resolvedTags == null) {
                return CompletableFuture.failedFuture(new PlcException("Tags are null"));
            }
            return executeWrite(writeRequest, resolvedTags);
        });
    }

    private CompletableFuture<PlcWriteResponse> executeWrite(PlcWriteRequest writeRequest, Map<AdsTag, DirectAdsTag> resolvedTags) {
        if (resolvedTags.size() == 1) {
            AdsTag adsTag = (AdsTag) writeRequest.getTags().getFirst();
            DirectAdsTag directAdsTag = resolvedTags.get(adsTag);
            return singleWrite(writeRequest, directAdsTag);
        } else {
            // TODO: Check if the version of the remote station is at least TwinCAT v2.11 Build >= 1550 otherwise split up into single item requests.
            return multiWrite(writeRequest, resolvedTags);
        }
    }

    private CompletableFuture<PlcWriteResponse> singleWrite(PlcWriteRequest writeRequest, DirectAdsTag directAdsTag) {
        if (directAdsTag == null) {
            return CompletableFuture.completedFuture(new DefaultPlcWriteResponse(writeRequest, Collections.singletonMap(
                writeRequest.getTagNames().iterator().next(), PlcResponseCode.INVALID_ADDRESS)));
        }
        String tagName = writeRequest.getTagNames().iterator().next();
        PlcValue plcValue = writeRequest.getPlcValue(tagName);
        byte[] serializedValue;
        try {
            serializedValue = serializePlcValue(plcValue, directAdsTag.getPlcDataType());
        } catch (Exception e) {
            return CompletableFuture.failedFuture(new PlcException("Error serializing data tag value for tag '" + tagName + "'", e));
        }

        AmsPacket request = new AdsWriteRequest(
            getConfiguration().getTargetAmsNetId(), getConfiguration().getTargetAmsPort(),
            getConfiguration().getSourceAmsNetId(), getConfiguration().getSourceAmsPort(), ReturnCode.OK, getInvokeId(),
            directAdsTag.getIndexGroup(), directAdsTag.getIndexOffset(), serializedValue);
        return sendAmsRequest(request, AdsWriteResponse.class).thenApply(response -> {
            if (response.getResult() == ReturnCode.OK) {
                return convertToPlc4xWriteResponse(writeRequest,
                    Collections.singletonMap((AdsTag) writeRequest.getTag(tagName), directAdsTag), response);
            }
            throw new PlcRuntimeException("Unexpected return code " + response.getResult());
        });
    }

    private CompletableFuture<PlcWriteResponse> multiWrite(PlcWriteRequest writeRequest, Map<AdsTag, DirectAdsTag> resolvedTags) {
        int numTags = writeRequest.getTags().size();
        List<byte[]> serializedTags = new ArrayList<>(numTags);
        Map<DirectAdsTag, AdsDataTypeTableEntry> tagDatatypes = new LinkedHashMap<>(numTags);
        for (String tagName : writeRequest.getTagNames()) {
            AdsTag adsTag = (AdsTag) writeRequest.getTag(tagName);
            if (resolvedTags.get(adsTag) == null) continue;
            DirectAdsTag directAdsTag = resolvedTags.get(adsTag);
            PlcValue plcValue = writeRequest.getPlcValue(tagName);
            Optional<AdsDataTypeTableEntry> opt = getDataTypeTableEntry(directAdsTag.getPlcDataType());
            if (opt.isEmpty()) {
                return CompletableFuture.failedFuture(new PlcException("couldn't find datatype: " + directAdsTag.getPlcDataType()));
            }
            try {
                serializedTags.add(serializePlcValue(plcValue, directAdsTag.getPlcDataType()));
                tagDatatypes.put(directAdsTag, opt.get());
            } catch (Exception e) {
                return CompletableFuture.failedFuture(new PlcException("Error serializing data", e));
            }
        }

        int serializedSize = serializedTags.stream().mapToInt(b -> b.length).sum();
        WriteBufferByteBased writeBuffer = new WriteBufferByteBased(new byte[serializedSize]);
        for (byte[] s : serializedTags) {
            try {
                writeBuffer.writeBits(s.length * 8, s);
            } catch (BufferException e) {
                return CompletableFuture.failedFuture(new PlcException("Error serializing data", e));
            }
        }

        AmsPacket request = new AdsReadWriteRequest(
            getConfiguration().getTargetAmsNetId(), getConfiguration().getTargetAmsPort(),
            getConfiguration().getSourceAmsNetId(), getConfiguration().getSourceAmsPort(), ReturnCode.OK, getInvokeId(),
            ReservedIndexGroups.ADSIGRP_MULTIPLE_WRITE.getValue(), (long) serializedTags.size(),
            (long) numTags * 4,
            tagDatatypes.entrySet().stream().map(e -> new AdsMultiRequestItemWrite(
                e.getKey().getIndexGroup(), e.getKey().getIndexOffset(), e.getValue().getSize()))
                .collect(Collectors.toList()), writeBuffer.getBytes());

        return sendAmsRequest(request, AdsReadWriteResponse.class).thenApply(response -> {
            if (response.getResult() == ReturnCode.OK) {
                return convertToPlc4xWriteResponse(writeRequest, resolvedTags, response);
            }
            throw new PlcRuntimeException("Unexpected result " + response.getResult());
        });
    }

    private byte[] serializePlcValue(PlcValue plcValue, String datatypeName) throws BufferException {
        Optional<AdsDataTypeTableEntry> opt = getDataTypeTableEntry(datatypeName);
        if (opt.isEmpty()) {
            throw new BufferException("Could not find data type: " + datatypeName);
        }
        AdsDataTypeTableEntry dataType = opt.get();
        WriteBufferByteBased wb = new WriteBufferByteBased(new byte[(int) dataType.getSize()],
            WithByteBasedOption.WithByteOrder("LITTLE_ENDIAN"));
        serializeInternal(plcValue, dataType, dataType.getArrayInfo(), wb);
        return wb.getBytes();
    }

    private void serializeInternal(PlcValue contextValue, AdsDataTypeTableEntry dataType,
                                   List<AdsDataTypeArrayInfo> arrayInfo, WriteBufferByteBased writeBuffer) throws BufferException {
        if (!arrayInfo.isEmpty()) {
            if (!contextValue.isList()) {
                throw new BufferException("Expected a PlcList, but got a " + contextValue.getPlcValueType().name());
            }
            AdsDataTypeArrayInfo cur = arrayInfo.get(0);
            List<? extends PlcValue> list = contextValue.getList();
            if (cur.getNumElements() != list.size()) {
                throw new BufferException(String.format(
                    "Expected a PlcList of size %d, but got one of size %d", cur.getNumElements(), list.size()));
            }
            Optional<AdsDataTypeTableEntry> opt = getDataTypeTableEntry(dataType.getSecondaryName());
            if (opt.isEmpty()) {
                throw new BufferException("Could not find data type: " + dataType.getSecondaryName());
            }
            AdsDataTypeTableEntry childDataType = opt.get();
            for (PlcValue v : list) {
                serializeInternal(v, childDataType, arrayInfo.subList(1, arrayInfo.size()), writeBuffer);
            }
        } else if (!dataType.getChildren().isEmpty()) {
            if (!contextValue.isStruct()) {
                throw new BufferException("Expected a PlcStruct, but got a " + contextValue.getPlcValueType().name());
            }
            PlcStruct plcStruct = (PlcStruct) contextValue;
            int startPos = (writeBuffer.getPositionInBits() / 8);
            int curPos = 0;
            for (AdsDataTypeTableEntry child : dataType.getChildren()) {
                Optional<AdsDataTypeTableEntry> opt = getDataTypeTableEntry(child.getSecondaryName());
                if (opt.isEmpty()) {
                    throw new BufferException("Could not find data type: " + child.getSecondaryName());
                }
                AdsDataTypeTableEntry childDataType = opt.get();
                if (!plcStruct.hasKey(child.getMainName())) {
                    throw new BufferException("PlcStruct is missing a child with the name " + child.getMainName());
                }
                if (child.getOffset() > curPos) {
                    long fillBytes = child.getOffset() - curPos;
                    for (long i = 0; i < fillBytes; i++) {
                        writeBuffer.writeSignedByte(8, (byte) 0x00, WithOption.WithName("fillByte"));
                    }
                }
                PlcValue childValue = plcStruct.getValue(child.getMainName());
                serializeInternal(childValue, childDataType, childDataType.getArrayInfo(), writeBuffer);
                curPos = (writeBuffer.getPositionInBits() / 8) - startPos;
            }
        } else {
            PlcValueType plcValueType = getPlcValueTypeForAdsDataType(dataType);
            if (plcValueType == null) {
                throw new BufferException("Unsupported simple type: " + dataType.getMainName());
            }
            int stringLength = 0;
            if ((plcValueType == PlcValueType.STRING) || (plcValueType == PlcValueType.WSTRING)) {
                String n = dataType.getMainName();
                stringLength = Integer.parseInt(n.substring(n.indexOf("(") + 1, n.indexOf(")")));
            }
            DataItem.staticSerialize(writeBuffer, contextValue, plcValueType, stringLength);
        }
    }

    private PlcWriteResponse convertToPlc4xWriteResponse(PlcWriteRequest writeRequest, Map<AdsTag, DirectAdsTag> resolvedTags, AmsPacket adsData) {
        Map<String, PlcResponseCode> responseCodes = new HashMap<>();
        if (adsData instanceof AdsWriteResponse adsWriteResponse) {
            responseCodes.put(writeRequest.getTagNames().iterator().next(), parsePlcResponseCode(adsWriteResponse.getResult()));
        } else if (adsData instanceof AdsReadWriteResponse adsReadWriteResponse) {
            ReadBuffer readBuffer = new ReadBufferByteBased(adsReadWriteResponse.getData(), WithOption.WithUnsignedIntegerEncoding("unsigned-binary"), WithOption.WithSignedIntegerEncoding("twos-complement"), WithByteBasedOption.WithByteOrder("LITTLE_ENDIAN"));
            for (String tagName : writeRequest.getTagNames()) {
                AdsTag adsTag = (AdsTag) writeRequest.getTag(tagName);
                if (resolvedTags.get(adsTag) == null) {
                    responseCodes.put(tagName, PlcResponseCode.INVALID_ADDRESS);
                    continue;
                }
                try {
                    ReturnCode result = ReturnCode.enumForValue(readBuffer.readUnsignedLong(32));
                    responseCodes.put(tagName, parsePlcResponseCode(result));
                } catch (BufferException e) {
                    responseCodes.put(tagName, PlcResponseCode.INTERNAL_ERROR);
                }
            }
        }
        return new DefaultPlcWriteResponse(writeRequest, responseCodes);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Subscribe / Unsubscribe
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Override
    protected CompletableFuture<PlcSubscriptionResponse> onSubscribe(PlcSubscriptionRequest subscriptionRequest) {
        List<PlcTag> innerTags = subscriptionRequest.getTags().stream()
            .map(t -> ((PlcSubscriptionTag) t).getTag())
            .collect(Collectors.toList());

        return getDirectAddresses(innerTags).thenCompose(resolvedTags -> {
            if (resolvedTags == null) {
                return CompletableFuture.failedFuture(new PlcException("Tags are null"));
            }
            return executeSubscribe(subscriptionRequest, resolvedTags);
        });
    }

    private CompletableFuture<PlcSubscriptionResponse> executeSubscribe(PlcSubscriptionRequest subscriptionRequest, Map<AdsTag, DirectAdsTag> resolvedTags) {
        List<AmsTCPPacket> amsTCPPackets = new ArrayList<>();
        for (PlcSubscriptionTag t : subscriptionRequest.getTags()) {
            String dataTypeName = resolvedTags.get((AdsTag) t.getTag()).getPlcDataType();
            Optional<AdsDataTypeTableEntry> dataTypeOpt = getDataTypeTableEntry(dataTypeName);
            if (dataTypeOpt.isEmpty()) {
                amsTCPPackets.add(null);
                continue;
            }
            AdsDataTypeTableEntry dataType = dataTypeOpt.get();
            DirectAdsTag directAdsTag = getDirectAdsTagForSymbolicName(t.getTag());
            // TODO: We should implement multi-dimensional arrays here ...
            int numberOfElements = (t.getArrayInfo().isEmpty()) ? 1 : t.getArrayInfo().get(0).getSize();
            amsTCPPackets.add(new AmsTCPPacket(new AdsAddDeviceNotificationRequest(
                getConfiguration().getTargetAmsNetId(), getConfiguration().getTargetAmsPort(),
                getConfiguration().getSourceAmsNetId(), getConfiguration().getSourceAmsPort(), ReturnCode.OK, getInvokeId(),
                directAdsTag.getIndexGroup(), directAdsTag.getIndexOffset(),
                dataType.getSize() * numberOfElements,
                t.getPlcSubscriptionType() == PlcSubscriptionType.CYCLIC ? AdsTransMode.CYCLIC : AdsTransMode.ON_CHANGE,
                0L,
                t.getDuration().orElse(java.time.Duration.ZERO).toMillis())));
        }

        Map<String, PlcResponseItem<PlcSubscriptionHandle>> responses = new LinkedHashMap<>();
        CompletableFuture<PlcSubscriptionResponse> future = new CompletableFuture<>();
        Iterator<String> nameIt = subscriptionRequest.getTagNames().iterator();
        Iterator<AmsTCPPacket> packetIt = amsTCPPackets.iterator();
        subscribeRecursively(subscriptionRequest, nameIt, resolvedTags, responses, future, packetIt);
        return future;
    }

    private void subscribeRecursively(PlcSubscriptionRequest subscriptionRequest,
                                      Iterator<String> tagNames,
                                      Map<AdsTag, DirectAdsTag> resolvedTags,
                                      Map<String, PlcResponseItem<PlcSubscriptionHandle>> responses,
                                      CompletableFuture<PlcSubscriptionResponse> future,
                                      Iterator<AmsTCPPacket> amsTCPPackets) {
        if (!amsTCPPackets.hasNext()) {
            future.complete(new DefaultPlcSubscriptionResponse(subscriptionRequest, responses));
            return;
        }
        AmsTCPPacket packet = amsTCPPackets.next();
        boolean hasMorePackets = amsTCPPackets.hasNext();
        String tagName = tagNames.next();
        if (packet == null) {
            responses.put(tagName, new DefaultPlcResponseItem<>(PlcResponseCode.INTERNAL_ERROR, null));
            subscribeRecursively(subscriptionRequest, tagNames, resolvedTags, responses, future, amsTCPPackets);
            return;
        }
        long invokeId = packet.getUserdata().getInvokeId();
        executeThrottled(() -> sendAmsTCPPacket(packet, invokeId)).thenAccept(resp -> {
            AmsPacket userdata = resp.getUserdata();
            if (!(userdata instanceof AdsAddDeviceNotificationResponse response)) {
                future.completeExceptionally(new PlcException("Unexpected response type: " + userdata.getClass().getSimpleName()));
                return;
            }
            if (response.getResult() == ReturnCode.OK) {
                PlcSubscriptionTag subscriptionTag = subscriptionRequest.getTag(tagName);
                String dataTypeName = resolvedTags.get((AdsTag) subscriptionTag.getTag()).getPlcDataType();
                Optional<AdsDataTypeTableEntry> dt = getDataTypeTableEntry(dataTypeName);
                if (dt.isEmpty()) {
                    future.completeExceptionally(new PlcRuntimeException("Could not find data type: " + dataTypeName));
                    return;
                }
                responses.put(tagName, new DefaultPlcResponseItem<>(
                    parsePlcResponseCode(response.getResult()),
                    new AdsSubscriptionHandle(this, tagName, dt.get(), response.getNotificationHandle())));
                if (!hasMorePackets) {
                    future.complete(new DefaultPlcSubscriptionResponse(subscriptionRequest, responses));
                    return;
                }
                subscribeRecursively(subscriptionRequest, tagNames, resolvedTags, responses, future, amsTCPPackets);
            } else {
                if (response.getResult() == ReturnCode.ADSERR_DEVICE_INVALIDSIZE) {
                    future.completeExceptionally(new PlcException("The parameter size was not correct (Internal error)"));
                } else {
                    future.completeExceptionally(new PlcException("Unexpected result " + response.getResult()));
                }
            }
        }).exceptionally(t -> {
            future.completeExceptionally(t);
            return null;
        });
    }

    @Override
    protected CompletableFuture<PlcUnsubscriptionResponse> onUnsubscribe(PlcUnsubscriptionRequest unsubscriptionRequest) {
        List<Long> notificationHandles = new ArrayList<>();
        unsubscriptionRequest.getSubscriptionHandles().stream()
            .filter(handle -> handle instanceof AdsSubscriptionHandle)
            .map(handle -> (AdsSubscriptionHandle) handle)
            .forEach(adsHandle -> {
                notificationHandles.add(adsHandle.getNotificationHandle());
                consumers.keySet().stream()
                    .filter(reg -> reg.getSubscriptionHandles().contains(adsHandle))
                    .forEach(DefaultPlcConsumerRegistration::unregister);
            });

        List<AmsTCPPacket> amsTCPPackets = notificationHandles.stream().map(handle -> new AmsTCPPacket(
            new AdsDeleteDeviceNotificationRequest(
                getConfiguration().getTargetAmsNetId(), getConfiguration().getTargetAmsPort(),
                getConfiguration().getSourceAmsNetId(), getConfiguration().getSourceAmsPort(), ReturnCode.OK, getInvokeId(), handle)))
            .collect(Collectors.toList());

        CompletableFuture<PlcUnsubscriptionResponse> future = new CompletableFuture<>();
        Iterator<AmsTCPPacket> it = amsTCPPackets.iterator();
        unsubscribeRecursively(unsubscriptionRequest, future, it);
        return future;
    }

    private void unsubscribeRecursively(PlcUnsubscriptionRequest unsubscriptionRequest,
                                        CompletableFuture<PlcUnsubscriptionResponse> future,
                                        Iterator<AmsTCPPacket> amsTCPPackets) {
        if (!amsTCPPackets.hasNext()) {
            future.complete(new DefaultPlcUnsubscriptionResponse(unsubscriptionRequest));
            return;
        }
        AmsTCPPacket packet = amsTCPPackets.next();
        boolean hasMorePackets = amsTCPPackets.hasNext();
        long invokeId = packet.getUserdata().getInvokeId();
        executeThrottled(() -> sendAmsTCPPacket(packet, invokeId)).thenAccept(resp -> {
            AmsPacket userdata = resp.getUserdata();
            if (!(userdata instanceof AdsDeleteDeviceNotificationResponse response)) {
                future.completeExceptionally(new PlcException("Unexpected response type: " + userdata.getClass().getSimpleName()));
                return;
            }
            if (response.getResult() == ReturnCode.OK) {
                if (!hasMorePackets) {
                    future.complete(new DefaultPlcUnsubscriptionResponse(unsubscriptionRequest));
                    return;
                }
                unsubscribeRecursively(unsubscriptionRequest, future, amsTCPPackets);
            } else if (response.getResult() == ReturnCode.ADSERR_DEVICE_NOTIFYHNDINVALID) {
                future.completeExceptionally(new PlcException("The notification handle is invalid (Internal error)"));
            } else {
                future.completeExceptionally(new PlcException("Unexpected result " + response.getResult()));
            }
        }).exceptionally(t -> {
            future.completeExceptionally(t);
            return null;
        });
    }

    @Override
    protected PlcConsumerRegistration onRegisterConsumer(Consumer<PlcSubscriptionEvent> consumer, Collection<PlcSubscriptionHandle> handles) {
        DefaultPlcConsumerRegistration registration = new DefaultPlcConsumerRegistration(
            this, consumer, handles.toArray(new PlcSubscriptionHandle[0]));
        consumers.put(registration, consumer);
        return registration;
    }

    @Override
    protected void onUnregisterConsumer(PlcConsumerRegistration registration) {
        if (registration instanceof DefaultPlcConsumerRegistration r) {
            consumers.remove(r);
        }
    }

    private void dispatchDeviceNotification(AdsDeviceNotificationRequest notification) throws BufferException {
        long receiveTs = System.currentTimeMillis();
        for (AdsStampHeader stamp : notification.getAdsStampHeaders()) {
            // Convert Windows FILETIME to unix epoch ms.
            long unixEpochTimestamp = stamp.getTimestamp().divide(java.math.BigInteger.valueOf(10000L)).longValue() - 11644473600000L;
            for (AdsNotificationSample sample : stamp.getAdsNotificationSamples()) {
                long handle = sample.getNotificationHandle();
                for (DefaultPlcConsumerRegistration registration : consumers.keySet()) {
                    for (PlcSubscriptionHandle subscriptionHandle : registration.getSubscriptionHandles()) {
                        if (subscriptionHandle instanceof AdsSubscriptionHandle adsHandle
                            && adsHandle.getNotificationHandle() == handle) {
                            Map<String, PlcResponseItem<PlcValue>> values = convertSampleToPlc4XResult(adsHandle, sample.getData());
                            DefaultPlcSubscriptionEvent event = new DefaultPlcSubscriptionEvent(Instant.ofEpochMilli(unixEpochTimestamp), values);
                            consumers.get(registration).accept(event);
                        }
                    }
                }
            }
        }
    }

    private Map<String, PlcResponseItem<PlcValue>> convertSampleToPlc4XResult(AdsSubscriptionHandle subscriptionHandle, byte[] data) throws BufferException {
        Map<String, PlcResponseItem<PlcValue>> values = new HashMap<>();
        ReadBufferByteBased rb = new ReadBufferByteBased(data, WithOption.WithUnsignedIntegerEncoding("unsigned-binary"), WithOption.WithSignedIntegerEncoding("twos-complement"), WithByteBasedOption.WithByteOrder("LITTLE_ENDIAN"));
        values.put(subscriptionHandle.getTagName(), new DefaultPlcResponseItem<>(PlcResponseCode.OK,
            DataItem.staticParse(rb, getPlcValueTypeForAdsDataType(subscriptionHandle.getAdsDataType()), data.length)));
        return values;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Browse
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Override
    protected CompletableFuture<PlcBrowseResponse> onBrowse(PlcBrowseRequest browseRequest) {
        return onBrowseWithInterceptor(browseRequest, (queryName, query, item) -> true);
    }

    @Override
    protected CompletableFuture<PlcBrowseResponse> onBrowseWithInterceptor(PlcBrowseRequest browseRequest, PlcBrowseRequestInterceptor interceptor) {
        Map<String, PlcResponseCode> responseCodes = new HashMap<>();
        Map<String, List<PlcBrowseItem>> values = new HashMap<>();
        for (String queryName : browseRequest.getQueryNames()) {
            PlcQuery query = browseRequest.getQuery(queryName);
            List<PlcBrowseItem> resultsForQuery = new ArrayList<>();
            for (AdsSymbolTableEntry symbol : symbolTable.values()) {
                Optional<AdsDataTypeTableEntry> dataTypeOpt = getDataTypeTableEntry(symbol.getDataTypeName());
                if (dataTypeOpt.isEmpty()) {
                    LOGGER.warn("couldn't find datatype: {}", symbol.getDataTypeName());
                    continue;
                }
                AdsDataTypeTableEntry dataType = dataTypeOpt.get();
                PlcValueType plcValueType = getPlcValueTypeForAdsDataTypeForBrowse(dataType);

                List<PlcBrowseItem> children = getBrowseItems(symbol.getName(), symbol.getGroup(), symbol.getOffset(), !symbol.getFlagReadOnly(), dataType);
                Map<String, PlcBrowseItem> childMap = new HashMap<>();
                for (PlcBrowseItem child : children) {
                    childMap.put(child.getName(), child);
                }

                Map<String, PlcValue> options = new HashMap<>();
                options.put("comment", new PlcSTRING(symbol.getComment()));
                options.put("group-id", new PlcUDINT(symbol.getGroup()));
                options.put("offset", new PlcUDINT(symbol.getOffset()));
                options.put("size-in-bytes", new PlcUDINT(symbol.getSize()));

                List<ArrayInfo> arrayInfo = new ArrayList<>(dataType.getArrayInfo().size());
                List<ArrayInfo> itemArrayInfo = new ArrayList<>(dataType.getArrayInfo().size());
                for (AdsDataTypeArrayInfo a : dataType.getArrayInfo()) {
                    arrayInfo.add(new DefaultArrayInfo((int) a.getLowerBound(), (int) a.getUpperBound()));
                    itemArrayInfo.add(new DefaultArrayInfo((int) a.getLowerBound(), (int) a.getUpperBound()));
                }
                DefaultPlcBrowseItem item = new DefaultPlcBrowseItem(
                    new SymbolicAdsTag(symbol.getName(), plcValueType, arrayInfo), symbol.getName(),
                    true, !symbol.getFlagReadOnly(), true, false, itemArrayInfo, childMap, options);

                if (interceptor.intercept(queryName, query, item)) {
                    resultsForQuery.add(item);
                }
            }
            responseCodes.put(queryName, PlcResponseCode.OK);
            values.put(queryName, resultsForQuery);
        }
        return CompletableFuture.completedFuture(new DefaultPlcBrowseResponse(browseRequest, responseCodes, values));
    }

    private List<PlcBrowseItem> getBrowseItems(String basePath, long baseGroupId, long baseOffset, boolean parentWritable, AdsDataTypeTableEntry dataType) {
        if (dataType.getArrayDimensions() > 0) {
            Optional<AdsDataTypeTableEntry> opt = getDataTypeTableEntry(dataType.getMainName());
            if (opt.isEmpty()) {
                LOGGER.warn("couldn't find datatype: {}", dataType.getMainName());
                return Collections.emptyList();
            }
            dataType = opt.get();
        }
        if (dataType.getNumChildren() == 0) {
            return Collections.emptyList();
        }
        List<PlcBrowseItem> values = new ArrayList<>(dataType.getNumChildren());
        for (AdsDataTypeTableEntry child : dataType.getChildren()) {
            Optional<AdsDataTypeTableEntry> opt = getDataTypeTableEntry(child.getSecondaryName());
            if (opt.isEmpty()) {
                LOGGER.warn("couldn't find datatype: {} for child {}", dataType.getSecondaryName(), child.getMainName());
                continue;
            }
            AdsDataTypeTableEntry childDataType = opt.get();
            String itemAddress = basePath + "." + child.getMainName();
            PlcValueType plc4xPlcValueType = PlcValueType.valueOf(getPlcValueTypeForAdsDataTypeForBrowse(childDataType).toString());

            List<PlcBrowseItem> children = getBrowseItems(itemAddress, baseGroupId, baseOffset + child.getOffset(), parentWritable, childDataType);
            Map<String, PlcBrowseItem> childMap = new HashMap<>();
            for (PlcBrowseItem ch : children) {
                childMap.put(ch.getName(), ch);
            }

            Map<String, PlcValue> options = new HashMap<>();
            options.put("comment", new PlcSTRING(child.getComment()));
            options.put("group-id", new PlcUDINT(baseGroupId));
            options.put("offset", new PlcUDINT(baseOffset + child.getOffset()));
            options.put("size-in-bytes", new PlcUDINT(childDataType.getSize()));

            List<ArrayInfo> arrayInfo = new ArrayList<>(childDataType.getArrayInfo().size());
            List<ArrayInfo> itemArrayInfo = new ArrayList<>(childDataType.getArrayInfo().size());
            for (AdsDataTypeArrayInfo a : childDataType.getArrayInfo()) {
                arrayInfo.add(new DefaultArrayInfo((int) a.getLowerBound(), (int) a.getUpperBound()));
                itemArrayInfo.add(new DefaultArrayInfo((int) a.getLowerBound(), (int) a.getUpperBound()));
            }
            values.add(new DefaultPlcBrowseItem(
                new SymbolicAdsTag(basePath + "." + child.getMainName(), plc4xPlcValueType, arrayInfo),
                child.getMainName(),
                true, parentWritable, true, false, itemArrayInfo, childMap, options));
        }
        return values;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Symbolic resolution
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    private CompletableFuture<Map<AdsTag, DirectAdsTag>> getDirectAddresses(List<PlcTag> tags) {
        CompletableFuture<Map<AdsTag, DirectAdsTag>> future = new CompletableFuture<>();

        List<SymbolicAdsTag> referencedSymbolicTags = tags.stream()
            .filter(SymbolicAdsTag.class::isInstance)
            .map(SymbolicAdsTag.class::cast)
            .collect(Collectors.toList());

        List<SymbolicAdsTag> symbolicTagsNeedingResolution = referencedSymbolicTags.stream()
            .filter(t -> getDirectAdsTagForSymbolicName(t) == null)
            .collect(Collectors.toList());

        if (!symbolicTagsNeedingResolution.isEmpty()) {
            List<SymbolicAdsTag> requiredResolutionTags = symbolicTagsNeedingResolution.stream()
                .filter(t -> !pendingResolutionRequests.containsKey(t))
                .collect(Collectors.toList());
            if (!requiredResolutionTags.isEmpty()) {
                CompletableFuture<Void> resolutionFuture;
                if (requiredResolutionTags.size() == 1) {
                    SymbolicAdsTag t = requiredResolutionTags.get(0);
                    resolutionFuture = resolveSingleSymbolicAddress(t);
                    pendingResolutionRequests.put(t, resolutionFuture);
                } else {
                    resolutionFuture = resolveMultipleSymbolicAddresses(requiredResolutionTags);
                    for (SymbolicAdsTag t : requiredResolutionTags) {
                        pendingResolutionRequests.put(t, resolutionFuture);
                    }
                }
            }

            CompletableFuture<Void> resolutionComplete = CompletableFuture.allOf(symbolicTagsNeedingResolution.stream()
                .map(pendingResolutionRequests::get)
                .toArray(CompletableFuture[]::new));

            resolutionComplete.handleAsync((unused, throwable) -> {
                Map<AdsTag, DirectAdsTag> mapping = new HashMap<>(tags.size());
                for (PlcTag tag : tags) {
                    if (tag instanceof SymbolicAdsTag) {
                        if (pendingResolutionRequests.containsKey(tag) && pendingResolutionRequests.get(tag).isCompletedExceptionally()) {
                            mapping.put((AdsTag) tag, null);
                        } else {
                            mapping.put((AdsTag) tag, getDirectAdsTagForSymbolicName(tag));
                        }
                    } else {
                        mapping.put((AdsTag) tag, (DirectAdsTag) tag);
                    }
                }
                return future.complete(mapping);
            });
        } else {
            Map<AdsTag, DirectAdsTag> mapping = new HashMap<>(tags.size());
            for (PlcTag tag : tags) {
                if (tag instanceof SymbolicAdsTag) {
                    mapping.put((AdsTag) tag, getDirectAdsTagForSymbolicName(tag));
                } else {
                    mapping.put((AdsTag) tag, (DirectAdsTag) tag);
                }
            }
            future.complete(mapping);
        }
        return future;
    }

    private CompletableFuture<Void> resolveSingleSymbolicAddress(SymbolicAdsTag symbolicAdsTag) {
        AmsPacket request = new AdsReadWriteRequest(
            getConfiguration().getTargetAmsNetId(), getConfiguration().getTargetAmsPort(),
            getConfiguration().getSourceAmsNetId(), getConfiguration().getSourceAmsPort(), ReturnCode.OK, getInvokeId(),
            ReservedIndexGroups.ADSIGRP_SYM_HNDBYNAME.getValue(), 0L, 4L, null,
            getNullByteTerminatedArray(symbolicAdsTag.getSymbolicAddress()));

        return sendAmsRequest(request, AdsReadWriteResponse.class).thenCompose(response -> {
            if (response.getResult() != ReturnCode.OK) {
                return CompletableFuture.failedFuture(new PlcException(
                    "Couldn't retrieve handle for symbolic tag " + symbolicAdsTag.getSymbolicAddress()
                        + " got return code " + response.getResult().name()));
            }
            try {
                ReadBuffer rb = new ReadBufferByteBased(response.getData(), WithOption.WithUnsignedIntegerEncoding("unsigned-binary"), WithOption.WithSignedIntegerEncoding("twos-complement"), WithByteBasedOption.WithByteOrder("LITTLE_ENDIAN"));
                long handle = rb.readUnsignedLong(32);
                // TODO: Find out how to read the datatype for the given symbolic tag
                //       (the original implementation doesn't actually populate the symbolic→direct
                //        mapping after retrieving the handle, so subscribe-on-symbolic isn't fully
                //        functional — preserved as-is.)
                return CompletableFuture.completedFuture(null);
            } catch (BufferException e) {
                return CompletableFuture.failedFuture(e);
            }
        });
    }

    private CompletableFuture<Void> resolveMultipleSymbolicAddresses(List<SymbolicAdsTag> symbolicAdsTags) {
        long expectedResponseDataSize = (long) symbolicAdsTags.size() * 12;
        byte[] addressData = symbolicAdsTags.stream()
            .map(SymbolicAdsTag::getSymbolicAddress).collect(Collectors.joining("")).getBytes();

        AmsPacket request = new AdsReadWriteRequest(
            getConfiguration().getTargetAmsNetId(), getConfiguration().getTargetAmsPort(),
            getConfiguration().getSourceAmsNetId(), getConfiguration().getSourceAmsPort(), ReturnCode.OK, getInvokeId(),
            ReservedIndexGroups.ADSIGRP_MULTIPLE_READ_WRITE.getValue(),
            (long) symbolicAdsTags.size(), expectedResponseDataSize,
            symbolicAdsTags.stream().map(t -> new AdsMultiRequestItemReadWrite(
                ReservedIndexGroups.ADSIGRP_SYM_HNDBYNAME.getValue(), 0L, 4L, (long) t.getSymbolicAddress().length()))
                .collect(Collectors.toList()),
            addressData);

        return sendAmsRequest(request, AdsReadWriteResponse.class).thenAccept(response -> {
            ReadBuffer rb = new ReadBufferByteBased(response.getData(), WithOption.WithUnsignedIntegerEncoding("unsigned-binary"), WithOption.WithSignedIntegerEncoding("twos-complement"), WithByteBasedOption.WithByteOrder("LITTLE_ENDIAN"));
            Map<SymbolicAdsTag, Long> returnCodes = new HashMap<>();
            symbolicAdsTags.forEach(t -> {
                try {
                    long returnCode = rb.readUnsignedLong(32);
                    long itemLength = rb.readUnsignedLong(32);
                    assert itemLength == 4;
                    returnCodes.put(t, returnCode);
                } catch (BufferException e) {
                    throw new PlcRuntimeException(e);
                }
            });
            symbolicAdsTags.forEach(t -> {
                try {
                    ReturnCode returnCode = ReturnCode.enumForValue(returnCodes.get(t));
                    if (returnCode == ReturnCode.OK) {
                        long handle = rb.readUnsignedLong(32);
                        // TODO: Finish parsing of the response and possibly the reading of
                        //       datatype information for the current tag (preserved as-is).
                    } else if (returnCode == ReturnCode.ADSERR_DEVICE_SYMBOLNOTFOUND) {
                        pendingResolutionRequests.put(t, CompletableFuture.failedFuture(
                            new PlcInvalidTagException("Could not resolve tag " + t.getSymbolicAddress())));
                    }
                } catch (BufferException e) {
                    throw new PlcRuntimeException(e);
                }
            });
        });
    }

    private DirectAdsTag getDirectAdsTagForSymbolicName(PlcTag tag) {
        if (tag instanceof DirectAdsTag d) {
            return d;
        }
        SymbolicAdsTag symbolicAdsTag = (SymbolicAdsTag) tag;
        String symbolicAddress = symbolicAdsTag.getSymbolicAddress();
        String[] addressParts = symbolicAddress.split("\\.");

        if (addressParts.length < 2) {
            if (!symbolTable.containsKey(symbolicAddress)) return null;
            AdsSymbolTableEntry entry = symbolTable.get(symbolicAddress);
            Optional<AdsDataTypeTableEntry> opt = getDataTypeTableEntry(entry.getDataTypeName());
            if (opt.isEmpty()) return null;
            AdsDataTypeTableEntry dataType = opt.get();
            return new DirectAdsTag(entry.getGroup(), entry.getOffset(),
                dataType.getMainName(), dataType.getArrayDimensions());
        } else {
            String symbolName = addressParts[0] + "." + addressParts[1];
            if (!symbolTable.containsKey(symbolName)) return null;
            AdsSymbolTableEntry entry = symbolTable.get(symbolName);
            Optional<AdsDataTypeTableEntry> opt = getDataTypeTableEntry(entry.getDataTypeName());
            if (opt.isEmpty()) return null;
            AdsDataTypeTableEntry dataType = opt.get();
            return resolveDirectAdsTagForSymbolicNameFromDataType(
                Arrays.asList(addressParts).subList(2, addressParts.length),
                entry.getGroup(), entry.getOffset(), dataType);
        }
    }

    private DirectAdsTag resolveDirectAdsTagForSymbolicNameFromDataType(List<String> remainingAddressParts, long currentGroup, long currentOffset, AdsDataTypeTableEntry adsDataTypeTableEntry) {
        if (remainingAddressParts.isEmpty()) {
            // TODO: Implement Array support
            if (adsDataTypeTableEntry.getDataType().getValue() == AdsDataType.CHAR.getValue()) {
                int stringLength = (int) adsDataTypeTableEntry.getSize() - 1;
                return new DirectAdsStringTag(currentGroup, currentOffset, adsDataTypeTableEntry.getMainName(), stringLength, 1);
            } else if (adsDataTypeTableEntry.getDataType().getValue() == AdsDataType.WCHAR.getValue()) {
                int stringLength = (int) (adsDataTypeTableEntry.getSize() - 2) / 2;
                return new DirectAdsStringTag(currentGroup, currentOffset, adsDataTypeTableEntry.getMainName(), stringLength, 1);
            } else {
                return new DirectAdsTag(currentGroup, currentOffset, adsDataTypeTableEntry.getMainName(), 1);
            }
        }
        for (AdsDataTypeTableEntry child : adsDataTypeTableEntry.getChildren()) {
            if (child.getMainName().equals(remainingAddressParts.get(0))) {
                Optional<AdsDataTypeTableEntry> opt = getDataTypeTableEntry(child.getSecondaryName());
                if (opt.isEmpty()) {
                    throw new PlcRuntimeException("Could not resolve data type " + child.getSecondaryName());
                }
                return resolveDirectAdsTagForSymbolicNameFromDataType(
                    remainingAddressParts.subList(1, remainingAddressParts.size()),
                    currentGroup, currentOffset + child.getOffset(), opt.get());
            }
        }
        throw new PlcRuntimeException(String.format("Couldn't find child with name '%s' for type '%s'",
            remainingAddressParts.get(0), adsDataTypeTableEntry.getMainName()));
    }

    private PlcValueType getPlcValueTypeForAdsDataTypeForBrowse(AdsDataTypeTableEntry dataTypeTableEntry) {
        String dataTypeName = (!dataTypeTableEntry.getSecondaryName().isEmpty() && !dataTypeTableEntry.getMainName().equals("BOOL")) ?
            dataTypeTableEntry.getSecondaryName() : dataTypeTableEntry.getMainName();
        if (dataTypeName.startsWith("STRING(")) dataTypeName = "STRING";
        else if (dataTypeName.startsWith("WSTRING(")) dataTypeName = "WSTRING";
        try {
            return PlcValueType.valueOf(dataTypeName);
        } catch (IllegalArgumentException e) {
            return PlcValueType.Struct;
        }
    }

    private PlcValueType getPlcValueTypeForAdsDataType(AdsDataTypeTableEntry dataTypeTableEntry) {
        String dataTypeName = dataTypeTableEntry.getMainName();
        if (dataTypeName.startsWith("STRING(")) dataTypeName = "STRING";
        else if (dataTypeName.startsWith("WSTRING(")) dataTypeName = "WSTRING";
        try {
            return PlcValueType.valueOf(dataTypeName);
        } catch (IllegalArgumentException e) {
            if (dataTypeTableEntry.getArrayDimensions() > 0) return PlcValueType.List;
            if (dataTypeTableEntry.getChildren().isEmpty()) {
                try {
                    dataTypeName = dataTypeTableEntry.getSecondaryName();
                    if (dataTypeName.startsWith("STRING(")) dataTypeName = "STRING";
                    else if (dataTypeName.startsWith("WSTRING(")) dataTypeName = "WSTRING";
                    return PlcValueType.valueOf(dataTypeName);
                } catch (IllegalArgumentException e2) {
                    return PlcValueType.NULL;
                }
            }
            return PlcValueType.Struct;
        }
    }

    private Optional<AdsDataTypeTableEntry> getDataTypeTableEntry(String name) {
        if (dataTypeTable.containsKey(name)) {
            return Optional.of(dataTypeTable.get(name));
        }
        try {
            AdsDataType adsDataType;
            int numBytes;
            if (name.startsWith("STRING(")) {
                adsDataType = AdsDataType.valueOf("CHAR");
                numBytes = Integer.parseInt(name.substring(7, name.length() - 1)) + 1;
            } else if (name.startsWith("WSTRING(")) {
                adsDataType = AdsDataType.valueOf("WCHAR");
                numBytes = Integer.parseInt(name.substring(8, name.length() - 1)) * 2 + 2;
            } else {
                adsDataType = AdsDataType.valueOf(name);
                numBytes = adsDataType.getNumBytes();
            }
            return Optional.of(new AdsDataTypeTableEntry(
                128L, 1L, 0L, 0L, (long) numBytes, 0L,
                AdsDatatypeId.enumForValue(adsDataType.getValue()),
                false, false, false, false, false, false, false, false, false, false, false, false,
                false, false, false, false, false, false, false, false, false, false, false,
                0, 0,
                name, "", "",
                Collections.emptyList(), Collections.emptyList(), new byte[0],
                null, null, null, new byte[0]));
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    private byte[] getNullByteTerminatedArray(String value) {
        byte[] valueBytes = value.getBytes();
        byte[] nullTerminatedBytes = new byte[valueBytes.length + 1];
        System.arraycopy(valueBytes, 0, nullTerminatedBytes, 0, valueBytes.length);
        return nullTerminatedBytes;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // AMS route setup (preserved from original; not currently invoked because authentication
    // is not surfaced through ConnectionBase). Kept here for reference / future re-enablement.
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @SuppressWarnings("unused")
    private CompletableFuture<Void> setupAmsRoute(PlcUsernamePasswordAuthentication authentication, InetAddress remoteAddress) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        new Thread(() -> {
            try {
                // The local address determination is no longer trivially available here; if the
                // transport layer can supply a SocketAddress in future versions, plug it in here.
                AmsNetId sourceAmsNetId = new AmsNetId(
                    getConfiguration().getSourceAmsNetId().getOctet1(), getConfiguration().getSourceAmsNetId().getOctet2(),
                    getConfiguration().getSourceAmsNetId().getOctet3(), getConfiguration().getSourceAmsNetId().getOctet4(),
                    getConfiguration().getSourceAmsNetId().getOctet5(), getConfiguration().getSourceAmsNetId().getOctet6());
                String routeName = String.format("PLC4X-%d.%d.%d.%d.%d.%d",
                    sourceAmsNetId.getOctet1(), sourceAmsNetId.getOctet2(), sourceAmsNetId.getOctet3(),
                    sourceAmsNetId.getOctet4(), sourceAmsNetId.getOctet5(), sourceAmsNetId.getOctet6());

                AdsDiscovery req = new AdsDiscovery(getInvokeId(), Operation.ADD_OR_UPDATE_ROUTE_REQUEST,
                    sourceAmsNetId, AdsPortNumbers.SYSTEM_SERVICE,
                    Arrays.asList(new AdsDiscoveryBlockRouteName(new AmsString(routeName)),
                        new AdsDiscoveryBlockAmsNetId(sourceAmsNetId),
                        new AdsDiscoveryBlockUserName(new AmsString(authentication.getUsername())),
                        new AdsDiscoveryBlockPassword(new AmsString(authentication.getPassword())),
                        new AdsDiscoveryBlockHostName(new AmsString(remoteAddress.getHostAddress()))));

                try (DatagramSocket socket = new DatagramSocket(Constants.ADSDISCOVERYUDPDEFAULTPORT)) {
                    WriteBufferByteBased wb = new WriteBufferByteBased(new byte[req.getLengthInBytes()],
                        WithByteBasedOption.WithByteOrder("LITTLE_ENDIAN"));
                    req.serialize(wb);

                    DatagramPacket pkt = new DatagramPacket(wb.getBytes(), wb.getBytes().length,
                        remoteAddress, Constants.ADSDISCOVERYUDPDEFAULTPORT);
                    socket.send(pkt);

                    byte[] buf = new byte[100];
                    DatagramPacket responsePacket = new DatagramPacket(buf, buf.length);
                    socket.setSoTimeout(getConfiguration().getTimeoutRequest());
                    socket.receive(responsePacket);

                    ReadBuffer rb = new ReadBufferByteBased(responsePacket.getData(),
                        WithByteBasedOption.WithByteOrder("LITTLE_ENDIAN"));
                    AdsDiscovery resp = AdsDiscovery.staticParse(rb);
                    if (resp.getRequestId() == 1) {
                        for (AdsDiscoveryBlock block : resp.getBlocks()) {
                            if (block.getBlockType() == AdsDiscoveryBlockType.STATUS) {
                                AdsDiscoveryBlockStatus status = (AdsDiscoveryBlockStatus) block;
                                if (status.getStatus() != Status.SUCCESS) {
                                    future.completeExceptionally(new PlcException("Error adding AMS route"));
                                    return;
                                }
                            }
                        }
                    }
                    future.complete(null);
                }
            } catch (Exception e) {
                future.completeExceptionally(new PlcException("Error adding AMS route", e));
            }
        }).start();
        return future;
    }

}
