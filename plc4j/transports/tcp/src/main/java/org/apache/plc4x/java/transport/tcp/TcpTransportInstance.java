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
package org.apache.plc4x.java.transport.tcp;

import org.apache.plc4x.java.spi.transports.api.AsyncTransportInstance;
import org.apache.plc4x.java.spi.transports.api.BaseTransportInstance;
import org.apache.plc4x.java.spi.transports.api.RingBuffer;
import org.apache.plc4x.java.spi.transports.api.exceptions.TransportException;
import org.apache.plc4x.java.transport.tcp.config.TcpTransportConfiguration;
import org.apache.plc4x.java.utils.auditlog.api.AuditLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * Java 21+ optimized version using virtual threads - TCP transport implementation using NIO SocketChannel with async support.
 * Implements AsyncTransportInstance for event-driven I/O without polling.
 */
public class TcpTransportInstance extends BaseTransportInstance<TcpTransportConfiguration> implements AsyncTransportInstance<TcpTransportConfiguration> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TcpTransportInstance.class);
    private static final int DEFAULT_BUFFER_SIZE = 81920;
    private static final byte[] EMPTY_BYTES = new byte[0];

    private final SocketChannel socketChannel;
    private final RingBuffer ringBuffer;
    private final ByteBuffer readBuffer;  // Pre-allocated direct buffer for zero-copy I/O
    private final Lock readLock = new ReentrantLock();
    private final Lock writeLock = new ReentrantLock();
    private volatile boolean open = true;

    // Async support
    private final Selector selector;
    private volatile Runnable dataListener;
    private volatile Consumer<Throwable> disconnectListener;
    private final Thread selectorThread;

    public TcpTransportInstance(InetSocketAddress remoteAddress, TcpTransportConfiguration configuration, AuditLog auditLog) throws TransportException {
        super(configuration, auditLog);
        LOGGER.debug("TcpTransportInstance");
        this.ringBuffer = new RingBuffer(DEFAULT_BUFFER_SIZE);
        this.readBuffer = ByteBuffer.allocateDirect(DEFAULT_BUFFER_SIZE);  // Direct buffer for zero-copy

        try {
            // Open socket channel
            this.socketChannel = SocketChannel.open();

            // Bind to a local address if specified
            if (configuration.localAddress != null && !configuration.localAddress.isEmpty()) {
                SocketAddress localAddr = new InetSocketAddress(configuration.localAddress, configuration.localPort);
                socketChannel.bind(localAddr);
                LOGGER.debug("Bound to local address {}:{}", configuration.localAddress, configuration.localPort);
            }

            // Configure socket options before connecting
            socketChannel.socket().setTcpNoDelay(configuration.tcpNoDelay);
            socketChannel.socket().setKeepAlive(configuration.keepAlive);

            if (configuration.sendBufferSize > 0) {
                socketChannel.socket().setSendBufferSize(configuration.sendBufferSize);
            }
            if (configuration.receiveBufferSize > 0) {
                socketChannel.socket().setReceiveBufferSize(configuration.receiveBufferSize);
            }
            if (configuration.readTimeout > 0) {
                socketChannel.socket().setSoTimeout(configuration.readTimeout);
            }

            // Connect with timeout
            socketChannel.socket().connect(remoteAddress, configuration.connectTimeout);

            // Configure non-blocking mode for NIO selector
            socketChannel.configureBlocking(false);

            // Create a selector for async I/O
            this.selector = Selector.open();
            socketChannel.register(selector, SelectionKey.OP_READ);

            // Start selector thread using virtual thread (Java 21+)
            this.selectorThread = Thread.ofVirtual()
                .name("TCP-Selector-" + remoteAddress.getHostName() + ":" + remoteAddress.getPort())
                .start(this::runSelectorLoop);

            LOGGER.info("Connected to {}:{} with async support", remoteAddress.getHostName(), remoteAddress.getPort());
        } catch (IOException e) {
            String errorMsg = String.format("Failed to connect to %s:%d - %s",
                remoteAddress.getHostName(), remoteAddress.getPort(), e.getMessage());
            LOGGER.error(errorMsg, e);
            throw new TransportException(errorMsg, e);
        }
    }

    @Override
    public boolean isOpen() {
        return open && socketChannel.isConnected();
    }

    @Override
    public int getNumBytesAvailable() throws TransportException {
        readLock.lock();
        try {
            if (!isOpen()) {
                return 0;
            }

            return ringBuffer.availableForReading();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public byte[] peekReadableBytes(int numBytes) throws TransportException {
        if (numBytes <= 0) {
            return EMPTY_BYTES;
        }

        readLock.lock();
        try {
            ensureOpen();

            // Fill the ring buffer if necessary
            getNumBytesAvailable();

            if (ringBuffer.availableForReading() < numBytes) {
                throw new TransportException(
                    String.format("Requested %d bytes but only %d available", numBytes, ringBuffer.availableForReading())
                );
            }

            // Peek without consuming
            return ringBuffer.peek(numBytes);

        } finally {
            readLock.unlock();
        }
    }

    @Override
    public byte[] read(int numBytes) throws TransportException {
        if (numBytes <= 0) {
            return EMPTY_BYTES;
        }

        readLock.lock();
        try {
            ensureOpen();

            // Fill the ring buffer if necessary
            getNumBytesAvailable();

            if (ringBuffer.availableForReading() < numBytes) {
                throw new TransportException(
                    String.format("Requested %d bytes but only %d available", numBytes, ringBuffer.availableForReading())
                );
            }

            // Read and consume bytes
            return ringBuffer.read(numBytes);

        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void write(byte[] bytes) throws TransportException {
        if (bytes == null || bytes.length == 0) {
            return;
        }

        writeLock.lock();
        try {
            ensureOpen();

            ByteBuffer writeBuffer = ByteBuffer.wrap(bytes);

            while (writeBuffer.hasRemaining()) {
                int written = socketChannel.write(writeBuffer);
                if (written == -1) {
                    open = false;
                    throw new TransportException("Connection closed while writing");
                }
            }

            LOGGER.trace("Wrote {} bytes to {}", bytes.length, socketChannel.getRemoteAddress());

        } catch (IOException e) {
            throw new TransportException("Failed to write data", e);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void close() throws TransportException {
        if (!open) {
            return;
        }

        writeLock.lock();
        try {
            readLock.lock();
            try {
                open = false;

                // Wake up selector
                selector.wakeup();

                // Close socket channel
                socketChannel.close();

                // Close selector
                selector.close();

                // Wait for the selector thread to finish
                if (selectorThread != null) {
                    try {
                        selectorThread.join(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }

                LOGGER.debug("TCP connection closed");
            } catch (IOException e) {
                throw new TransportException("Failed to close connection", e);
            } finally {
                readLock.unlock();
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Ensures the connection is still open, throws exception otherwise.
     */
    private void ensureOpen() throws TransportException {
        if (!isOpen()) {
            throw new TransportException("Transport is closed");
        }
    }

    // ========== AsyncTransportInstance Implementation ==========

    @Override
    public void registerDataListener(Runnable listener) {
        this.dataListener = listener;
        LOGGER.debug("Data listener registered");
    }

    @Override
    public void removeDataListener() {
        this.dataListener = null;
        LOGGER.debug("Data listener removed");
    }

    @Override
    public void registerDisconnectListener(Consumer<Throwable> listener) {
        this.disconnectListener = listener;
        LOGGER.debug("Disconnect listener registered");
    }

    @Override
    public void removeDisconnectListener() {
        this.disconnectListener = null;
        LOGGER.debug("Disconnect listener removed");
    }

    /**
     * Notifies the disconnect listener if one is registered.
     *
     * @param cause the exception that caused the disconnect, or null for graceful close
     */
    private void notifyDisconnect(Throwable cause) {
        Consumer<Throwable> listener = disconnectListener;
        if (listener != null) {
            try {
                listener.accept(cause);
            } catch (Exception e) {
                LOGGER.error("Error in disconnect listener", e);
            }
        }
    }

    /**
     * Selector loop that runs in a virtual thread and notifies listeners when data arrives.
     * This is the core of the async implementation - no polling needed in the driver!
     */
    private void runSelectorLoop() {
        LOGGER.debug("Selector loop started");

        while (open && !Thread.currentThread().isInterrupted()) {
            try {
                // Block until events are available (no CPU waste!)
                int readyChannels = selector.select();

                if (readyChannels == 0) {
                    continue;
                }

                var selectedKeys = selector.selectedKeys();
                var iterator = selectedKeys.iterator();

                while (iterator.hasNext()) {
                    SelectionKey key = iterator.next();
                    iterator.remove();

                    if (!key.isValid()) {
                        continue;
                    }

                    if (key.isReadable()) {
                        // Data available - read it into the ring buffer
                        readLock.lock();
                        try {
                            readBuffer.clear();
                            int bytesRead = socketChannel.read(readBuffer);

                            if (bytesRead > 0) {
                                readBuffer.flip();
                                byte[] data = new byte[readBuffer.remaining()];
                                readBuffer.get(data);
                                ringBuffer.write(data);

                                // Notify the listener that data is available
                                Runnable listener = dataListener;
                                if (listener != null) {
                                    listener.run();
                                }
                            } else if (bytesRead == -1) {
                                // Connection closed gracefully by remote
                                LOGGER.info("Connection closed by remote");
                                open = false;
                                notifyDisconnect(null);  // null indicates graceful close
                                break;
                            }
                        } finally {
                            readLock.unlock();
                        }
                    }
                }

            } catch (IOException e) {
                if (open) {
                    LOGGER.error("Error in selector loop", e);
                    open = false;
                    notifyDisconnect(e);
                }
                break;
            }
        }

        LOGGER.debug("Selector loop stopped");
    }

}
