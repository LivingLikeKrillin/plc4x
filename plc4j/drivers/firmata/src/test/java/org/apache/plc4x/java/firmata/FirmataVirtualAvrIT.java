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
package org.apache.plc4x.java.firmata;

import com.github.pfichtner.testcontainers.virtualavr.VirtualAvrConnection;
import com.github.pfichtner.testcontainers.virtualavr.VirtualAvrConnection.PinReportMode;
import java.net.URI;
import org.apache.plc4x.java.DefaultPlcDriverManager;
import org.apache.plc4x.java.api.PlcConnection;
import org.apache.plc4x.java.api.messages.PlcSubscriptionEvent;
import org.apache.plc4x.java.api.messages.PlcSubscriptionRequest;
import org.apache.plc4x.java.api.messages.PlcSubscriptionResponse;
import org.apache.plc4x.java.api.messages.PlcWriteRequest;
import org.apache.plc4x.java.api.messages.PlcWriteResponse;
import org.apache.plc4x.java.api.types.PlcResponseCode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for the Firmata driver against a virtual Arduino running
 * StandardFirmata inside a Docker container, talking to it over the driver's
 * TCP transport.
 *
 * <p>The container is a small extension over {@code pfichtner/virtualavr}
 * (see {@code src/test/resources/virtualavr-tcp/Dockerfile}) that replaces
 * the upstream PTY-bridge entrypoint with a single {@code socat TCP-LISTEN
 * ... EXEC:"node /app/virtualavr.js sketch.ino",pty,rawer,fdin=3,fdout=4}.
 * The simulator's UART is wired directly to a TCP socket exposed on the
 * container, which testcontainers maps to a stable host port. No host-side
 * {@code socat} or PTY is involved — so this IT runs on every OS Docker
 * supports, including macOS where {@code jSerialComm} won't open the
 * temp-file PTY the upstream serial mode produces.</p>
 *
 * <p>The simulator is spawned by socat on the first TCP {@code accept}, so
 * we open the PlcConnection once in {@code @BeforeAll} and share it across
 * all tests — that pays the sketch-startup cost once and keeps a single
 * simulator process alive (its WebSocket side-channel on port 8080 is what
 * the {@link VirtualAvrConnection} helper uses to inspect pin state and to
 * inject input transitions).</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Testcontainers(disabledWithoutDocker = true)
public class FirmataVirtualAvrIT {

    private static final int CONTAINER_TCP_SERIAL_PORT = 3030;
    private static final int CONTAINER_WEBSOCKET_PORT = 8080;

    private GenericContainer<?> virtualAvr;
    private PlcConnection plcConnection;
    private VirtualAvrConnection virtualAvrConnection;

    @BeforeAll
    void startContainer() throws Exception {
        virtualAvr = new GenericContainer<>(
            new ImageFromDockerfile()
                .withFileFromClasspath("Dockerfile", "virtualavr-tcp/Dockerfile"))
            .withExposedPorts(CONTAINER_TCP_SERIAL_PORT, CONTAINER_WEBSOCKET_PORT);
        // No Wait.forListeningPort — that probe opens a TCP connection,
        // which consumes our single accept slot (we run socat without
        // ",fork" because each accept spawns a fresh simulator and the
        // second one would clash on the WebSocket port). We retry the
        // driver connect below instead; failed connects before socat is
        // up just hit "connection refused" without spending an accept slot.
        virtualAvr.start();

        // Open the driver connection once. This triggers socat-EXEC inside
        // the container, which starts the simulator process; the simulator
        // in turn brings up the WebSocket server. After this returns we can
        // safely attach the WebSocket helper.
        String url = "firmata:tcp://" + virtualAvr.getHost() + ":"
            + virtualAvr.getMappedPort(CONTAINER_TCP_SERIAL_PORT)
            + "?request-timeout=30000";
        plcConnection = openWithRetry(url);

        // The library's static factory uses container.getFirstMappedPort()
        // which here is our TCP serial port. Build the WebSocket URI ourselves
        // pointing at the simulator's actual WS port (8080 in the container).
        // The constructor of VirtualAvrConnection already calls
        // connectBlocking() — so this *is* a connect attempt, not just a
        // handle setup. Wait a moment for the connect handshake to finish
        // before the first test runs.
        URI wsUri = URI.create("ws://" + virtualAvr.getHost() + ":"
            + virtualAvr.getMappedPort(CONTAINER_WEBSOCKET_PORT));
        virtualAvrConnection = new VirtualAvrConnection(wsUri);
        long wsDeadline = System.currentTimeMillis() + 10_000L;
        while (!virtualAvrConnection.isOpen() && System.currentTimeMillis() < wsDeadline) {
            //noinspection BusyWait
            Thread.sleep(100);
        }
        if (!virtualAvrConnection.isOpen()) {
            throw new AssertionError("Could not connect to virtualavr WebSocket at " + wsUri);
        }
    }

    private PlcConnection openWithRetry(String url) throws Exception {
        long deadline = System.currentTimeMillis() + 60_000L;
        Exception last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                return new DefaultPlcDriverManager().getConnection(url);
            } catch (Exception e) {
                last = e;
                //noinspection BusyWait
                Thread.sleep(500);
            }
        }
        throw new AssertionError("Could not open " + url + " within timeout", last);
    }

    @AfterAll
    void stopContainer() throws Exception {
        if (plcConnection != null) {
            plcConnection.close();
        }
        if (virtualAvrConnection != null) {
            virtualAvrConnection.close();
        }
        if (virtualAvr != null) {
            virtualAvr.stop();
        }
    }

    /**
     * Round-trips a single digital-pin write: write to pin 13 via the driver,
     * verify the simulator's pin state reflects the new value.
     *
     * <p>Exercises {@link FirmataConnection#onWrite} including the implicit
     * {@code SetPinMode → SetDigitalPinValue} sequencing the driver emits on
     * first write to a pin, and round-trips the bytes over the driver's TCP
     * transport.</p>
     */
    @Test
    void writeDigitalPin() throws Exception {
        // Tell the simulator's WebSocket side-channel to publish digital
        // updates for D13 — otherwise the AVR's internal pin transitions
        // never reach lastStates() and we'd never see the write land.
        virtualAvrConnection.pinReportMode("13", PinReportMode.DIGITAL);

        PlcWriteRequest writeRequest = plcConnection.writeRequestBuilder()
            .addTagAddress("led", "digital:13", true)
            .build();
        PlcWriteResponse response = writeRequest.execute().get(5, TimeUnit.SECONDS);
        assertEquals(PlcResponseCode.OK, response.getResponseCode("led"));

        awaitPinState("13", true);
    }

    /**
     * Subscribes to a digital pin, simulates a transition on the AVR side,
     * and asserts the driver delivers a {@link PlcSubscriptionEvent}.
     *
     * <p>Exercises {@link FirmataConnection#onSubscribe},
     * {@code SubscribeDigitalPinValue} on the wire, and the push-event
     * dispatch in {@code publishDigitalEvents}.</p>
     */
    @Test
    void subscribeDigitalPin() throws Exception {
        // Put D2 into DIGITAL report mode so pinState() injections actually
        // propagate to the AVR's pin register and then to the sketch.
        virtualAvrConnection.pinReportMode("2", PinReportMode.DIGITAL);

        LinkedBlockingQueue<PlcSubscriptionEvent> events = new LinkedBlockingQueue<>();
        PlcSubscriptionRequest request = plcConnection.subscriptionRequestBuilder()
            .addEventTagAddress("button", "digital:2")
            .build();
        PlcSubscriptionResponse response = request.execute().get(5, TimeUnit.SECONDS);
        assertEquals(PlcResponseCode.OK, response.getResponseCode("button"));

        // Subscriptions only deliver events once the consumer is explicitly
        // attached to the returned handle — the builder's setConsumer just
        // stashes a reference on the request itself.
        response.getSubscriptionHandle("button").register(events::offer);

        // Drive the simulated input pin HIGH; the StandardFirmata sketch
        // turns the transition into an unsolicited DigitalIO message.
        virtualAvrConnection.pinState("2", true);

        PlcSubscriptionEvent event = events.poll(10, TimeUnit.SECONDS);
        assertNotNull(event, "Expected a subscription event after pin transition");
        assertEquals(PlcResponseCode.OK, event.getResponseCode("button"));
        assertTrue(event.getPlcValue("button").getBoolean(),
            "Pin 2 should be reported as HIGH");
    }

    private void awaitPinState(String pinName, boolean expected) throws Exception {
        // virtualavr.js publishes pin-state diffs every PUBLISH_MILLIS
        // (default 250 ms). Allow several publish cycles + plenty of
        // simulator step time before we declare the write hasn't landed.
        long deadline = System.currentTimeMillis() + 15_000L;
        Object lastSeen = null;
        while (System.currentTimeMillis() < deadline) {
            Map<String, Object> states = virtualAvrConnection.lastStates();
            Object actual = states.get(pinName);
            lastSeen = actual;
            if (actual instanceof Boolean b && b == expected) {
                return;
            }
            if (actual instanceof Number n && (n.intValue() != 0) == expected) {
                return;
            }
            //noinspection BusyWait
            Thread.sleep(100);
        }
        throw new AssertionError("Pin " + pinName + " did not reach state " + expected
            + " within timeout (last seen: " + lastSeen
            + ", available pins: " + virtualAvrConnection.lastStates().keySet() + ")");
    }

}
