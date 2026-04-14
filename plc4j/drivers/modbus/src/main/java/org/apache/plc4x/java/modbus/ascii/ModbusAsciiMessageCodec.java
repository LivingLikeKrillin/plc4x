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
package org.apache.plc4x.java.modbus.ascii;

import org.apache.plc4x.java.modbus.readwrite.DriverType;
import org.apache.plc4x.java.modbus.readwrite.ModbusADU;
import org.apache.plc4x.java.modbus.readwrite.ModbusAsciiADU;
import org.apache.plc4x.java.spi.buffers.api.exceptions.BufferException;
import org.apache.plc4x.java.spi.buffers.bytebased.ReadBufferByteBased;
import org.apache.plc4x.java.spi.drivers.MessageCodecBase;
import org.apache.plc4x.java.spi.transports.api.TransportInstance;

import java.util.function.Consumer;

/**
 * MessageCodec for Modbus ASCII protocol.
 * Handles the encoding and decoding of Modbus ASCII ADU (Application Data Unit) messages.
 *
 * ASCII framing: starts with ':' (0x3A), hex-encoded data, ends with CR+LF.
 * The minimum frame is: ':' + address (2 hex chars) + function code (2 hex chars) + LRC (2 hex chars) + CR + LF = 9 bytes.
 */
public class ModbusAsciiMessageCodec extends MessageCodecBase<ModbusAsciiADU> {

    // Minimum ASCII message: ':' (1) + address (2) + function (2) + LRC (2) + CR (1) + LF (1) = 9 bytes
    private static final int MODBUS_ASCII_MIN_SIZE = 9;

    public ModbusAsciiMessageCodec(TransportInstance<?> transportInstance, Consumer<ModbusAsciiADU> messageHandler) {
        super("Modbus ASCII", transportInstance, messageHandler);
    }

    @Override
    protected int getMinimumHeaderSize() {
        return MODBUS_ASCII_MIN_SIZE;
    }

    @Override
    protected int calculateTotalMessageSize(byte[] header, int availableBytes) {
        // ASCII framing: scan for CR+LF terminator to determine message boundary.
        // Since we only have the header bytes here, return the available bytes
        // and let parseMessage handle the actual frame detection.
        return availableBytes;
    }

    @Override
    protected ModbusAsciiADU parseMessage(ReadBufferByteBased readBuffer) throws BufferException {
        ModbusADU modbusParsed = ModbusADU.staticParse(readBuffer, DriverType.MODBUS_ASCII, true);
        if (!(modbusParsed instanceof ModbusAsciiADU modbusADU)) {
            throw new BufferException("Parsed message is not a ModbusAsciiADU");
        }
        return modbusADU;
    }

}
