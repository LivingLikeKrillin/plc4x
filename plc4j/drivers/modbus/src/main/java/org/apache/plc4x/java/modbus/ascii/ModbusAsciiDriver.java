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

import org.apache.plc4x.java.modbus.base.tag.ModbusTag;
import org.apache.plc4x.java.modbus.ascii.config.ModbusAsciiConfiguration;
import org.apache.plc4x.java.modbus.tcp.config.ModbusTcpTcpTransportConfiguration;
import org.apache.plc4x.java.spi.config.Configuration;
import org.apache.plc4x.java.spi.drivers.ConnectionBase;
import org.apache.plc4x.java.spi.drivers.DriverBase;
import org.apache.plc4x.java.spi.transports.api.Transport;
import org.apache.plc4x.java.spi.transports.api.TransportInstance;
import org.apache.plc4x.java.spi.transports.api.config.TransportConfiguration;
import org.apache.plc4x.java.utils.auditlog.api.AuditLog;

import java.util.List;
import java.util.Optional;

public class ModbusAsciiDriver extends DriverBase {

    @Override
    public String getProtocolCode() {
        return "modbus-ascii";
    }

    @Override
    public String getProtocolName() {
        return "Modbus ASCII";
    }

    @Override
    protected Class<? extends Configuration> getConfigurationClass() {
        return ModbusAsciiConfiguration.class;
    }

    @Override
    protected Class<? extends TransportConfiguration> getTransportConfigurationClass(Transport<?> transport) {
        if ("tcp".equals(transport.getTransportCode())) {
            return ModbusTcpTcpTransportConfiguration.class;
        }
        return super.getTransportConfigurationClass(transport);
    }

    @Override
    public Optional<String> getDefaultTransportCode() {
        return Optional.of("serial");
    }

    @Override
    public List<String> getSupportedTransportCodes() {
        return List.of("tcp", "serial", "test");
    }

    @Override
    protected boolean canPing() {
        return true;
    }

    @Override
    protected boolean canRead() {
        return true;
    }

    @Override
    protected boolean canWrite() {
        return true;
    }

    @Override
    protected ConnectionBase<ModbusAsciiConfiguration> getConnection(Configuration configuration, TransportInstance<?> transportInstance, AuditLog auditLog) {
        return new ModbusAsciiConnection((ModbusAsciiConfiguration) configuration, transportInstance, auditLog);
    }

    @Override
    public ModbusTag prepareTag(String tagAddress) {
        return ModbusTag.of(tagAddress);
    }

}
