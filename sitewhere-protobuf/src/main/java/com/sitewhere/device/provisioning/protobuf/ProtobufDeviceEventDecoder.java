/*
 * ProtobufDeviceEventDecoder.java 
 * --------------------------------------------------------------------------------------
 * Copyright (c) Reveal Technologies, LLC. All rights reserved. http://www.reveal-tech.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package com.sitewhere.device.provisioning.protobuf;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Date;

import org.apache.log4j.Logger;

import com.sitewhere.device.provisioning.protobuf.proto.Sitewhere.SiteWhere;
import com.sitewhere.device.provisioning.protobuf.proto.Sitewhere.SiteWhere.Acknowledge;
import com.sitewhere.device.provisioning.protobuf.proto.Sitewhere.SiteWhere.DeviceLocation;
import com.sitewhere.device.provisioning.protobuf.proto.Sitewhere.SiteWhere.Header;
import com.sitewhere.device.provisioning.protobuf.proto.Sitewhere.SiteWhere.RegisterDevice;
import com.sitewhere.rest.model.device.event.request.DeviceCommandResponseCreateRequest;
import com.sitewhere.rest.model.device.event.request.DeviceLocationCreateRequest;
import com.sitewhere.rest.model.device.event.request.DeviceRegistrationRequest;
import com.sitewhere.rest.model.device.provisioning.DecodedDeviceEventRequest;
import com.sitewhere.spi.SiteWhereException;
import com.sitewhere.spi.device.provisioning.IDecodedDeviceEventRequest;
import com.sitewhere.spi.device.provisioning.IDeviceEventDecoder;

/**
 * Decodes a message payload that was previously encoded using the Google Protocol Buffers
 * with the SiteWhere proto.
 * 
 * @author Derek
 */
public class ProtobufDeviceEventDecoder implements IDeviceEventDecoder {

	/** Static logger instance */
	private static Logger LOGGER = Logger.getLogger(ProtobufDeviceEventDecoder.class);

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.sitewhere.spi.device.provisioning.IDeviceEventDecoder#decode(byte[])
	 */
	@Override
	public IDecodedDeviceEventRequest decode(byte[] payload) throws SiteWhereException {
		try {
			ByteArrayInputStream stream = new ByteArrayInputStream(payload);
			Header header = SiteWhere.Header.parseDelimitedFrom(stream);
			DecodedDeviceEventRequest decoded = new DecodedDeviceEventRequest();
			decoded.setOriginator(header.getOriginator());
			switch (header.getCommand()) {
			case REGISTER: {
				RegisterDevice register = RegisterDevice.parseDelimitedFrom(stream);
				LOGGER.debug("Decoded registration for: " + register.getHardwareId());
				DeviceRegistrationRequest request = new DeviceRegistrationRequest();
				request.setHardwareId(register.getHardwareId());
				request.setSpecificationToken(register.getSpecificationToken());
				request.setReplyTo(null);
				decoded.setHardwareId(register.getHardwareId());
				decoded.setRequest(request);
				return decoded;
			}
			case ACKNOWLEDGE: {
				Acknowledge ack = Acknowledge.parseDelimitedFrom(stream);
				LOGGER.debug("Decoded acknowledge for: " + ack.getHardwareId());
				DeviceCommandResponseCreateRequest request = new DeviceCommandResponseCreateRequest();
				request.setOriginatingEventId(header.getOriginator());
				request.setResponse(ack.getMessage());
				decoded.setHardwareId(ack.getHardwareId());
				decoded.setRequest(request);
				return decoded;
			}
			case DEVICELOCATION: {
				DeviceLocation location = DeviceLocation.parseDelimitedFrom(stream);
				LOGGER.debug("Decoded location for: " + location.getHardwareId());
				DeviceLocationCreateRequest request = new DeviceLocationCreateRequest();
				request.setLatitude(Double.parseDouble(String.valueOf(location.getLatitude())));
				request.setLongitude(Double.parseDouble(String.valueOf(location.getLongitude())));
				if (location.hasElevation()) {
					request.setElevation(Double.parseDouble(String.valueOf(location.getElevation())));
				}
				if (location.hasEventDate()) {
					request.setEventDate(new Date(location.getEventDate()));
				} else {
					request.setEventDate(new Date());
				}
				decoded.setHardwareId(location.getHardwareId());
				decoded.setRequest(request);
				return decoded;
			}
			default: {
				throw new SiteWhereException("Unable to decode message. Type not supported: "
						+ header.getCommand().name());
			}
			}
		} catch (IOException e) {
			throw new SiteWhereException("Unable to decode protobuf message.", e);
		}
	}
}