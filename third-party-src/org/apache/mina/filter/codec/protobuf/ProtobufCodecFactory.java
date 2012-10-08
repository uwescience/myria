/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
package org.apache.mina.filter.codec.protobuf;

import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFactory;
import org.apache.mina.filter.codec.ProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolEncoder;

import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;

/**
 * Factory that is being used by MINA for creating protobuf-specific
 * decoders and encoders. Instance of {@link ProtobufEncoder} is generic
 * and common for different types of protobuf messages encoded to wire
 * format. Every instance of the factory returns the same instance
 * of {@link ProtobufDecoder} on every call of getDecoder method, but every decoder
 * is specific for single given type of message passed as prototype while creating
 * the instance of factory.   
 * 
 * @author Tomasz Blachowicz
 * @author The Apache MINA Project (dev@mina.apache.org)
 * @version $Rev$, $Date$
 */
public class ProtobufCodecFactory implements ProtocolCodecFactory  {

	/**
	 * Instance of {@link ProtobufEncoder} (singleton).
	 */
	private static final ProtobufEncoder ENCODER_INSTANCE = new ProtobufEncoder(); 
	
	/**
	 * Factory method that creates instance of {@link ProtocolCodecFactory}
	 * for handling Protobuf wire format.
	 * 
	 * @param messagePrototype Protobuf message prototype used to acquire {@link Builder}.
	 * @return Instance of {@link ProtobufCodecFactory}.
	 */
	public static ProtobufCodecFactory newInstance(Message messagePrototype) {
		return new ProtobufCodecFactory(new ProtobufDecoder(messagePrototype));
	}

	/**
	 * Factory method that creates instance of {@link ProtocolCodecFactory}
	 * for handling Protobuf wire format.
	 * 
	 * @param messagePrototype Protobuf message prototype used to acquire {@link Builder}.
	 * @param extensionRegistry 
	 * @return 
	 */
	public static ProtobufCodecFactory newInstance(
		Message messagePrototype, ExtensionRegistry extensionRegistry) {
		return new ProtobufCodecFactory(new ProtobufDecoder(messagePrototype, extensionRegistry));
	}
	
	/**
	 * Instance of {@link ProtocolDecoder} that can deal with the Prtobuf wire format.
	 */
	private final ProtobufDecoder decoder;
	
	/**
	 * Private constructor used by factory methods.
	 * 
	 * @param decoder Instance of {@link ProtobufDecoder}.
	 */
	private ProtobufCodecFactory(ProtobufDecoder decoder) {
		this.decoder = decoder;
	}
	
	/**
	 * @param session Not in use
	 * @return Instance of {@link ProtobufDecoder}
	 */
	public ProtocolDecoder getDecoder(IoSession session) throws Exception {
		return decoder;
	}

	/**
	 * @param session Not in use
	 * @return Instance of {@link ProtobufEncoder}
	 */
	public ProtocolEncoder getEncoder(IoSession session) throws Exception {
		return ENCODER_INSTANCE;
	}	
	
}
