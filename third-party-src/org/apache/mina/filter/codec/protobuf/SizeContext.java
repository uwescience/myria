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

import java.io.IOException;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;

/**
 * Internal class used to control expected size of data buffer.
 * The instance is being stored in {@link IoSession} between
 * ProtobufEncoder.doDecode calls.
 * 
 * @author Tomasz Blachowicz
 * @author The Apache MINA Project (dev@mina.apache.org)
 * @version $Rev$, $Date$
 */
class SizeContext {
	
	/**
	 * The key of attribute in {@link IoSession}.
	 */
	static String KEY = String.format("%s#KEY", SizeContext.class.getCanonicalName());
	
	/**
	 * Expected size of the data needed to decode message.
	 */
	private final int size;

	/**
	 * Retrieves instance of the context form given {@link IoSession}. If there is no
	 * attribute holding the instance of context the new one is created. In that
	 * case the size of the message is being read from {@link IoBuffer} provided.
	 * The size is encoded as protobuf varint (int32).   
	 * 
	 * @param session the session that holds instance of context as attribute 
	 * @param in
	 * @return
	 * @throws IOException
	 */
	static SizeContext get(IoSession session, IoBuffer in) throws IOException {
		SizeContext ctx = (SizeContext) session.getAttribute(KEY);
		if(ctx == null) {
			int size = CodedInputStream.newInstance(in.array(), in.position(), 5).readRawVarint32();
			ctx = new SizeContext(SizeContext.computeTotal(size));
			session.setAttribute(KEY, ctx);
		}
		return ctx;
	}
	
	/**
	 * Computes total size of the message that includes the size
	 * itself encoded as protobuf varint (int32). In contrast to
	 * the fixed int32 encoding varint may span from 1 up to 5 bytes.
	 * 
	 * The method is being used by the {@link ProtobufEncoder} to
	 * allocate {@link IoBuffer}.
	 * 
	 * @param size Size of the message.
	 * @return The total size (message size and size itself).
	 */
	static int computeTotal(int size) {
		return size + CodedOutputStream.computeRawVarint32Size(size);
	}
	
	/**
	 * Creates instance of context.
	 * 
	 * @param size The size of the message.
	 */
	private SizeContext(int size) {
		this.size = size;
	}
	
	/**
	 * Determines weather the buffer contains enough data
	 * to decode message. Uses method IoBuffer.remaining.
	 * 
	 * @param in The buffer
	 * @return <code>true</code> if buffer contains enough data,
	 * otherwise <code>false</code>.
	 */
	boolean hasEnoughData(IoBuffer in) {
		return in.remaining() >= size;
	}
	
	/**
	 * Creates the protobuf {@link CodedInputStream} from {@link IoBuffer}.
	 * To avoid consumption of the whole buffer uses internal array and size
	 * to extract only the slice of the buffer as source for the stream.
	 * 
	 * @param in The buffer
	 * @return The protobuf stream used to read messages.
	 */
	CodedInputStream getInputStream(IoBuffer in) {
		return CodedInputStream.newInstance(
			in.array(),
			in.position(),
			size);
	}
	
	/**
	 * Shifts position of the {@link IoBuffer} and removes
	 * the context from {@link IoSession}. This method is
	 * being called once the message has been successfully
	 * decoded and the framework can proceed with the next
	 * message in sequence.
	 * 
	 * @param session The session
	 * @param in The buffer
	 */
	void shiftPositionAndReset(IoSession session, IoBuffer in) {
		in.position(in.position() + size);
		session.removeAttribute(KEY);
	}
	
}