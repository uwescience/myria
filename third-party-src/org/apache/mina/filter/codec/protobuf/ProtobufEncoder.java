/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.mina.filter.codec.protobuf;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolEncoderAdapter;
import org.apache.mina.filter.codec.ProtocolEncoderOutput;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;

/**
 * The protocol encoded used to encode protobuf {@link Message} into the wire format. The encoder is able to encode any
 * protobuf message and is stateles, so the same instance can be used by many threads. In fact the
 * {@link ProtobufCodecFactory} creates single instance of the encoder i.e. singleton.
 * 
 * The protobuf messages are designed to be as lightweight as possible, so there are very limited metadata included in
 * serialized form of the message. The size of the message have to be explicitly prepended before every message. The
 * encoded does so and encodes the size as protobuf varint (int32) value.
 * 
 * The instance of the encoder should be created by the {@link ProtobufCodecFactory}, however the protected constructor
 * has been provided for convenience.
 * 
 * @author Tomasz Blachowicz
 * @author The Apache MINA Project (dev@mina.apache.org)
 * @version $Rev$, $Date$
 */
public class ProtobufEncoder extends ProtocolEncoderAdapter {

  /**
   * Creates the instance of the encoder.
   */
  protected ProtobufEncoder() {
  }

  /**
   * Encodes the protobuf {@link Message} provided into the wire format.
   * 
   * @param session The session (not used).
   * @param message The protobuf {@link Message}.
   * @param out The encoder output used to write buffer into.
   */
  @Override
  public void encode(IoSession session, Object message, ProtocolEncoderOutput out) throws Exception {
    Message msg = (Message) message;
    int size = msg.getSerializedSize();
    IoBuffer buffer = IoBuffer.allocate(SizeContext.computeTotal(size) + 1);
    CodedOutputStream cos = CodedOutputStream.newInstance(buffer.asOutputStream());
    cos.writeRawByte((byte) 170);
    cos.writeRawVarint32(size);
    msg.writeTo(cos);
    cos.flush();
    buffer.flip();
    out.write(buffer);
  }
}