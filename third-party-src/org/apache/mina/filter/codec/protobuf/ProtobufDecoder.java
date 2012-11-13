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
import org.apache.mina.filter.codec.CumulativeProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolDecoderOutput;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.Message;

/**
 * Protocol decoder used to decode protobuf messages from the wire. It is based on {@link CumulativeProtocolDecoder} to
 * deal with fragmented messages handled by the framework in asynchronous fashion.
 * 
 * This decoder handles single type of messages provided as prototype. However the registry of extensions
 * {@link ExtensionRegistry} can also be provided.
 * 
 * The instance of the decoder should be created by the {@link ProtobufCodecFactory}, however the protected constructors
 * has been provided for convenience.
 * 
 * @author Tomasz Blachowicz
 * @author The Apache MINA Project (dev@mina.apache.org)
 * @version $Rev$, $Date$
 */
public class ProtobufDecoder extends CumulativeProtocolDecoder {

  /**
   * The prototype message used to decode messages.
   */
  private final Message prototype;

  /**
   * The registry of extensions used during decoding messages with extensions.
   */
  private final ExtensionRegistry extentions;

  /**
   * Creates instance of decoder specific for given message type.
   * 
   * @param prototype The prototype message used to decode messages.
   */
  protected ProtobufDecoder(Message prototype) {
    this(prototype, ExtensionRegistry.getEmptyRegistry());
  }

  /**
   * Creates instance of decoder specific for given message type.
   * 
   * @param prototype The prototype message used to decode messages.
   * @param extensionRegistry The registry of extensions used during decoding messages with extensions.
   */
  protected ProtobufDecoder(Message prototype, ExtensionRegistry extensionRegistry) {
    this.prototype = prototype;
    extentions = extensionRegistry;
  }

  /**
   * Decodes protobuf messages of given type from buffer. If not enough data has been presented delegates to the
   * {@link CumulativeProtocolDecoder} base class to read more data from the wire.
   * 
   * It uses instance of internal {@link SizeContext} class to calculate size of buffer expected by the given type of
   * message. The size of every message that arrives on the wire is specified by the prepending varint value.
   * 
   * @param session The session used to store internal {@link SizeContext}.
   * @param in The buffer used to read messages if contains enough data.
   * @param out The output for messages decoded from data provided.
   * 
   * @see ProtobufEncoder
   * @see SizeContext
   */
  @Override
  protected boolean doDecode(IoSession session, IoBuffer in, ProtocolDecoderOutput out) throws Exception {
    // 5: the maxlength of a varint32
    // byte[] dataArray = in.array();
    // int inPosition = in.position();
    // if (dataArray[inPosition] != (byte) 170) {
    // System.err.println("error coding.");
    // in.get();
    // return false;
    // }

    int sizeCodedMaxLength = 5;
    if (in.remaining() < sizeCodedMaxLength) {
      sizeCodedMaxLength = in.remaining();
    }

    int dataSize = 0;

    try {
      dataSize = CodedInputStream.newInstance(in.array(), in.position(), sizeCodedMaxLength).readRawVarint32();
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }

    int dataSizeOccupiedSize = CodedOutputStream.computeRawVarint32Size(dataSize);
    if (dataSize + dataSizeOccupiedSize <= in.remaining()) {
      try {
        Message.Builder builder = prototype.newBuilderForType();
        CodedInputStream.newInstance(in.array(), in.position(), dataSize + dataSizeOccupiedSize).readMessage(builder,
            extentions);
        out.write(builder.build());
        return true;
      } finally {
        in.position(in.position() + dataSize + dataSizeOccupiedSize);
      }
    }
    return false;
  }
}
