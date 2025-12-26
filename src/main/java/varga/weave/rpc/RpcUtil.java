package varga.weave.rpc;

/*-
 * #%L
 * Weave
 * %%
 * Copyright (C) 2025 Varga Foundation
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */


import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;

import java.io.DataOutputStream;

public class RpcUtil {

    public static void sendMessages(DataOutputStream out, Message... messages)
            throws Exception {
        // compute length of non-null messages
        int length = 0;
        for (Message message : messages) {
            if (message != null) {
                int l = message.getSerializedSize();
                length += l + CodedOutputStream.computeRawVarint32Size(l);
            }
        }

        // write total packet length
        out.writeInt(length);

        // write non-null messages
        for (Message message : messages) {
            if (message != null) {
                message.writeDelimitedTo(out);
            }
        }

        out.flush();
    }
}
