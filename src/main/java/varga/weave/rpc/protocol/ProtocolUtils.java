package varga.weave.rpc.protocol;

/*-
 * #%L
 * Weave
 * %%
 * Copyright (C) 2025 - 2026 Varga Foundation
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

import com.google.protobuf.UnknownFieldSet;

import java.nio.charset.StandardCharsets;

public class ProtocolUtils {

    public static final String MESSAGE = "Please check if the lib is present";

    public static String getRunId(UnknownFieldSet unknownFields) {
        try {
            return unknownFields.getField(104).getLengthDelimitedList().get(0).toString(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException("Can not find run in wire. " + MESSAGE);
        }
    }

    public static String getJobId(UnknownFieldSet unknownFields) {
        try {
            return unknownFields.getField(103).getLengthDelimitedList().get(0).toString(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException("Can not find job in wire. " + MESSAGE);
        }
    }

    public static String getProjectId(UnknownFieldSet unknownFields) {
        try {
            return unknownFields.getField(102).getLengthDelimitedList().get(0).toString(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException("Can not find project in wire. " + MESSAGE);
        }
    }

    public static String getBridgeId(UnknownFieldSet unknownFields) {
        try {
            return unknownFields.getField(101).getLengthDelimitedList().get(0).toString(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException("Can not find bridge in wire. " + MESSAGE);
        }
    }

    public static String getTenantId(UnknownFieldSet unknownFields) {
        try {
                return unknownFields.getField(100).getLengthDelimitedList().get(0).toString(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException("Can not find tenant in wire. " + MESSAGE);
        }
    }

    public static String getApplicationKey(UnknownFieldSet unknownFields) {
        try {
            return unknownFields.getField(97).getLengthDelimitedList().get(0).toString(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException("Can not find app key in wire. " + MESSAGE);
        }
    }

    public static String getApplicationSecret(UnknownFieldSet unknownFields) {
        try {
            return unknownFields.getField(98).getLengthDelimitedList().get(0).toString(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException("Can not find app secret in wire. " + MESSAGE);
        }
    }

    public static String getInfrastructureId(UnknownFieldSet unknownFields) {
        try {
            return unknownFields.getField(99).getLengthDelimitedList().get(0).toString(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException("Can not find infrastructure in wire. " + MESSAGE);
        }
    }
}
