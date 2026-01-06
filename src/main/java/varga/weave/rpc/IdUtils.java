package varga.weave.rpc;

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

import org.apache.hadoop.yarn.proto.YarnProtos;

import java.net.URI;

public class IdUtils {

    // container_1481156246874_0001_01_000001
    // container_<<cluster id>>_<<application id>>_<<attempt id>>_<<container id>>
    // A string representation of containerId. The format is container_e*epoch*_*clusterTimestamp*_*appId*_*attemptId*_*containerId*
    // when epoch is larger than 0 (e.g. container_e17_1410901177871_0001_01_000005). *epoch* is increased when RM restarts or fails over.
    // When epoch is 0, epoch is omitted (e.g. container_1410901177871_0001_01_000005).
    public static String getContainerId(YarnProtos.ApplicationIdProto applicationId,
                                        YarnProtos.ApplicationAttemptIdProto applicationAttemptId,
                                        YarnProtos.ContainerIdProto containerId) {
        return "container_" +
                String.format("%013d", applicationId.getClusterTimestamp()) +
                "_" +
                String.format("%04d", applicationId.getId()) +
                "_" +
                String.format("%02d", applicationAttemptId != null ? applicationAttemptId.getAttemptId() : 1) +
                "_" +
                String.format("%06d", containerId != null ? containerId.getId() : 1);
    }

    public static String getContainerId(YarnProtos.ContainerIdProto containerId) {
        return getContainerId(containerId.getAppId(), containerId.getAppAttemptId(), containerId);
    }

    // application_1481156246874_0001
    // application_<<cluster id>>_<<application id>>
    public static String getApplicationId(YarnProtos.ApplicationIdProto applicationId) {
        return "application_" +
                String.format("%013d", applicationId.getClusterTimestamp()) +
                "_" +
                String.format("%04d", applicationId.getId());
    }

    // job_1481156246874_0001
    // job_<<cluster id>>_<<application id>>
    public static String getJobId(YarnProtos.ApplicationIdProto applicationId) {
        return "job_" +
                String.format("%013d", applicationId.getClusterTimestamp()) +
                "_" +
                String.format("%04d", applicationId.getId());
    }

    // http://a7dbd1f99879bd05f8a8851e64b10935f437df0b-job-kwh9c:19888/jobhistory/job/job_3981105851362488722_928891646
    public static Long getClusterIdFromTrackingUrl(String trackingUrl) {
        return Long.valueOf(URI.create(trackingUrl).getPath().replace("/jobhistory/job/job_", "")
                .split("_")[0]);
    }

    // http://a7dbd1f99879bd05f8a8851e64b10935f437df0b-job-kwh9c:19888/jobhistory/job/job_3981105851362488722_928891646
    public static int getApplicationIdFromTrackingUrl(String trackingUrl) {
        return Integer.valueOf(URI.create(trackingUrl).getPath().replace("/jobhistory/job/job_", "")
                .split("_")[1]);
    }
}
