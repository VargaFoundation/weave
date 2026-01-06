package varga.weave.virtual;

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

import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import varga.weave.core.Job;
import varga.weave.core.JobManagerInputPort;
import varga.weave.core.JobRun;
import varga.weave.core.Project;
import varga.weave.core.Tenant;

import java.time.Instant;
import java.util.UUID;

@Service
public class VirtualJobManagerAdapter implements JobManagerInputPort {

    @Override
    public Mono<Job> createJob(Tenant tenant, Project project, Job job) {
        if (job.getId() == null) {
            job.setId(UUID.randomUUID().toString());
        }
        job.setProjectId(project.getId());
        job.setCreated(Instant.now());
        return Mono.just(job);
    }

    @Override
    public Mono<JobRun> createRunForJob(Tenant tenant, String jobId, JobRun run) {
        if (run.getId() == null) {
            run.setId(UUID.randomUUID().toString());
        }
        run.setJobId(jobId);
        run.setCreated(Instant.now());
        run.setStatus("running");
        return Mono.just(run);
    }

    @Override
    public Mono<Job> findJobByJobId(Tenant tenant, String jobId) {
        Job job = new Job();
        job.setId(jobId);
        job.setName("Virtual Job " + jobId);
        job.setCreated(Instant.now());
        return Mono.just(job);
    }

    @Override
    public Mono<JobRun> findRunByJobIdAndRunId(Tenant tenant, String jobId, String runId) {
        JobRun run = new JobRun();
        run.setId(runId);
        run.setJobId(jobId);
        run.setStatus("running");
        run.setCreated(Instant.now());
        return Mono.just(run);
    }
}
