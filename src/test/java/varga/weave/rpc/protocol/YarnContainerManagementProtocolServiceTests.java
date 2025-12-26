package varga.weave.rpc.protocol;

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

//import varga.weave.job.scheduler.k8s.FreemarkerRenderer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class YarnContainerManagementProtocolServiceTests {

    @Test
    public void test() {

//        FreemarkerRenderer freemarkerRenderer = new FreemarkerRenderer();
//
//        String result = YarnContainerManagementProtocolService.renderCommand(freemarkerRenderer, "user", "jobid", "bucket", "http://xxx");
//
//        assertThat(result).isEqualTo("""
//                /bin/sh <<'EOF'
//                touch /hadoop/yarn/local/filecache/.keep
//                aws s3 ls s3://bucket/tmp/hadoop-yarn/staging/user/.staging/jobid/ --endpoint-url http://xxx --no-verify-ssl --output json --debug
//                aws s3 cp s3://bucket/tmp/hadoop-yarn/staging/user/.staging/jobid/job.xml /hadoop/yarn/local/filecache/job.xml --endpoint-url http://xxx --no-verify-ssl --debug
//                aws s3 cp s3://bucket/tmp/hadoop-yarn/staging/user/.staging/jobid/job.split /hadoop/yarn/local/filecache/job.split --endpoint-url http://xxx --no-verify-ssl --debug
//                aws s3 cp s3://bucket/tmp/hadoop-yarn/staging/user/.staging/jobid/job.splitmetainfo /hadoop/yarn/local/filecache/job.splitmetainfo --endpoint-url http://xxx --no-verify-ssl --debug
//                echo '* listing /hadoop/yarn/local/filecache/'
//                ls -all /hadoop/yarn/local/filecache/
//                EOF
//                """);
    }
}
