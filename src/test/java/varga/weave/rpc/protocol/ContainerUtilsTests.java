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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ContainerUtilsTests {

    @Test
    public void logs() {

        String command = ContainerUtils.renderCommand("/usr/local/openjdk-11/bin/java -Djava.io.tmpdir=/usr/local/hadoop/tmp -Dlog4j.configuration=container-log4j.properties -Dyarn.app.container.log.dir=<LOG_DIR> -Dyarn.app.container.log.filesize=0 -Dhadoop.root.logger=INFO,CLA -Dhadoop.root.logfile=syslog -Xmx1024m org.apache.hadoop.mapreduce.v2.app.MRAppMaster 1><LOG_DIR>/stdout 2><LOG_DIR>/stderr");

        assertThat(command).isEqualTo("/usr/local/openjdk-11/bin/java -Djava.io.tmpdir=/usr/local/hadoop/tmp -Dlog4j.configuration=container-log4j.properties -Dyarn.app.container.log.dir=/tmp -Dyarn.app.container.log.filesize=0 -Dhadoop.root.logger=INFO,CLA -Dhadoop.root.logfile=syslog -Xmx1024m org.apache.hadoop.mapreduce.v2.app.MRAppMaster");
    }

    @Test
    public void testIp() {
        assertThat(ContainerUtils.changeIp("XXX", "192.168.1.1")).isEqualTo("XXX");
        assertThat(ContainerUtils.changeIp("XXX", "ccc 192.168.1.1 aaa")).isEqualTo("ccc XXX aaa");
    }

    @Test
    public void ipForYarnChild() {

        String command = ContainerUtils.changeIp("XXX", "$JAVA_HOME/bin/java -Djava.net.preferIPv4Stack=true -Dhadoop.metrics.log.level=WARN   -Xmx820m -Djava.io.tmpdir=$PWD/tmp -Dlog4j.configuration=container-log4j.properties -Dyarn.app.container.log.dir=<LOG_DIR> -Dyarn.app.container.log.filesize=0 -Dhadoop.root.logger=INFO,CLA -Dhadoop.root.logfile=syslog org.apache.hadoop.mapred.YarnChild 192.168.65.2 50020 attempt_7284806354291807208_1772947990_m_000000_0 1 1><LOG_DIR>/stdout 2><LOG_DIR>/stderr ");
        assertThat(command).isEqualTo("$JAVA_HOME/bin/java -Djava.net.preferIPv4Stack=true -Dhadoop.metrics.log.level=WARN   -Xmx820m -Djava.io.tmpdir=$PWD/tmp -Dlog4j.configuration=container-log4j.properties -Dyarn.app.container.log.dir=<LOG_DIR> -Dyarn.app.container.log.filesize=0 -Dhadoop.root.logger=INFO,CLA -Dhadoop.root.logfile=syslog org.apache.hadoop.mapred.YarnChild XXX 50020 attempt_7284806354291807208_1772947990_m_000000_0 1 1><LOG_DIR>/stdout 2><LOG_DIR>/stderr ");
    }
}
