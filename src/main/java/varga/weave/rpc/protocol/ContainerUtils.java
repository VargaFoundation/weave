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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ContainerUtils {

    public static final String IP = "(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";
    private static final Pattern IPV4_PATTERN = Pattern.compile(IP);

    public static String renderCommand(String command) {
        command = command.replace("<LOG_DIR>", "/tmp");
        command = command.replace(" 1>/tmp/stdout 2>/tmp/stderr", "");
        command = command.replace("{{HADOOP_COMMON_HOME}}", "/usr/local/hadoop");
        command = command.replace("-Dlog4j.configuration=", "-Djava.util.logging.config.file=logging.properties -Dsun.security.krb5.debug=true -Dsun.security.spnego.debug=true -Dlog4j.configuration=");
        return command;
    }

    public static String changeIp(String newIp, String command) {
        Matcher matcher = IPV4_PATTERN.matcher(command);
        return matcher.replaceAll(newIp);
    }
}
