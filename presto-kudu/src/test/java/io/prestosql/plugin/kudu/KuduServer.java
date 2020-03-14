
/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.kudu;

import com.google.common.net.HostAndPort;
import io.airlift.log.Logger;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.Closeable;
import java.io.File;

import static com.google.common.io.Resources.getResource;

public class KuduServer
        implements Closeable
{
    private static Logger log = Logger.get(KuduServer.class);

    private static final int KUDU_MASTER_PORT = 7051;
    private static final int KUDU_TSERVER_PORT = 7050;

    private final DockerComposeContainer<?> dockerComposeContainer;

    public KuduServer()
    {
        log.info("Starting kudu...");
        dockerComposeContainer = new DockerComposeContainer(new File(getResource("docker-compose.yml").getFile()))
                .withExposedService("kudu-master-1_1", KUDU_MASTER_PORT)
                .withExposedService("kudu-tserver-1_1", KUDU_TSERVER_PORT)
                .withExposedService("kudu-tserver-2_1", 7150)
                .withExposedService("kudu-tserver-3_1", 7250);

        dockerComposeContainer.start();
    }

    public HostAndPort getMasterAddress()
    {
        return HostAndPort.fromParts(
                dockerComposeContainer.getServiceHost("kudu-master-1_1", KUDU_MASTER_PORT),
                dockerComposeContainer.getServicePort("kudu-master-1_1", KUDU_MASTER_PORT));
    }

    public static void main(String[] args)
    {
        KuduServer kuduServer = new KuduServer();
        System.out.println("Start");
    }

    @Override
    public void close()
    {
        dockerComposeContainer.close();
    }
}
