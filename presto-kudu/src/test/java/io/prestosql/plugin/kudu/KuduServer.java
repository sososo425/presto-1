
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

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import io.airlift.log.Logger;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;

import java.io.Closeable;
import java.util.List;

public class KuduServer
        implements Closeable
{
    private static Logger log = Logger.get(KuduServer.class);

    private static final List<Integer> KUDU_MASTER_PORTS = ImmutableList.of(7051, 7151, 7251);
    private static final List<Integer> KUDU_TSERVER_PORTS = ImmutableList.of(7050, 7150, 7250);

    private final List<GenericContainer<?>> masterContainers;
    private final List<GenericContainer<?>> tServerContainers;

    public KuduServer()
    {
        log.info("Starting kudu...");
        ImmutableList.Builder<GenericContainer<?>> masterBuilder = ImmutableList.builder();
        ImmutableList.Builder<GenericContainer<?>> tServerBuilder = ImmutableList.builder();
        String address =  DockerClientFactory.instance().dockerHostIpAddress();
        String kuduMasters = address + ":7051," + address + ":7151," + address + ":7251";
        for (Integer masterPort : KUDU_MASTER_PORTS) {
            masterBuilder.add(new GenericContainer<>("apache/kudu-1.10.0")
                    .withExposedPorts(masterPort)
                    .withCommand("master")
                    .withEnv("KUDU_MASTERS", kuduMasters));
        }
        masterContainers = masterBuilder.build();
        for (Integer slavePort : KUDU_TSERVER_PORTS) {
            tServerBuilder.add(new GenericContainer<>("apache/kudu-1.10.0")
                    .withExposedPorts(slavePort)
                    .withCommand("tserver")
                    .withEnv("KUDU_MASTERS", kuduMasters));
        }
        tServerContainers = tServerBuilder.build();
        masterContainers.forEach(GenericContainer::start);
        tServerContainers.forEach(GenericContainer::start);
    }

    public HostAndPort getMasterAddress()
    {
        return HostAndPort.fromParts(
                masterContainers.get(0).getContainerIpAddress(),
                masterContainers.get(0).getMappedPort(7051));
    }

    @Override
    public void close()
    {
        masterContainers.forEach(GenericContainer::stop);
        tServerContainers.forEach(GenericContainer::stop);
    }
}
