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
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import java.io.Closeable;
import java.util.List;

public class KuduServer
        implements Closeable
{
    private static final Integer KUDU_MASTER_PORT = 7051;
    private static final Integer KUDU_TSERVER_PORT = 7050;

    private final GenericContainer<?> masterContainer;
    private final List<GenericContainer<?>> tServerContainers;

    public KuduServer()
    {
        this(1);
    }

    public KuduServer(int numberOfReplica)
    {
        Network network = Network.newNetwork();

        ImmutableList.Builder<GenericContainer<?>> tContainersBuilder = ImmutableList.builder();
        this.masterContainer = new GenericContainer<>("apache/kudu:1.10.0")
                .withExposedPorts(KUDU_MASTER_PORT)
                .withCommand("master")
                .withNetwork(network)
                .withNetworkAliases("kudu-master")
                .withEnv("MASTER_ARGS", "--fs_wal_dir=/var/lib/kudu/master --use_hybrid_clock=false --default_num_replicas=" + numberOfReplica);

        for (int instance = 0; instance < numberOfReplica; instance++) {
            String instanceName = "kudu-tserver-" + instance;
            tContainersBuilder.add(new GenericContainer<>("apache/kudu:1.10.0")
                    .withExposedPorts(KUDU_TSERVER_PORT)
                    .withCommand("tserver")
                    .withEnv("KUDU_MASTERS", "kudu-master:" + KUDU_MASTER_PORT)
                    .withNetwork(network)
                    .withNetworkAliases("kudu-tserver-" + instance)
                    .dependsOn(masterContainer)
                    .withEnv("TSERVER_ARGS", "--fs_wal_dir=/var/lib/kudu/tserver --use_hybrid_clock=false --rpc_advertised_addresses=" + instanceName));
        }
        this.tServerContainers = tContainersBuilder.build();
        masterContainer.start();
        tServerContainers.forEach(GenericContainer::start);
    }

    public String getMasterAddress()
    {
        return HostAndPort.fromParts(masterContainer.getContainerIpAddress(), masterContainer.getMappedPort(KUDU_MASTER_PORT)).toString();
    }

    public static void main(String[] args)
            throws InterruptedException
    {
        System.out.println(new KuduServer(1).getMasterAddress());
        Thread.sleep(10_000);
    }

    @Override
    public void close()
    {
        tServerContainers.forEach(GenericContainer::stop);
        masterContainer.stop();
    }
}
