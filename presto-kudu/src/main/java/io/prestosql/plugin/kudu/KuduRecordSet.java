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

import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.RecordSet;
import io.prestosql.spi.type.Type;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduTable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.kudu.KuduColumnHandle.ROW_ID_POSITION;

public class KuduRecordSet
        implements RecordSet
{
    private final KuduClientSession clientSession;
    private final KuduSplit kuduSplit;
    private final List<KuduColumnHandle> columns;
    private final boolean containsVirtualRowId;

    public KuduRecordSet(KuduClientSession clientSession, KuduSplit kuduSplit, List<? extends ColumnHandle> columns)
    {
        this.clientSession = clientSession;
        this.kuduSplit = kuduSplit;
        this.columns = columns.stream().map(KuduColumnHandle.class::cast).collect(toImmutableList());
        this.containsVirtualRowId = columns.contains(KuduColumnHandle.ROW_ID_HANDLE);
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columns.stream()
                .map(KuduColumnHandle::getType)
                .collect(toImmutableList());
    }

    @Override
    public RecordCursor cursor()
    {
        KuduScanner scanner = clientSession.createScanner(kuduSplit);
        if (!containsVirtualRowId) {
            Map<Integer, Integer> map = new HashMap<>();
            int index = 0;
            for (KuduColumnHandle columnHandle : columns) {
                map.put(index, scanner.getProjectionSchema().getColumnIndex(columnHandle.getName()));
                index++;
            }
            return new KuduRecordCursorWithVirtualRowId(scanner, getTable(), getColumnTypes(), map);
        }
        else {
            Map<Integer, Integer> fieldMapping = new HashMap<>();
            for (int i = 0; i < columns.size(); i++) {
                KuduColumnHandle handle = columns.get(i);
                if (!handle.isVirtualRowId()) {
                    fieldMapping.put(i, handle.getOrdinalPosition());
                }
                else {
                    fieldMapping.put(i, ROW_ID_POSITION);
                }
            }

            KuduTable table = getTable();
            return new KuduRecordCursorWithVirtualRowId(scanner, table, getColumnTypes(), fieldMapping);
        }
    }

    KuduTable getTable()
    {
        return kuduSplit.getTableHandle().getTable(clientSession);
    }

    KuduClientSession getClientSession()
    {
        return clientSession;
    }
}
