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
package io.prestosql.metadata;

import javax.annotation.concurrent.Immutable;

import static io.prestosql.metadata.NamePart.createDefaultNamePart;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

@Immutable
public class QualifiedObjectNamePart
{
    private final NamePart catalogName;
    private final NamePart schemaName;
    private final NamePart objectName;

    public static QualifiedObjectNamePart fromQualifiedObjectName(QualifiedObjectName qualifiedObjectName)
    {
        return new QualifiedObjectNamePart(
                createDefaultNamePart(qualifiedObjectName.getCatalogName()),
                createDefaultNamePart(qualifiedObjectName.getSchemaName()),
                createDefaultNamePart(qualifiedObjectName.getObjectName()));
    }

    public QualifiedObjectNamePart(NamePart catalogName, NamePart schemaName, NamePart objectName)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.objectName = requireNonNull(objectName, "objectName is null");
    }

    public NamePart getCatalogName()
    {
        return catalogName;
    }

    public String getLegacyCatalogName()
    {
        return catalogName.getValue().toLowerCase(ENGLISH);
    }

    public NamePart getSchemaName()
    {
        return schemaName;
    }

    public NamePart getObjectName()
    {
        return objectName;
    }

    public QualifiedObjectName asQualifiedObjectName()
    {
        return new QualifiedObjectName(catalogName.getValue().toLowerCase(ENGLISH), schemaName.getValue().toLowerCase(ENGLISH), objectName.getValue().toLowerCase(ENGLISH));
    }
}
