/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.encoder;

import com.vesoft.nebula.meta.ColumnDef;
import com.vesoft.nebula.meta.ColumnTypeDef;
import com.vesoft.nebula.meta.EdgeItem;
import com.vesoft.nebula.meta.Schema;
import com.vesoft.nebula.meta.TagItem;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

public class NebulaCodecImpl implements NebulaCodec {
    private static final int PARTITION_ID_SIZE = 4;
    private static final int VERTEX_ID_SIZE = 8;
    private static final int TAG_ID_SIZE = 4;
    private static final int TAG_VERSION_SIZE = 8;
    private static final int EDGE_TYPE_SIZE = 4;
    private static final int EDGE_RANKING_SIZE = 8;
    private static final int EDGE_VERSION_SIZE = 8;
    private static final int VERTEX_SIZE = PARTITION_ID_SIZE + VERTEX_ID_SIZE
            + TAG_ID_SIZE + TAG_VERSION_SIZE;
    private static final int EDGE_SIZE = PARTITION_ID_SIZE + VERTEX_ID_SIZE
            + EDGE_TYPE_SIZE + EDGE_RANKING_SIZE + VERTEX_ID_SIZE + EDGE_VERSION_SIZE;

    private static final int DATA_KEY_TYPE = 0x00000001;
    private static final int TAG_MASK      = 0xBFFFFFFF;
    private static final int EDGE_MASK     = 0x40000000;
    private final MetaCache metaCache;

    public NebulaCodecImpl(MetaCache metaCache) {
        this.metaCache = metaCache;
    }

    @Override
    public byte[] vertexKey(int partitionId, byte[] vertexId, int tagId, long tagVersion) {
        ByteBuffer buffer = ByteBuffer.allocate(VERTEX_SIZE);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        partitionId = (partitionId << 8) | DATA_KEY_TYPE;
        buffer.putInt(partitionId);
        buffer.put(vertexId);
        tagId &= TAG_MASK;
        buffer.putInt(tagId);
        buffer.putLong(tagVersion);
        return buffer.array();
    }

    @Override
    public byte[] edgeKey(int partitionId,
                          byte[] srcId,
                          int edgeType,
                          long edgeRank,
                          byte[] dstId,
                          long edgeVersion) {
        ByteBuffer buffer = ByteBuffer.allocate(EDGE_SIZE);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        partitionId = (partitionId << 8) | DATA_KEY_TYPE;
        buffer.putInt(partitionId);
        buffer.put(srcId);
        edgeType |= EDGE_MASK;
        buffer.putInt(edgeType);
        buffer.putLong(edgeRank);
        buffer.put(dstId);
        buffer.putLong(edgeVersion);
        return buffer.array();
    }

    public SchemaProviderImpl genSchemaProvider(long ver, Schema schema) {
        SchemaProviderImpl schemaProvider = new SchemaProviderImpl(ver);
        for (ColumnDef col : schema.getColumns()) {
            ColumnTypeDef type = col.getType();
            boolean nullable = col.nullable;
            boolean hasDefault = col.isSetDefault_value();
            int len = type.isSetType_length() ? type.getType_length() : 0;
            schemaProvider.addField(new String(col.getName()),
                                    type.type,
                                    len,
                                    nullable,
                                    hasDefault ? null : null);
        }
        return schemaProvider;
    }

    @Override
    public byte[] encode(String schemaName, List<String> names, List<Object> values)
        throws RuntimeException {
        if (names == null || values == null || names.size() != values.size()) {
            throw new RuntimeException("NebulaCodeImpl input wrong value");
        }

        TagItem tag = metaCache.getTag(schemaName);
        Schema schema;
        long ver;
        if (tag == null) {
            EdgeItem edge = metaCache.getEdge(schemaName);
            if (edge == null) {
                throw new RuntimeException("Schema name `" + schemaName + "' is not exist.");
            }
            schema = edge.schema;
            ver = edge.version;
        } else {
            schema = tag.schema;
            ver = tag.version;
        }
        RowWriterImpl writer = new RowWriterImpl(genSchemaProvider(ver, schema));
        for (int i = 0; i < names.size(); i++) {
            writer.setValue(names.get(i), values.get(i));
        }
        writer.finish();
        return writer.encodeStr();
    }
}
