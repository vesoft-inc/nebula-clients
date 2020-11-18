/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.encoder;

import com.vesoft.nebula.HostAddr;
import com.vesoft.nebula.meta.EdgeItem;
import com.vesoft.nebula.meta.IdName;
import com.vesoft.nebula.meta.SpaceItem;
import com.vesoft.nebula.meta.TagItem;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetaCacheImpl implements MetaCache {
    private Map<String,SpaceItem> spaceItems = new HashMap<>();
    private Map<String, TagItem> tagItems = new HashMap<>();
    private Map<String, EdgeItem> edgeItems = new HashMap<>();

    // TODO: Input metaClient
    public MetaCacheImpl() {
    }

    @Override
    public SpaceItem getSpace(String spaceName) {
        if (!spaceItems.containsKey(spaceName)) {
            return null;
        }
        return spaceItems.get(spaceName);
    }

    @Override
    public List<IdName> listSpaces() {
        return null;
    }


    @Override
    public TagItem getTag(String tagName) {
        if (!tagItems.containsKey(tagName)) {
            return null;
        }
        return tagItems.get(tagName);
    }

    @Override
    public List<TagItem> listTags() {
        return new ArrayList<>(tagItems.values());
    }

    @Override
    public EdgeItem getEdge(String edgeName) {
        if (!edgeItems.containsKey(edgeName)) {
            return null;
        }
        return edgeItems.get(edgeName);
    }

    @Override
    public List<EdgeItem> listEdges() {
        return new ArrayList<>(edgeItems.values());
    }

    @Override
    public Map<Integer, List<HostAddr>> getPartsAlloc(String spaceName) {
        return null;
    }
}
