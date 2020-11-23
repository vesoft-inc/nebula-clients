/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.client.graph.storage.data;

import java.util.List;

public class Vertex {
    private String vid;
    private List<Tag> tags;

    public Vertex(String vid, List<Tag> tags) {
        this.vid = vid;
        this.tags = tags;
    }

    public String getVid() {
        return vid;
    }

    public void setVid(String vid) {
        this.vid = vid;
    }

    public List<Tag> getTags() {
        return tags;
    }

    public void setTags(List<Tag> tags) {
        this.tags = tags;
    }

    @Override
    public String toString() {
        return "Vertex{"
                + "vid=" + vid
                + ", tags=" + tags
                + '}';
    }
}

