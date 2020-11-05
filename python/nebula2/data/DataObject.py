#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

from nebula2.common import ttypes
from nebula2.Exception import (
    OutOfRangeException,
    InvalidKeyException
)


class ConvertValue(object):
    @classmethod
    def convert(cls, value):
        if value.getType() == ttypes.Value.VVAL:
            return Node(value.get_vVal())

        if value.getType() == ttypes.Value.EVAL:
            return Relationship(value.get_eVal())

        if value.getType() == ttypes.Value.PVAL:
            return Path(value.get_pVal())
        return value

    @classmethod
    def as_node(cls, value):
        if value.getType() != ttypes.Value.VVAL:
            return None
        return Node(value.get_vVal())

    @classmethod
    def as_relationship(cls, value):
        if value.getType() != ttypes.Value.EVAL:
            return None
        return Relationship(value.get_eVal())

    @classmethod
    def as_path(cls, value):
        if value.getType() != ttypes.Value.PVAL:
            return None
        return Path(value.get_pVal())


class GenValue(object):
    @classmethod
    def gen_vertex(cls, vid, tags):
        vertex = ttypes.Vertex()
        vertex.vid = vid
        vertex.tags = tags
        return vertex

    @classmethod
    def gen_edge(cls, src_id, dst_id, type, edge_name, ranking, props):
        edge = ttypes.Edge()
        edge.src = src_id
        edge.dst = dst_id
        edge.type = type
        edge.name = edge_name
        edge.ranking = ranking
        edge.props = props
        return edge

    @classmethod
    def gen_segment(cls, start_node, end_node, relationship):
        segment = Segment()
        segment.start_node = start_node
        segment.end_node = end_node
        segment.relationship = relationship
        return segment


class Node(object):
    def __init__(self, vertex):
        self._value = vertex
        self._tag_indexes = dict()
        for index, tag in enumerate(self._value.tags, start=0):
            self._tag_indexes[tag.name] = index

    def get_id(self):
        return self._value.vid

    def tags(self):
        return list(self._tag_indexes.keys())

    def has_tag(self, tag):
        return True if tag in self._tag_indexes.keys() else False

    def propertys(self, tag):
        if tag not in self._tag_indexes.keys():
            raise InvalidKeyException(tag)
        return self._value.tags[self._tag_indexes[tag]].props

    def prop_names(self, tag):
        if tag not in self._tag_indexes.keys():
            raise InvalidKeyException(tag)
        return list(self._value.tags[self._tag_indexes[tag]].props.keys())

    def prop_values(self, tag):
        if tag not in self._tag_indexes.keys():
            raise InvalidKeyException(tag)
        return list(self._value.tags[self._tag_indexes[tag]].props.values())

    def __repr__(self):
        tag_str_list = list()
        for tag in self._value.tags:
            tag_str_list.append('{' + 'tag_name: {}'.format(tag.name) + ', props: {}'.format(tag.props) + '}')
        return '{%s}([%s]:{%s})' % (self.__class__.__name__, self._value.vid, ','.join(tag_str_list))

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class Relationship(object):
    def __init__(self, edge):
        self._value = edge

    def start_vertex_id(self):
        return self._value.src

    def end_vertex_id(self):
        return self._value.dst

    def edge_name(self):
        return self._value.name

    def ranking(self):
        return self._value.ranking

    def propertys(self):
        return self._value.props

    def keys(self):
        return list(self._value.props.keys())

    def values(self):
        return list(self._value.props.values())

    def __repr__(self):
        return "{}([{}-[{}({})]->{}@{}]:{})".format(self.__class__.__name__,
                                                    self._value.src,
                                                    self._value.name,
                                                    self._value.type,
                                                    self._value.dst,
                                                    self._value.ranking,
                                                    self._value.props)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class Segment(object):
    start_node = None
    end_node = None
    relationship = None

    def __repr__(self):
        return "{}(start_node: {}, relations: {}, end_node: {})".format(self.__class__.__name__,
                                                                        self.start_node,
                                                                        self.relationship,
                                                                        self.end_node)


class Path(object):
    def __init__(self, path):
        self._nodes = list()
        self._segments = list()
        self._relationships = list()

        self._path = path
        self._nodes.append(Node(path.src))

        for step in self._path.steps:
            type = step.type
            if step.type > 0:
                start_node = self._nodes[-1]
                end_node = Node(step.dst)
            else:
                type = -type
                end_node = self._nodes[-1]
                start_node = Node(step.dst)
            relationship = Relationship(GenValue.gen_edge(start_node.get_id(),
                                                          end_node.get_id(),
                                                          type,
                                                          step.name,
                                                          step.ranking,
                                                          step.props))

            self._relationships.append(relationship)
            segment = GenValue.gen_segment(start_node, end_node, relationship)
            if segment.start_node == self._nodes[-1]:
                self._nodes.append(segment.end_node)
            elif segment.end_node == self._nodes[-1]:
                self._nodes.append(segment.start_node)
            else:
                raise Exception("Relationship [{}] does not connect to the last node".
                                format(relationship))

            self._segments.append(segment)

    def __iter__(self):
        return self._segments.popleft()

    def start_node(self):
        if len(self._nodes) == 0:
            return None
        return self._nodes[0]

    def length(self):
        return len(self._segments)

    def contain_node(self, node):
        return True if node in self._nodes else False

    def contain_relationship(self, relationship):
        return True if relationship in self._relationships else False

    def nodes(self):
        return self._nodes

    def relationships(self):
        return self._relationships

    def segments(self):
        return self._segments

    def __repr__(self):
        return "{}(segments: {})".format(self.__class__.__name__, self._segments)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
