#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

from nebula2.common import ttypes
from nebula2.Exception import (
    InvalidValueTypeException,
    InvalidKeyException
)


class ValueWrapper(object):
    def __init__(self, value, decode_type='utf-8'):
        self._value = value
        self._decode_type = decode_type

    def get_value(self):
        return self._value

    def is_null(self):
        return self._value.getType() == ttypes.Value.NVAL

    def is_empty(self):
        return self._value.getType() == ttypes.Value.__EMPTY__

    def is_bool(self):
        return self._value.getType() == ttypes.Value.BVAL

    def is_int(self):
        return self._value.getType() == ttypes.Value.IVAL

    def is_double(self):
        return self._value.getType() == ttypes.Value.FVAL

    def is_string(self):
        return self._value.getType() == ttypes.Value.SVAL

    def is_list(self):
        return self._value.getType() == ttypes.Value.LVAL

    def is_set(self):
        return self._value.getType() == ttypes.Value.UVAL

    def is_map(self):
        return self._value.getType() == ttypes.Value.MVAL

    def is_time(self):
        '''
        TODO: Need to wrapper TimeWrapper
        :return: ttypes.Time
        '''
        return self._value.getType() == ttypes.Value.TVAL

    def is_date(self):
        '''
        TODO: Need to wrapper DateWrapper
        :return: ttypes.Date
        '''
        return self._value.getType() == ttypes.Value.DVAL

    def is_datetime(self):
        '''
        TODO: Need to wrapper DateTimeWrapper
        :return: ttypes.DateTime
        '''
        return self._value.getType() == ttypes.Value.DTVAL

    def is_vertex(self):
        return self._value.getType() == ttypes.Value.VVAL

    def is_edge(self):
        return self._value.getType() == ttypes.Value.EVAL

    def is_path(self):
        return self._value.getType() == ttypes.Value.PVAL

    def as_bool(self):
        if self._value.getType() == ttypes.Value.BVAL:
            return self._value.get_bVal()
        raise InvalidValueTypeException("expect bool type, but is " + self._get_type_name())

    def as_int(self):
        if self._value.getType() == ttypes.Value.IVAL:
            return self._value.get_iVal()
        raise InvalidValueTypeException("expect bool type, but is " + self._get_type_name())

    def as_double(self):
        if self._value.getType() == ttypes.Value.FVAL:
            return self._value.get_fVal()
        raise InvalidValueTypeException("expect int type, but is " + self._get_type_name())

    def as_string(self):
        if self._value.getType() == ttypes.Value.SVAL:
            return self._value.get_sVal().decode(self._decode_type)
        raise InvalidValueTypeException("expect string type, but is " + self._get_type_name())

    def as_time(self):
        if self._value.getType() == ttypes.Value.TVAL:
            return self._value.get_tVal()
        raise InvalidValueTypeException("expect time type, but is " + self._get_type_name())

    def as_date(self):
        if self._value.getType() == ttypes.Value.DVAL:
            return self._value.get_dVal()
        raise InvalidValueTypeException("expect date type, but is " + self._get_type_name())

    def as_datetime(self):
        if self._value.getType() == ttypes.Value.DTVAL:
            return self._value.get_dtVal()
        raise InvalidValueTypeException("expect datetime type, but is " + self._get_type_name())

    def as_list(self):
        if self._value.getType() == ttypes.Value.LVAL:
            result = []
            for val in self._value.get_lVal():
                result.append(ValueWrapper(val))
            return result
        raise InvalidValueTypeException("expect list type, but is " + self._get_type_name())

    def as_set(self):
        if self._value.getType() == ttypes.Value.UVAL:
            result = set()
            for val in self._value.get_uVal().values:
                result.add(ValueWrapper(val))
            return result
        raise InvalidValueTypeException("expect set type, but is " + self._get_type_name())

    def as_map(self):
        if self._value.getType() == ttypes.Value.MVAL:
            result = {}
            kvs = self._value.get_mVal()
            for key in kvs:
                result[key] = ValueWrapper(kvs[key])
            return result
        raise InvalidValueTypeException("expect map type, but is " + self._get_type_name())

    def as_node(self):
        if self._value.getType() == ttypes.Value.VVAL:
            return Node(self._value.get_vVal())
        raise InvalidValueTypeException("expect vertex type, but is " + self._get_type_name())

    def as_relationship(self):
        if self._value.getType() == ttypes.Value.EVAL:
            return Relationship(self._value.get_eVal())
        raise InvalidValueTypeException("expect edge type, but is " + self._get_type_name())

    def as_path(self):
        if self._value.getType() == ttypes.Value.PVAL:
            return Path(self._value.get_pVal())
        raise InvalidValueTypeException("expect path type, but is " + self._get_type_name())

    def _get_type_name(self):
        if self.is_empty():
            return "empty"
        if self.is_null():
            return "null"
        if self.is_bool():
            return "bool"
        if self.is_int():
            return "int"
        if self.is_double():
            return "double"
        if self.is_string():
            return "string"
        if self.is_list():
            return "list"
        if self.is_set():
            return "set"
        if self.is_map():
            return "map"
        if self.is_time():
            return "time"
        if self.is_date():
            return "date"
        if self.is_datetime():
            return "datetime"
        if self.is_vertex():
            return "vertex"
        if self.is_edge():
            return "edge"
        if self.is_path():
            return "path"
        return "unknown"


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
    def __init__(self, vertex, decode_type='utf-8'):
        self._value = vertex
        self._tag_indexes = dict()
        self._decode_type = decode_type
        for index, tag in enumerate(self._value.tags, start=0):
            self._tag_indexes[tag.name.decode(self._decode_type)] = index

    def get_id(self):
        return self._value.vid.decode(self._decode_type)

    def tags(self):
        return list(self._tag_indexes.keys())

    def has_tag(self, tag):
        return True if tag in self._tag_indexes.keys() else False

    def propertys(self, tag):
        if tag not in self._tag_indexes.keys():
            raise InvalidKeyException(tag)

        props = self._value.tags[self._tag_indexes[tag]].props
        result_props = {}
        for key in props.keys():
            result_props[key.decode(self._decode_type)] = ValueWrapper(props[key])
        return result_props

    def prop_names(self, tag):
        if tag not in self._tag_indexes.keys():
            raise InvalidKeyException(tag)
        index = self._tag_indexes[tag]
        return [(key.decode(self._decode_type)) for key in self._value.tags[index].props.keys()]

    def prop_values(self, tag):
        if tag not in self._tag_indexes.keys():
            raise InvalidKeyException(tag)
        index = self._tag_indexes[tag]
        return [(ValueWrapper(value)) for value in self._value.tags[index].props.values()]

    def __repr__(self):
        tag_str_list = list()
        for tag in self._value.tags:
            tag_str_list.append('{' + 'tag_name: {}'.format(tag.name)
                                + ', props: {}'.format(tag.props) + '}')
        return '{%s}([%s]:{%s})' % (self.__class__.__name__, self._value.vid, ','.join(tag_str_list))

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        return self.get_id() == other.get_id()

    def __ne__(self, other):
        return not (self == other)


class Relationship(object):
    def __init__(self, edge, decode_type='utf-8'):
        self._decode_type = decode_type
        self._value = edge

    def start_vertex_id(self):
        return self._value.src.decode(self._decode_type)

    def end_vertex_id(self):
        return self._value.dst.decode(self._decode_type)

    def edge_name(self):
        return self._value.name.decode(self._decode_type)

    def ranking(self):
        return self._value.ranking

    def propertys(self):
        props = {}
        for key in self._value.props.keys():
            props[key.decode(self._decode_type)] = ValueWrapper(self._value.props[key])
        return props

    def keys(self):
        return [(key.decode(self._decode_type)) for key in self._value.props.keys()]

    def values(self):
        return [(ValueWrapper(value)) for value in self._value.props.values]

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
        return self.start_vertex_id() == other.start_vertex_id() \
               and self.end_vertex_id() == other.end_vertex_id() \
               and self.edge_name() == other.edge_name() \
               and self.ranking() == self.ranking()

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

        vids = []
        vids.append(path.src.vid)
        for step in self._path.steps:
            type = step.type
            if step.type > 0:
                start_node = self._nodes[-1]
                end_node = Node(step.dst)
                src_id = vids[-1]
                dst_id = step.dst.vid
            else:
                type = -type
                end_node = self._nodes[-1]
                start_node = Node(step.dst)
                dst_id = vids[-1]
                src_id = step.dst.vid
            vids.append(step.dst.vid)
            relationship = Relationship(GenValue.gen_edge(src_id,
                                                          dst_id,
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
