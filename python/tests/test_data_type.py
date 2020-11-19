#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.join(current_dir, '..')
sys.path.insert(0, root_dir)

from nebula2.common import ttypes
from nebula2.graph import ttypes as graphTtype
from unittest import TestCase
from nebula2.gclient.data.ResultSet import ResultSet
from nebula2.gclient.data.DataObject import (
    ValueWrapper,
    Node,
    Relationship,
    Path
)


class TestBaseCase(TestCase):
    @classmethod
    def get_vertex_value(self, vid):
        vertex = ttypes.Vertex()
        vertex.vid = vid
        vertex.tags = list()
        for i in range(0, 3):
            tag = ttypes.Tag()
            tag.name = 'tag{}'.format(i)
            tag.props = dict()
            for j in range(0, 5):
                tag.props['prop{}'.format(j)] = j
            vertex.tags.append(tag)
        return vertex

    @classmethod
    def get_edge_value(self, src_id, dst_id):
        edge = ttypes.Edge()
        edge.src = src_id
        edge.dst = dst_id
        edge.type = 1
        edge.name = 'classmate'
        edge.ranking = 100
        edge.props = dict()
        for i in range(0, 5):
            edge.props['prop{}'.format(i)] = i
        return edge

    @classmethod
    def get_path_value(self, start_id, steps=5):
        path = ttypes.Path()
        path.src = self.get_vertex_value(start_id)
        path.steps = list()
        for i in range(0, steps):
            step = ttypes.Step()
            step.dst = self.get_vertex_value('vertex{}'.format(i))
            step.type = 1 if i % 2 == 0 else -1
            step.name = 'classmate'
            step.ranking = 100
            step.props = dict()
            for i in range(0, 5):
                step.props['prop{}'.format(i)] = i
            path.steps.append(step)
        return path

    @classmethod
    def get_result_set(self):
        resp = graphTtype.ExecutionResponse()
        resp.error_code = graphTtype.ErrorCode.E_BAD_PERMISSION
        resp.error_msg = "Permission"
        resp.comment = "Permission"
        resp.space_name = "test"
        resp.latency_in_us = 100
        data_set = ttypes.DataSet()
        data_set.column_names = ["col1", "col2", "col3", "col4", "col5", "col6", "col7"]
        row = ttypes.Row()
        value1 = ttypes.Value()
        value1.set_bVal(False)
        row.values = []
        row.values.append(value1)
        value2 = ttypes.Value()
        value2.set_iVal(100)
        row.values.append(value2)
        value3 = ttypes.Value()
        value3.set_fVal(10.01)
        row.values.append(value3)
        value4 = ttypes.Value()
        value4.set_sVal("hello world")
        row.values.append(value4)
        value5 = ttypes.Value()
        value5.set_vVal(self.get_vertex_value("Tom"))
        row.values.append(value5)
        value6 = ttypes.Value()
        value6.set_eVal(self.get_edge_value("Tom", "Lily"))
        row.values.append(value6)
        value7 = ttypes.Value()
        value7.set_pVal(self.get_path_value("Tom", 3))
        row.values.append(value7)
        data_set.rows = []
        data_set.rows.append(row)
        resp.data = data_set
        return ResultSet(resp)


class TesValueWrapper(TestBaseCase):
    def test_as_bool(self):
        value = ttypes.Value()
        value.set_bVal(False)
        value_wrapper = ValueWrapper(value)
        assert value_wrapper.is_bool()

        node = value_wrapper.as_bool()
        assert isinstance(node, bool)

    def test_as_int(self):
        value = ttypes.Value()
        value.set_iVal(100)
        value_wrapper = ValueWrapper(value)
        assert value_wrapper.is_int()

        node = value_wrapper.as_int()
        assert isinstance(node, int)

    def test_as_double(self):
        value = ttypes.Value()
        value.set_fVal(10.10)
        value_wrapper = ValueWrapper(value)
        assert value_wrapper.is_double()

        node = value_wrapper.as_double()
        assert isinstance(node, float)

    def test_as_string(self):
        value = ttypes.Value()
        value.set_sVal('Tom')
        value_wrapper = ValueWrapper(value)
        assert value_wrapper.is_string()

        strVal = value_wrapper.as_string()
        assert isinstance(strVal, str)

    def test_as_node(self):
        value = ttypes.Value()
        value.set_vVal(self.get_vertex_value('Tom'))
        value_wrapper = ValueWrapper(value)
        assert value_wrapper.is_vertex()

        node = value_wrapper.as_node()
        assert isinstance(node, Node)

    def test_as_relationship(self):
        value = ttypes.Value()
        value.set_eVal(self.get_edge_value('Tom', 'Lily'))
        value_wrapper = ValueWrapper(value)
        assert value_wrapper.is_edge()

        relationship = value_wrapper.as_relationship()
        assert isinstance(relationship, Relationship)

    def test_convert_path(self):
        value = ttypes.Value()
        value.set_pVal(self.get_path_value('Tom'))
        vaue_wrapper = ValueWrapper(value)
        assert vaue_wrapper.is_path()

        node = vaue_wrapper.as_path()
        assert isinstance(node, Path)


class TestNode(TestBaseCase):
    def test_node_api(self):
        node = Node(self.get_vertex_value('Tom'))
        assert 'Tom' == node.get_id()

        assert node.has_tag('tag2')

        print(node.prop_names('tag2'))
        assert ['prop0', 'prop1', 'prop2', 'prop3', 'prop4'] == node.prop_names('tag2')

        assert [0, 1, 2, 3, 4] == node.prop_values('tag2')

        assert ['tag0', 'tag1', 'tag2'] == node.tags()

        assert {'prop0': 0, 'prop1': 1, 'prop2': 2, 'prop3': 3, 'prop4': 4} == node.propertys('tag2')

        expect_str = "{Node}([Tom]:{" \
                     "{tag_name: tag0, props: {'prop0': 0, 'prop1': 1, 'prop2': 2, 'prop3': 3, 'prop4': 4}}," \
                     "{tag_name: tag1, props: {'prop0': 0, 'prop1': 1, 'prop2': 2, 'prop3': 3, 'prop4': 4}}," \
                     "{tag_name: tag2, props: {'prop0': 0, 'prop1': 1, 'prop2': 2, 'prop3': 3, 'prop4': 4}}})"
        assert str(node) == expect_str


class TestRelationship(TestBaseCase):
    def test_relationship_api(self):
        relationship = Relationship(self.get_edge_value('Tom', 'Lily'))

        assert 'Tom' == relationship.start_vertex_id()

        assert 'Lily' == relationship.end_vertex_id()

        assert 100 == relationship.ranking()

        assert 100 == relationship.ranking()

        assert 'classmate' == relationship.edge_name()

        assert ['prop0', 'prop1', 'prop2', 'prop3', 'prop4'] == relationship.keys()

        assert {'prop0': 0, 'prop1': 1, 'prop2': 2, 'prop3': 3, 'prop4': 4} == relationship.propertys()

        assert str(relationship) == "Relationship(" \
                                    "[Tom-[classmate(1)]->Lily@100]:" \
                                    "{'prop0': 0, 'prop1': 1, 'prop2': 2, 'prop3': 3, 'prop4': 4})"


class TestPath(TestBaseCase):
    def test_path_api(self):
        path = Path(self.get_path_value('Tom'))
        assert Node(self.get_vertex_value('Tom')) == path.start_node()

        assert 5 == path.length()

        assert path.contain_node(Node(self.get_vertex_value('vertex3')))

        assert path.contain_relationship(Relationship(self.get_edge_value('vertex3', 'vertex2')))

        nodes = list()
        nodes.append(path.start_node())
        for i in range(0, 5):
            nodes.append(Node(self.get_vertex_value('vertex'.format(i))))

        relationships = list()
        relationships.append(Relationship(self.get_edge_value('Tom', 'vertex0')))
        for i in range(0, 4):
            if i % 2 == 0:
                relationships.append(Relationship(self.get_edge_value('vertex{}'.format(i + 1), 'vertex{}'.format(i))))
            else:
                relationships.append(Relationship(self.get_edge_value('vertex{}'.format(i), 'vertex{}'.format(i + 1))))

        assert relationships == path.relationships()

        expect_str = "Path(" \
                     "segments: [" \
                     "Segment(" \
                     "start_node: {Node}([Tom]:{" \
                     "{tag_name: tag0, props: {'prop0': 0, 'prop1': 1, 'prop2': 2, 'prop3': 3, 'prop4': 4}}," \
                     "{tag_name: tag1, props: {'prop0': 0, 'prop1': 1, 'prop2': 2, 'prop3': 3, 'prop4': 4}}," \
                     "{tag_name: tag2, props: {'prop0': 0, 'prop1': 1, 'prop2': 2, 'prop3': 3, 'prop4': 4}}}), " \
                     "relations: Relationship([" \
                     "Tom-[classmate(1)]->vertex0@100]:" \
                     "{'prop0': 0, 'prop1': 1, 'prop2': 2, 'prop3': 3, 'prop4': 4}), " \
                     "end_node: {Node}([vertex0]:{" \
                     "{tag_name: tag0, props: {'prop0': 0, 'prop1': 1, 'prop2': 2, 'prop3': 3, 'prop4': 4}}," \
                     "{tag_name: tag1, props: {'prop0': 0, 'prop1': 1, 'prop2': 2, 'prop3': 3, 'prop4': 4}}," \
                     "{tag_name: tag2, props: {'prop0': 0, 'prop1': 1, 'prop2': 2, 'prop3': 3, 'prop4': 4}}})), " \
                     "Segment(" \
                     "start_node: {Node}([vertex1]:{" \
                     "{tag_name: tag0, props: {'prop0': 0, 'prop1': 1, 'prop2': 2, 'prop3': 3, 'prop4': 4}}," \
                     "{tag_name: tag1, props: {'prop0': 0, 'prop1': 1, 'prop2': 2, 'prop3': 3, 'prop4': 4}}," \
                     "{tag_name: tag2, props: {'prop0': 0, 'prop1': 1, 'prop2': 2, 'prop3': 3, 'prop4': 4}}}), " \
                     "relations: Relationship([" \
                     "vertex1-[classmate(1)]->vertex0@100]:" \
                     "{'prop0': 0, 'prop1': 1, 'prop2': 2, 'prop3': 3, 'prop4': 4}), " \
                     "end_node: {Node}([vertex0]:{" \
                     "{tag_name: tag0, props: {'prop0': 0, 'prop1': 1, 'prop2': 2, 'prop3': 3, 'prop4': 4}}," \
                     "{tag_name: tag1, props: {'prop0': 0, 'prop1': 1, 'prop2': 2, 'prop3': 3, 'prop4': 4}}," \
                     "{tag_name: tag2, props: {'prop0': 0, 'prop1': 1, 'prop2': 2, 'prop3': 3, 'prop4': 4}}}))])"

        print(Path(self.get_path_value('Tom', 2)))
        assert str(Path(self.get_path_value('Tom', 2))) == expect_str


class TestResultset(TestBaseCase):
    def test_all_interface(self):
        result = self.get_result_set()
        assert result.space_name() == "test"
        assert result.comment() == "Permission"
        assert result.error_msg() == "Permission"
        assert result.error_code() == graphTtype.ErrorCode.E_BAD_PERMISSION
        assert not result.is_succeeded()
        assert result.keys() == ["col1", "col2", "col3", "col4", "col5", "col6", "col7"]
        assert result.col_size() == 7
        assert result.row_size() == 1
        assert len(result.column_values("col6")) == 1
        assert len(result.row_values(0)) == 7
        assert len(result.rows()) == 1
        print(result.get_row_types())
        assert isinstance(result.get_row_types(), list)
        assert result.get_row_types() == [ttypes.Value.BVAL,
                                          ttypes.Value.IVAL,
                                          ttypes.Value.FVAL,
                                          ttypes.Value.SVAL,
                                          ttypes.Value.VVAL,
                                          ttypes.Value.EVAL,
                                          ttypes.Value.PVAL]
