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
from unittest import TestCase
from nebula2.data_type.DataObject import (
    ConvertValue,
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


class TesConvertValue(TestBaseCase):

    def test_convert_node(self):
        value = ttypes.Value()
        value.set_vVal(self.get_vertex_value('Tom'))
        node = ConvertValue.convert(value)
        assert isinstance(node, Node)

        node = ConvertValue.as_node(value)
        assert isinstance(node, Node)

    def test_convert_relationship(self):
        value = ttypes.Value()
        value.set_eVal(self.get_edge_value('Tom', 'Lily'))
        node = ConvertValue.convert(value)
        assert isinstance(node, Relationship)

        node = ConvertValue.as_relationship(value)
        assert isinstance(node, Relationship)

    def test_convert_path(self):
        value = ttypes.Value()
        value.set_pVal(self.get_path_value('Tom'))
        node = ConvertValue.convert(value)
        assert isinstance(node, Path)

        node = ConvertValue.as_path(value)
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
