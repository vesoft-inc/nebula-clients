/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.client.graph.data;

import com.vesoft.nebula.Path;
import com.vesoft.nebula.Value;
import com.vesoft.nebula.client.graph.exception.InvalidValueException;

public class ValueWrapper {
    public static class NullType {
        public static final int __NULL__ = 0;
        public static final int NaN = 1;
        public static final int BAD_DATA = 2;
        public static final int BAD_TYPE = 3;
        public static final int ERR_OVERFLOW = 4;
        public static final int UNKNOWN_PROP = 5;
        public static final int DIV_BY_ZERO = 6;
        public static final int OUT_OF_RANGE = 7;
        int nullType;

        public NullType(int nullType) {
            this.nullType = nullType;
        }

        public int getNullType() {
            return nullType;
        }
    }

    private Value value;

    public ValueWrapper(Value value) {
        this.value = value;
    }

    public Value getValue() {
        return value;
    }

    public int getType() {
        return value.getSetField();
    }

    private String descType() {
        switch (value.getSetField()) {
            case Value.NVAL:
                return "NULL";
            case Value.BVAL:
                return "BOOLEAN";
            case Value.IVAL:
                return "INT";
            case Value.FVAL:
                return "FLOAT";
            case Value.SVAL:
                return "STRING";
            case Value.DVAL:
                return "DATE";
            case Value.TVAL:
                return "TIME";
            case Value.DTVAL:
                return "DATETIME";
            case Value.VVAL:
                return "VERTEX";
            case Value.EVAL:
                return "EDGE";
            case Value.PVAL:
                return "PATH";
            case Value.LVAL:
                return "LIST";
            case Value.MVAL:
                return "MAP";
            case Value.UVAL:
                return "SET";
            case Value.GVAL:
                return "DATASET";
            default:
                throw new IllegalArgumentException("Unknown field id " + value.getSetField());
        }
    }

    public NullType asNull() throws RuntimeException {
        if (value.getSetField() == Value.NVAL) {
            return (NullType)(value.getFieldValue());
        } else {
            throw new InvalidValueException(
                    "Cannot get field nullType because value's type is " + descType());
        }
    }

    public int asInt() throws RuntimeException {
        if (value.getSetField() == Value.IVAL) {
            return (int)(value.getFieldValue());
        } else {
            throw new InvalidValueException(
                    "Cannot get field long because value's type is " + descType());
        }
    }

    public long asLong() throws RuntimeException {
        if (value.getSetField() == Value.IVAL) {
            return (long)(value.getFieldValue());
        } else {
            throw new InvalidValueException(
                    "Cannot get field long because value's type is " + descType());
        }
    }

    public boolean asBoolean() throws RuntimeException {
        if (value.getSetField() == Value.BVAL) {
            return (boolean)(value.getFieldValue());
        }
        throw new InvalidValueException(
                "Cannot get field boolean because value's type is " + descType());
    }

    public String asString() throws RuntimeException {
        if (value.getSetField() == Value.SVAL) {
            return new String((byte[])value.getFieldValue());
        }
        throw new InvalidValueException(
                "Cannot get field string because value's type is " + descType());
    }

    public double asDouble() throws RuntimeException {
        if (value.getSetField() == Value.SVAL) {
            return (double)value.getFieldValue();
        }
        throw new InvalidValueException(
                "Cannot get field double because value's type is " + descType());
    }

    public float asFloat() throws RuntimeException {
        if (value.getSetField() == Value.FVAL) {
            return (float)value.getFieldValue();
        }
        throw new InvalidValueException(
                "Cannot get field float because value's type is " + descType());
    }

    public Node asNode() throws InvalidValueException {
        if (value.getSetField() == Value.VVAL) {
            return new Node(value.getVVal());
        }
        throw new InvalidValueException(
                "Cannot get field Node because value's type is " + descType());
    }

    public Relationship asRelationShip() {
        if (value.getSetField() == Value.EVAL) {
            return new Relationship(value.getEVal());
        }
        throw new InvalidValueException(
                "Cannot get field Relationship because value's type is " + descType());
    }

    public PathWrapper asPath() throws InvalidValueException {
        if (value.getSetField() == Value.PVAL) {
            return new PathWrapper(value.getPVal());
        }
        throw new InvalidValueException(
                "Cannot get field PathWrapper because value's type is " + descType());
    }
}
