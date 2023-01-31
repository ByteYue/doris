// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ScalarFunction 'field'. This class is generated by GenerateFunction.
 */
public class Field extends ScalarFunction
        implements ExplicitlyCastableSignature, PropagateNullable {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(IntegerType.INSTANCE).varArgs(TinyIntType.INSTANCE),
            FunctionSignature.ret(IntegerType.INSTANCE).varArgs(SmallIntType.INSTANCE),
            FunctionSignature.ret(IntegerType.INSTANCE).varArgs(IntegerType.INSTANCE),
            FunctionSignature.ret(IntegerType.INSTANCE).varArgs(BigIntType.INSTANCE),
            FunctionSignature.ret(IntegerType.INSTANCE).varArgs(LargeIntType.INSTANCE),
            FunctionSignature.ret(IntegerType.INSTANCE).varArgs(FloatType.INSTANCE),
            FunctionSignature.ret(IntegerType.INSTANCE).varArgs(DoubleType.INSTANCE),
            FunctionSignature.ret(IntegerType.INSTANCE).varArgs(DecimalV2Type.SYSTEM_DEFAULT),
            FunctionSignature.ret(IntegerType.INSTANCE).varArgs(DateV2Type.INSTANCE),
            FunctionSignature.ret(IntegerType.INSTANCE).varArgs(DateTimeV2Type.SYSTEM_DEFAULT),
            FunctionSignature.ret(IntegerType.INSTANCE).varArgs(VarcharType.SYSTEM_DEFAULT),
            FunctionSignature.ret(IntegerType.INSTANCE).varArgs(StringType.INSTANCE)
    );

    /**
     * constructor with 1 or more arguments.
     */
    public Field(Expression arg, Expression... varArgs) {
        super("field", ExpressionUtils.mergeArguments(arg, varArgs));
        Preconditions.checkArgument(varArgs.length >= 1, "field function parameter size is less than 2");
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        for (int i = 1; i < children.size(); ++i) {
            if (!getArgument(i).isConstant()) {
                throw new AnalysisException(getName()
                        + " function except for the first argument, other parameter must be a constant.");
            }
        }
    }

    /**
     * withChildren.
     */
    @Override
    public Field withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() >= 1);
        return new Field(children.get(0),
                children.subList(1, children.size()).toArray(new Expression[0]));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitField(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }
}
