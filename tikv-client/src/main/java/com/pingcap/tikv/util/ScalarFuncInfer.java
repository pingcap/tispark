/*
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv.util;

import com.google.common.collect.ImmutableMap;
import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tidb.tipb.ScalarFuncSig;
import com.pingcap.tikv.types.DataTypeFactory;

import java.util.Map;

import static com.pingcap.tidb.tipb.ExprType.*;
import static com.pingcap.tidb.tipb.ScalarFuncSig.*;
import static com.pingcap.tikv.types.Types.*;
import static java.util.Objects.requireNonNull;

/**
 * The ScalarFunction Signature inferrer.
 * <p>
 * Used to infer a target signature for the given DataType and ExprType
 */
public class ScalarFuncInfer {
  private static final Map<ExprType, ScalarFuncSig> INTEGER_SCALAR_SIG_MAP =
      ImmutableMap.<ExprType, ScalarFuncSig>builder()
          .put(Case, CaseWhenInt)
          .put(Coalesce, CoalesceInt)
          .put(EQ, EQInt)
          .put(GE, GEInt)
          .put(GT, GTInt)
          .put(If, IfInt)
          .put(IfNull, IfNullInt)
          .put(In, InInt)
          .put(IsNull, IntIsNull)
          .put(IsTruth, IntIsTrue)
          .put(LE, LEInt)
          .put(LT, LTInt)
          .put(Minus, MinusInt)
          .put(Mul, MultiplyInt)
          .put(NE, NEInt)
          .put(NullEQ, NullEQInt)
          .put(Plus, PlusInt)
          .build();

  private static final Map<ExprType, ScalarFuncSig> DECIMAL_SCALAR_SIG_MAP =
      ImmutableMap.<ExprType, ScalarFuncSig>builder()
          .put(Case, CaseWhenDecimal)
          .put(Coalesce, CoalesceDecimal)
          .put(EQ, EQDecimal)
          .put(GE, GEDecimal)
          .put(GT, GTDecimal)
          .put(If, IfDecimal)
          .put(IfNull, IfNullDecimal)
          .put(In, InDecimal)
          .put(IsNull, DecimalIsNull)
          .put(IsTruth, DecimalIsTrue)
          .put(LE, LEDecimal)
          .put(LT, LTDecimal)
          .put(Minus, MinusDecimal)
          .put(Mul, MultiplyDecimal)
          .put(Div, DivideDecimal)
          .put(NE, NEDecimal)
          .put(NullEQ, NullEQDecimal)
          .put(Plus, PlusDecimal)
          .build();

  private static final Map<ExprType, ScalarFuncSig> REAL_SCALAR_SIG_MAP =
      ImmutableMap.<ExprType, ScalarFuncSig>builder()
          .put(Case, CaseWhenReal)
          .put(Coalesce, CoalesceReal)
          .put(EQ, EQReal)
          .put(GE, GEReal)
          .put(GT, GTReal)
          .put(If, IfReal)
          .put(IfNull, IfNullReal)
          .put(In, InReal)
          .put(IsNull, RealIsNull)
          .put(IsTruth, RealIsTrue)
          .put(LE, LEReal)
          .put(LT, LTReal)
          .put(Minus, MinusReal)
          .put(Mul, MultiplyReal)
          .put(Div, DivideReal)
          .put(NE, NEReal)
          .put(NullEQ, NullEQReal)
          .put(Plus, PlusReal)
          .build();

  private static final Map<ExprType, ScalarFuncSig> DURATION_SCALAR_SIG_MAP =
      ImmutableMap.<ExprType, ScalarFuncSig>builder()
          .put(Case, CaseWhenDuration)
          .put(Coalesce, CoalesceDuration)
          .put(EQ, EQDuration)
          .put(GE, GEDuration)
          .put(GT, GTDuration)
          .put(If, IfDuration)
          .put(IfNull, IfNullDuration)
          .put(In, InDuration)
          .put(IsNull, DurationIsNull)
          .put(LE, LEDuration)
          .put(LT, LTDuration)
          .put(NE, NEDuration)
          .put(NullEQ, NullEQDuration)
          .build();

  private static final Map<ExprType, ScalarFuncSig> TIME_SCALAR_SIG_MAP =
      ImmutableMap.<ExprType, ScalarFuncSig>builder()
          .put(Case, CaseWhenTime)
          .put(Coalesce, CoalesceTime)
          .put(EQ, EQTime)
          .put(GE, GETime)
          .put(GT, GTTime)
          .put(If, IfTime)
          .put(IfNull, IfNullTime)
          .put(In, InTime)
          .put(IsNull, TimeIsNull)
          .put(LE, LETime)
          .put(LT, LTTime)
          .put(NE, NETime)
          .put(NullEQ, NullEQTime)
          .build();

  private static final Map<ExprType, ScalarFuncSig> STRING_SCALAR_SIG_MAP =
      ImmutableMap.<ExprType, ScalarFuncSig>builder()
          .put(Case, CaseWhenString)
          .put(Coalesce, CoalesceString)
          .put(EQ, EQString)
          .put(GE, GEString)
          .put(GT, GTString)
          .put(If, IfString)
          .put(IfNull, IfNullString)
          .put(In, InString)
          .put(IsNull, StringIsNull)
          .put(LE, LEString)
          .put(LT, LTString)
          .put(NE, NEString)
          .put(NullEQ, NullEQString)
          .build();


  private static final Map<Integer, Map<ExprType, ScalarFuncSig>> SCALAR_SIG_MAP =
      ImmutableMap.<Integer, Map<ExprType, ScalarFuncSig>>builder()
          .put(TYPE_TINY, INTEGER_SCALAR_SIG_MAP)
          .put(TYPE_SHORT, INTEGER_SCALAR_SIG_MAP)
          .put(TYPE_LONG, INTEGER_SCALAR_SIG_MAP)
          .put(TYPE_INT24, INTEGER_SCALAR_SIG_MAP)
          .put(TYPE_LONG_LONG, INTEGER_SCALAR_SIG_MAP)
          .put(TYPE_YEAR, INTEGER_SCALAR_SIG_MAP)
          .put(TYPE_BIT, INTEGER_SCALAR_SIG_MAP)
          .put(TYPE_NEW_DECIMAL, DECIMAL_SCALAR_SIG_MAP)
          .put(TYPE_FLOAT, REAL_SCALAR_SIG_MAP)
          .put(TYPE_DOUBLE, REAL_SCALAR_SIG_MAP)
          .put(TYPE_DURATION, DURATION_SCALAR_SIG_MAP)
          .put(TYPE_DATETIME, TIME_SCALAR_SIG_MAP)
          .put(TYPE_TIMESTAMP, TIME_SCALAR_SIG_MAP)
          .put(TYPE_NEW_DATE, TIME_SCALAR_SIG_MAP)
          .put(TYPE_DATE, TIME_SCALAR_SIG_MAP)
          .put(TYPE_VARCHAR, STRING_SCALAR_SIG_MAP)
          .put(TYPE_JSON, STRING_SCALAR_SIG_MAP)
          .put(TYPE_ENUM, STRING_SCALAR_SIG_MAP)
          .put(TYPE_SET, STRING_SCALAR_SIG_MAP)
          .put(TYPE_TINY_BLOB, STRING_SCALAR_SIG_MAP)
          .put(TYPE_MEDIUM_BLOB, STRING_SCALAR_SIG_MAP)
          .put(TYPE_LONG_BLOB, STRING_SCALAR_SIG_MAP)
          .put(TYPE_BLOB, STRING_SCALAR_SIG_MAP)
          .put(TYPE_VAR_STRING, STRING_SCALAR_SIG_MAP)
          .put(TYPE_STRING, STRING_SCALAR_SIG_MAP)
          .put(TYPE_GEOMETRY, STRING_SCALAR_SIG_MAP)
          .build();

  /**
   * Scalar func sig inferrer.
   *
   * @param tp       the data type code
   * @param exprType the expression type
   * @return the scalar func sig
   */
  public static ScalarFuncSig of(int tp, ExprType exprType) {
    return requireNonNull(
        requireNonNull(
            SCALAR_SIG_MAP.get(tp),
            "DataType " + DataTypeFactory.of(tp) + " not supported yet."
        ).get(exprType),
        "ExprType " + exprType + " not supported yet."
    );
  }
}
