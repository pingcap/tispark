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

import static com.pingcap.tidb.tipb.ExprType.Case;
import static com.pingcap.tidb.tipb.ExprType.Coalesce;
import static com.pingcap.tidb.tipb.ExprType.Div;
import static com.pingcap.tidb.tipb.ExprType.EQ;
import static com.pingcap.tidb.tipb.ExprType.GE;
import static com.pingcap.tidb.tipb.ExprType.GT;
import static com.pingcap.tidb.tipb.ExprType.If;
import static com.pingcap.tidb.tipb.ExprType.IfNull;
import static com.pingcap.tidb.tipb.ExprType.In;
import static com.pingcap.tidb.tipb.ExprType.IsNull;
import static com.pingcap.tidb.tipb.ExprType.IsTruth;
import static com.pingcap.tidb.tipb.ExprType.LE;
import static com.pingcap.tidb.tipb.ExprType.LT;
import static com.pingcap.tidb.tipb.ExprType.Minus;
import static com.pingcap.tidb.tipb.ExprType.Mul;
import static com.pingcap.tidb.tipb.ExprType.NE;
import static com.pingcap.tidb.tipb.ExprType.NullEQ;
import static com.pingcap.tidb.tipb.ExprType.Plus;
import static com.pingcap.tidb.tipb.ScalarFuncSig.CaseWhenDecimal;
import static com.pingcap.tidb.tipb.ScalarFuncSig.CaseWhenDuration;
import static com.pingcap.tidb.tipb.ScalarFuncSig.CaseWhenInt;
import static com.pingcap.tidb.tipb.ScalarFuncSig.CaseWhenReal;
import static com.pingcap.tidb.tipb.ScalarFuncSig.CaseWhenString;
import static com.pingcap.tidb.tipb.ScalarFuncSig.CaseWhenTime;
import static com.pingcap.tidb.tipb.ScalarFuncSig.CoalesceDecimal;
import static com.pingcap.tidb.tipb.ScalarFuncSig.CoalesceDuration;
import static com.pingcap.tidb.tipb.ScalarFuncSig.CoalesceInt;
import static com.pingcap.tidb.tipb.ScalarFuncSig.CoalesceReal;
import static com.pingcap.tidb.tipb.ScalarFuncSig.CoalesceString;
import static com.pingcap.tidb.tipb.ScalarFuncSig.CoalesceTime;
import static com.pingcap.tidb.tipb.ScalarFuncSig.DecimalIsNull;
import static com.pingcap.tidb.tipb.ScalarFuncSig.DecimalIsTrue;
import static com.pingcap.tidb.tipb.ScalarFuncSig.DivideDecimal;
import static com.pingcap.tidb.tipb.ScalarFuncSig.DivideReal;
import static com.pingcap.tidb.tipb.ScalarFuncSig.DurationIsNull;
import static com.pingcap.tidb.tipb.ScalarFuncSig.EQDecimal;
import static com.pingcap.tidb.tipb.ScalarFuncSig.EQDuration;
import static com.pingcap.tidb.tipb.ScalarFuncSig.EQInt;
import static com.pingcap.tidb.tipb.ScalarFuncSig.EQReal;
import static com.pingcap.tidb.tipb.ScalarFuncSig.EQString;
import static com.pingcap.tidb.tipb.ScalarFuncSig.EQTime;
import static com.pingcap.tidb.tipb.ScalarFuncSig.GEDecimal;
import static com.pingcap.tidb.tipb.ScalarFuncSig.GEDuration;
import static com.pingcap.tidb.tipb.ScalarFuncSig.GEInt;
import static com.pingcap.tidb.tipb.ScalarFuncSig.GEReal;
import static com.pingcap.tidb.tipb.ScalarFuncSig.GEString;
import static com.pingcap.tidb.tipb.ScalarFuncSig.GETime;
import static com.pingcap.tidb.tipb.ScalarFuncSig.GTDecimal;
import static com.pingcap.tidb.tipb.ScalarFuncSig.GTDuration;
import static com.pingcap.tidb.tipb.ScalarFuncSig.GTInt;
import static com.pingcap.tidb.tipb.ScalarFuncSig.GTReal;
import static com.pingcap.tidb.tipb.ScalarFuncSig.GTString;
import static com.pingcap.tidb.tipb.ScalarFuncSig.GTTime;
import static com.pingcap.tidb.tipb.ScalarFuncSig.IfDecimal;
import static com.pingcap.tidb.tipb.ScalarFuncSig.IfDuration;
import static com.pingcap.tidb.tipb.ScalarFuncSig.IfInt;
import static com.pingcap.tidb.tipb.ScalarFuncSig.IfNullDecimal;
import static com.pingcap.tidb.tipb.ScalarFuncSig.IfNullDuration;
import static com.pingcap.tidb.tipb.ScalarFuncSig.IfNullInt;
import static com.pingcap.tidb.tipb.ScalarFuncSig.IfNullReal;
import static com.pingcap.tidb.tipb.ScalarFuncSig.IfNullString;
import static com.pingcap.tidb.tipb.ScalarFuncSig.IfNullTime;
import static com.pingcap.tidb.tipb.ScalarFuncSig.IfReal;
import static com.pingcap.tidb.tipb.ScalarFuncSig.IfString;
import static com.pingcap.tidb.tipb.ScalarFuncSig.IfTime;
import static com.pingcap.tidb.tipb.ScalarFuncSig.InDecimal;
import static com.pingcap.tidb.tipb.ScalarFuncSig.InDuration;
import static com.pingcap.tidb.tipb.ScalarFuncSig.InInt;
import static com.pingcap.tidb.tipb.ScalarFuncSig.InReal;
import static com.pingcap.tidb.tipb.ScalarFuncSig.InString;
import static com.pingcap.tidb.tipb.ScalarFuncSig.InTime;
import static com.pingcap.tidb.tipb.ScalarFuncSig.IntIsNull;
import static com.pingcap.tidb.tipb.ScalarFuncSig.IntIsTrue;
import static com.pingcap.tidb.tipb.ScalarFuncSig.LEDecimal;
import static com.pingcap.tidb.tipb.ScalarFuncSig.LEDuration;
import static com.pingcap.tidb.tipb.ScalarFuncSig.LEInt;
import static com.pingcap.tidb.tipb.ScalarFuncSig.LEReal;
import static com.pingcap.tidb.tipb.ScalarFuncSig.LEString;
import static com.pingcap.tidb.tipb.ScalarFuncSig.LETime;
import static com.pingcap.tidb.tipb.ScalarFuncSig.LTDecimal;
import static com.pingcap.tidb.tipb.ScalarFuncSig.LTDuration;
import static com.pingcap.tidb.tipb.ScalarFuncSig.LTInt;
import static com.pingcap.tidb.tipb.ScalarFuncSig.LTReal;
import static com.pingcap.tidb.tipb.ScalarFuncSig.LTString;
import static com.pingcap.tidb.tipb.ScalarFuncSig.LTTime;
import static com.pingcap.tidb.tipb.ScalarFuncSig.MinusDecimal;
import static com.pingcap.tidb.tipb.ScalarFuncSig.MinusInt;
import static com.pingcap.tidb.tipb.ScalarFuncSig.MinusReal;
import static com.pingcap.tidb.tipb.ScalarFuncSig.MultiplyDecimal;
import static com.pingcap.tidb.tipb.ScalarFuncSig.MultiplyInt;
import static com.pingcap.tidb.tipb.ScalarFuncSig.MultiplyReal;
import static com.pingcap.tidb.tipb.ScalarFuncSig.NEDecimal;
import static com.pingcap.tidb.tipb.ScalarFuncSig.NEDuration;
import static com.pingcap.tidb.tipb.ScalarFuncSig.NEInt;
import static com.pingcap.tidb.tipb.ScalarFuncSig.NEReal;
import static com.pingcap.tidb.tipb.ScalarFuncSig.NEString;
import static com.pingcap.tidb.tipb.ScalarFuncSig.NETime;
import static com.pingcap.tidb.tipb.ScalarFuncSig.NullEQDecimal;
import static com.pingcap.tidb.tipb.ScalarFuncSig.NullEQDuration;
import static com.pingcap.tidb.tipb.ScalarFuncSig.NullEQInt;
import static com.pingcap.tidb.tipb.ScalarFuncSig.NullEQReal;
import static com.pingcap.tidb.tipb.ScalarFuncSig.NullEQString;
import static com.pingcap.tidb.tipb.ScalarFuncSig.NullEQTime;
import static com.pingcap.tidb.tipb.ScalarFuncSig.PlusDecimal;
import static com.pingcap.tidb.tipb.ScalarFuncSig.PlusInt;
import static com.pingcap.tidb.tipb.ScalarFuncSig.PlusReal;
import static com.pingcap.tidb.tipb.ScalarFuncSig.RealIsNull;
import static com.pingcap.tidb.tipb.ScalarFuncSig.RealIsTrue;
import static com.pingcap.tidb.tipb.ScalarFuncSig.StringIsNull;
import static com.pingcap.tidb.tipb.ScalarFuncSig.TimeIsNull;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tidb.tipb.ScalarFuncSig;
import com.pingcap.tikv.types.DataTypeFactory;
import com.pingcap.tikv.types.MySQLType;
import java.util.Map;

/**
 * The ScalarFunction Signature inferrer.
 * <p>
 * Used to infer a target signature for the given DataType and ExprType
 */
public class ScalarFuncInferrer {
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


  private static final Map<MySQLType, Map<ExprType, ScalarFuncSig>> SCALAR_SIG_MAP =
      ImmutableMap.<MySQLType, Map<ExprType, ScalarFuncSig>>builder()
          .put(MySQLType.TypeTiny, INTEGER_SCALAR_SIG_MAP)
          .put(MySQLType.TypeShort, INTEGER_SCALAR_SIG_MAP)
          .put(MySQLType.TypeLong, INTEGER_SCALAR_SIG_MAP)
          .put(MySQLType.TypeInt24, INTEGER_SCALAR_SIG_MAP)
          .put(MySQLType.TypeLonglong, INTEGER_SCALAR_SIG_MAP)
          .put(MySQLType.TypeYear, INTEGER_SCALAR_SIG_MAP)
          .put(MySQLType.TypeBit, INTEGER_SCALAR_SIG_MAP)
          .put(MySQLType.TypeNewDecimal, DECIMAL_SCALAR_SIG_MAP)
          .put(MySQLType.TypeFloat, REAL_SCALAR_SIG_MAP)
          .put(MySQLType.TypeDouble, REAL_SCALAR_SIG_MAP)
          .put(MySQLType.TypeDuration, DURATION_SCALAR_SIG_MAP)
          .put(MySQLType.TypeDatetime, TIME_SCALAR_SIG_MAP)
          .put(MySQLType.TypeTimestamp, TIME_SCALAR_SIG_MAP)
          .put(MySQLType.TypeNewDate, TIME_SCALAR_SIG_MAP)
          .put(MySQLType.TypeDate, TIME_SCALAR_SIG_MAP)
          .put(MySQLType.TypeVarchar, STRING_SCALAR_SIG_MAP)
          .put(MySQLType.TypeJSON, STRING_SCALAR_SIG_MAP)
          .put(MySQLType.TypeEnum, STRING_SCALAR_SIG_MAP)
          .put(MySQLType.TypeSet, STRING_SCALAR_SIG_MAP)
          .put(MySQLType.TypeTinyBlob, STRING_SCALAR_SIG_MAP)
          .put(MySQLType.TypeMediumBlob, STRING_SCALAR_SIG_MAP)
          .put(MySQLType.TypeLongBlob, STRING_SCALAR_SIG_MAP)
          .put(MySQLType.TypeBlob, STRING_SCALAR_SIG_MAP)
          .put(MySQLType.TypeVarString, STRING_SCALAR_SIG_MAP)
          .put(MySQLType.TypeString, STRING_SCALAR_SIG_MAP)
          .put(MySQLType.TypeGeometry, STRING_SCALAR_SIG_MAP)
          .build();

  /**
   * Scalar func sig inferrer.
   *
   * @param argumentType       the data type code
   * @param exprType the expression type
   * @return the scalar func sig
   */
  public static ScalarFuncSig of(MySQLType argumentType, ExprType exprType) {
    return requireNonNull(
        requireNonNull(
            SCALAR_SIG_MAP.get(argumentType),
            "DataType " + DataTypeFactory.of(argumentType) + " not supported yet."
        ).get(exprType),
        "ExprType " + exprType + " not supported yet."
    );
  }
}
