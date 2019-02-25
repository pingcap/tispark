// Generated from MySqlParser.g4 by ANTLR 4.7.1
package com.pingcap.tikv.parser;

import org.antlr.v4.runtime.tree.ParseTreeListener;

/** This interface defines a complete listener for a parse tree produced by {@link MySqlParser}. */
public interface MySqlParserListener extends ParseTreeListener {
  /**
   * Enter a parse tree produced by {@link MySqlParser#intervalType}.
   *
   * @param ctx the parse tree
   */
  void enterIntervalType(MySqlParser.IntervalTypeContext ctx);
  /**
   * Exit a parse tree produced by {@link MySqlParser#intervalType}.
   *
   * @param ctx the parse tree
   */
  void exitIntervalType(MySqlParser.IntervalTypeContext ctx);
  /**
   * Enter a parse tree produced by {@link MySqlParser#fullId}.
   *
   * @param ctx the parse tree
   */
  void enterFullId(MySqlParser.FullIdContext ctx);
  /**
   * Exit a parse tree produced by {@link MySqlParser#fullId}.
   *
   * @param ctx the parse tree
   */
  void exitFullId(MySqlParser.FullIdContext ctx);
  /**
   * Enter a parse tree produced by {@link MySqlParser#fullColumnName}.
   *
   * @param ctx the parse tree
   */
  void enterFullColumnName(MySqlParser.FullColumnNameContext ctx);
  /**
   * Exit a parse tree produced by {@link MySqlParser#fullColumnName}.
   *
   * @param ctx the parse tree
   */
  void exitFullColumnName(MySqlParser.FullColumnNameContext ctx);
  /**
   * Enter a parse tree produced by {@link MySqlParser#charsetName}.
   *
   * @param ctx the parse tree
   */
  void enterCharsetName(MySqlParser.CharsetNameContext ctx);
  /**
   * Exit a parse tree produced by {@link MySqlParser#charsetName}.
   *
   * @param ctx the parse tree
   */
  void exitCharsetName(MySqlParser.CharsetNameContext ctx);
  /**
   * Enter a parse tree produced by {@link MySqlParser#collationName}.
   *
   * @param ctx the parse tree
   */
  void enterCollationName(MySqlParser.CollationNameContext ctx);
  /**
   * Exit a parse tree produced by {@link MySqlParser#collationName}.
   *
   * @param ctx the parse tree
   */
  void exitCollationName(MySqlParser.CollationNameContext ctx);
  /**
   * Enter a parse tree produced by {@link MySqlParser#uid}.
   *
   * @param ctx the parse tree
   */
  void enterUid(MySqlParser.UidContext ctx);
  /**
   * Exit a parse tree produced by {@link MySqlParser#uid}.
   *
   * @param ctx the parse tree
   */
  void exitUid(MySqlParser.UidContext ctx);
  /**
   * Enter a parse tree produced by {@link MySqlParser#simpleId}.
   *
   * @param ctx the parse tree
   */
  void enterSimpleId(MySqlParser.SimpleIdContext ctx);
  /**
   * Exit a parse tree produced by {@link MySqlParser#simpleId}.
   *
   * @param ctx the parse tree
   */
  void exitSimpleId(MySqlParser.SimpleIdContext ctx);
  /**
   * Enter a parse tree produced by {@link MySqlParser#dottedId}.
   *
   * @param ctx the parse tree
   */
  void enterDottedId(MySqlParser.DottedIdContext ctx);
  /**
   * Exit a parse tree produced by {@link MySqlParser#dottedId}.
   *
   * @param ctx the parse tree
   */
  void exitDottedId(MySqlParser.DottedIdContext ctx);
  /**
   * Enter a parse tree produced by {@link MySqlParser#decimalLiteral}.
   *
   * @param ctx the parse tree
   */
  void enterDecimalLiteral(MySqlParser.DecimalLiteralContext ctx);
  /**
   * Exit a parse tree produced by {@link MySqlParser#decimalLiteral}.
   *
   * @param ctx the parse tree
   */
  void exitDecimalLiteral(MySqlParser.DecimalLiteralContext ctx);
  /**
   * Enter a parse tree produced by {@link MySqlParser#stringLiteral}.
   *
   * @param ctx the parse tree
   */
  void enterStringLiteral(MySqlParser.StringLiteralContext ctx);
  /**
   * Exit a parse tree produced by {@link MySqlParser#stringLiteral}.
   *
   * @param ctx the parse tree
   */
  void exitStringLiteral(MySqlParser.StringLiteralContext ctx);
  /**
   * Enter a parse tree produced by {@link MySqlParser#booleanLiteral}.
   *
   * @param ctx the parse tree
   */
  void enterBooleanLiteral(MySqlParser.BooleanLiteralContext ctx);
  /**
   * Exit a parse tree produced by {@link MySqlParser#booleanLiteral}.
   *
   * @param ctx the parse tree
   */
  void exitBooleanLiteral(MySqlParser.BooleanLiteralContext ctx);
  /**
   * Enter a parse tree produced by {@link MySqlParser#hexadecimalLiteral}.
   *
   * @param ctx the parse tree
   */
  void enterHexadecimalLiteral(MySqlParser.HexadecimalLiteralContext ctx);
  /**
   * Exit a parse tree produced by {@link MySqlParser#hexadecimalLiteral}.
   *
   * @param ctx the parse tree
   */
  void exitHexadecimalLiteral(MySqlParser.HexadecimalLiteralContext ctx);
  /**
   * Enter a parse tree produced by {@link MySqlParser#nullNotnull}.
   *
   * @param ctx the parse tree
   */
  void enterNullNotnull(MySqlParser.NullNotnullContext ctx);
  /**
   * Exit a parse tree produced by {@link MySqlParser#nullNotnull}.
   *
   * @param ctx the parse tree
   */
  void exitNullNotnull(MySqlParser.NullNotnullContext ctx);
  /**
   * Enter a parse tree produced by {@link MySqlParser#constant}.
   *
   * @param ctx the parse tree
   */
  void enterConstant(MySqlParser.ConstantContext ctx);
  /**
   * Exit a parse tree produced by {@link MySqlParser#constant}.
   *
   * @param ctx the parse tree
   */
  void exitConstant(MySqlParser.ConstantContext ctx);
  /**
   * Enter a parse tree produced by {@link MySqlParser#convertedDataType}.
   *
   * @param ctx the parse tree
   */
  void enterConvertedDataType(MySqlParser.ConvertedDataTypeContext ctx);
  /**
   * Exit a parse tree produced by {@link MySqlParser#convertedDataType}.
   *
   * @param ctx the parse tree
   */
  void exitConvertedDataType(MySqlParser.ConvertedDataTypeContext ctx);
  /**
   * Enter a parse tree produced by {@link MySqlParser#lengthOneDimension}.
   *
   * @param ctx the parse tree
   */
  void enterLengthOneDimension(MySqlParser.LengthOneDimensionContext ctx);
  /**
   * Exit a parse tree produced by {@link MySqlParser#lengthOneDimension}.
   *
   * @param ctx the parse tree
   */
  void exitLengthOneDimension(MySqlParser.LengthOneDimensionContext ctx);
  /**
   * Enter a parse tree produced by {@link MySqlParser#lengthTwoDimension}.
   *
   * @param ctx the parse tree
   */
  void enterLengthTwoDimension(MySqlParser.LengthTwoDimensionContext ctx);
  /**
   * Exit a parse tree produced by {@link MySqlParser#lengthTwoDimension}.
   *
   * @param ctx the parse tree
   */
  void exitLengthTwoDimension(MySqlParser.LengthTwoDimensionContext ctx);
  /**
   * Enter a parse tree produced by {@link MySqlParser#expressions}.
   *
   * @param ctx the parse tree
   */
  void enterExpressions(MySqlParser.ExpressionsContext ctx);
  /**
   * Exit a parse tree produced by {@link MySqlParser#expressions}.
   *
   * @param ctx the parse tree
   */
  void exitExpressions(MySqlParser.ExpressionsContext ctx);
  /**
   * Enter a parse tree produced by {@link MySqlParser#currentTimestamp}.
   *
   * @param ctx the parse tree
   */
  void enterCurrentTimestamp(MySqlParser.CurrentTimestampContext ctx);
  /**
   * Exit a parse tree produced by {@link MySqlParser#currentTimestamp}.
   *
   * @param ctx the parse tree
   */
  void exitCurrentTimestamp(MySqlParser.CurrentTimestampContext ctx);
  /**
   * Enter a parse tree produced by the {@code specificFunctionCall} labeled alternative in {@link
   * MySqlParser#functionCall}.
   *
   * @param ctx the parse tree
   */
  void enterSpecificFunctionCall(MySqlParser.SpecificFunctionCallContext ctx);
  /**
   * Exit a parse tree produced by the {@code specificFunctionCall} labeled alternative in {@link
   * MySqlParser#functionCall}.
   *
   * @param ctx the parse tree
   */
  void exitSpecificFunctionCall(MySqlParser.SpecificFunctionCallContext ctx);
  /**
   * Enter a parse tree produced by the {@code scalarFunctionCall} labeled alternative in {@link
   * MySqlParser#functionCall}.
   *
   * @param ctx the parse tree
   */
  void enterScalarFunctionCall(MySqlParser.ScalarFunctionCallContext ctx);
  /**
   * Exit a parse tree produced by the {@code scalarFunctionCall} labeled alternative in {@link
   * MySqlParser#functionCall}.
   *
   * @param ctx the parse tree
   */
  void exitScalarFunctionCall(MySqlParser.ScalarFunctionCallContext ctx);
  /**
   * Enter a parse tree produced by the {@code udfFunctionCall} labeled alternative in {@link
   * MySqlParser#functionCall}.
   *
   * @param ctx the parse tree
   */
  void enterUdfFunctionCall(MySqlParser.UdfFunctionCallContext ctx);
  /**
   * Exit a parse tree produced by the {@code udfFunctionCall} labeled alternative in {@link
   * MySqlParser#functionCall}.
   *
   * @param ctx the parse tree
   */
  void exitUdfFunctionCall(MySqlParser.UdfFunctionCallContext ctx);
  /**
   * Enter a parse tree produced by the {@code simpleFunctionCall} labeled alternative in {@link
   * MySqlParser#specificFunction}.
   *
   * @param ctx the parse tree
   */
  void enterSimpleFunctionCall(MySqlParser.SimpleFunctionCallContext ctx);
  /**
   * Exit a parse tree produced by the {@code simpleFunctionCall} labeled alternative in {@link
   * MySqlParser#specificFunction}.
   *
   * @param ctx the parse tree
   */
  void exitSimpleFunctionCall(MySqlParser.SimpleFunctionCallContext ctx);
  /**
   * Enter a parse tree produced by the {@code dataTypeFunctionCall} labeled alternative in {@link
   * MySqlParser#specificFunction}.
   *
   * @param ctx the parse tree
   */
  void enterDataTypeFunctionCall(MySqlParser.DataTypeFunctionCallContext ctx);
  /**
   * Exit a parse tree produced by the {@code dataTypeFunctionCall} labeled alternative in {@link
   * MySqlParser#specificFunction}.
   *
   * @param ctx the parse tree
   */
  void exitDataTypeFunctionCall(MySqlParser.DataTypeFunctionCallContext ctx);
  /**
   * Enter a parse tree produced by the {@code valuesFunctionCall} labeled alternative in {@link
   * MySqlParser#specificFunction}.
   *
   * @param ctx the parse tree
   */
  void enterValuesFunctionCall(MySqlParser.ValuesFunctionCallContext ctx);
  /**
   * Exit a parse tree produced by the {@code valuesFunctionCall} labeled alternative in {@link
   * MySqlParser#specificFunction}.
   *
   * @param ctx the parse tree
   */
  void exitValuesFunctionCall(MySqlParser.ValuesFunctionCallContext ctx);
  /**
   * Enter a parse tree produced by the {@code caseFunctionCall} labeled alternative in {@link
   * MySqlParser#specificFunction}.
   *
   * @param ctx the parse tree
   */
  void enterCaseFunctionCall(MySqlParser.CaseFunctionCallContext ctx);
  /**
   * Exit a parse tree produced by the {@code caseFunctionCall} labeled alternative in {@link
   * MySqlParser#specificFunction}.
   *
   * @param ctx the parse tree
   */
  void exitCaseFunctionCall(MySqlParser.CaseFunctionCallContext ctx);
  /**
   * Enter a parse tree produced by the {@code charFunctionCall} labeled alternative in {@link
   * MySqlParser#specificFunction}.
   *
   * @param ctx the parse tree
   */
  void enterCharFunctionCall(MySqlParser.CharFunctionCallContext ctx);
  /**
   * Exit a parse tree produced by the {@code charFunctionCall} labeled alternative in {@link
   * MySqlParser#specificFunction}.
   *
   * @param ctx the parse tree
   */
  void exitCharFunctionCall(MySqlParser.CharFunctionCallContext ctx);
  /**
   * Enter a parse tree produced by the {@code positionFunctionCall} labeled alternative in {@link
   * MySqlParser#specificFunction}.
   *
   * @param ctx the parse tree
   */
  void enterPositionFunctionCall(MySqlParser.PositionFunctionCallContext ctx);
  /**
   * Exit a parse tree produced by the {@code positionFunctionCall} labeled alternative in {@link
   * MySqlParser#specificFunction}.
   *
   * @param ctx the parse tree
   */
  void exitPositionFunctionCall(MySqlParser.PositionFunctionCallContext ctx);
  /**
   * Enter a parse tree produced by the {@code substrFunctionCall} labeled alternative in {@link
   * MySqlParser#specificFunction}.
   *
   * @param ctx the parse tree
   */
  void enterSubstrFunctionCall(MySqlParser.SubstrFunctionCallContext ctx);
  /**
   * Exit a parse tree produced by the {@code substrFunctionCall} labeled alternative in {@link
   * MySqlParser#specificFunction}.
   *
   * @param ctx the parse tree
   */
  void exitSubstrFunctionCall(MySqlParser.SubstrFunctionCallContext ctx);
  /**
   * Enter a parse tree produced by the {@code trimFunctionCall} labeled alternative in {@link
   * MySqlParser#specificFunction}.
   *
   * @param ctx the parse tree
   */
  void enterTrimFunctionCall(MySqlParser.TrimFunctionCallContext ctx);
  /**
   * Exit a parse tree produced by the {@code trimFunctionCall} labeled alternative in {@link
   * MySqlParser#specificFunction}.
   *
   * @param ctx the parse tree
   */
  void exitTrimFunctionCall(MySqlParser.TrimFunctionCallContext ctx);
  /**
   * Enter a parse tree produced by the {@code weightFunctionCall} labeled alternative in {@link
   * MySqlParser#specificFunction}.
   *
   * @param ctx the parse tree
   */
  void enterWeightFunctionCall(MySqlParser.WeightFunctionCallContext ctx);
  /**
   * Exit a parse tree produced by the {@code weightFunctionCall} labeled alternative in {@link
   * MySqlParser#specificFunction}.
   *
   * @param ctx the parse tree
   */
  void exitWeightFunctionCall(MySqlParser.WeightFunctionCallContext ctx);
  /**
   * Enter a parse tree produced by the {@code extractFunctionCall} labeled alternative in {@link
   * MySqlParser#specificFunction}.
   *
   * @param ctx the parse tree
   */
  void enterExtractFunctionCall(MySqlParser.ExtractFunctionCallContext ctx);
  /**
   * Exit a parse tree produced by the {@code extractFunctionCall} labeled alternative in {@link
   * MySqlParser#specificFunction}.
   *
   * @param ctx the parse tree
   */
  void exitExtractFunctionCall(MySqlParser.ExtractFunctionCallContext ctx);
  /**
   * Enter a parse tree produced by the {@code getFormatFunctionCall} labeled alternative in {@link
   * MySqlParser#specificFunction}.
   *
   * @param ctx the parse tree
   */
  void enterGetFormatFunctionCall(MySqlParser.GetFormatFunctionCallContext ctx);
  /**
   * Exit a parse tree produced by the {@code getFormatFunctionCall} labeled alternative in {@link
   * MySqlParser#specificFunction}.
   *
   * @param ctx the parse tree
   */
  void exitGetFormatFunctionCall(MySqlParser.GetFormatFunctionCallContext ctx);
  /**
   * Enter a parse tree produced by {@link MySqlParser#caseFuncAlternative}.
   *
   * @param ctx the parse tree
   */
  void enterCaseFuncAlternative(MySqlParser.CaseFuncAlternativeContext ctx);
  /**
   * Exit a parse tree produced by {@link MySqlParser#caseFuncAlternative}.
   *
   * @param ctx the parse tree
   */
  void exitCaseFuncAlternative(MySqlParser.CaseFuncAlternativeContext ctx);
  /**
   * Enter a parse tree produced by the {@code levelWeightList} labeled alternative in {@link
   * MySqlParser#levelsInWeightString}.
   *
   * @param ctx the parse tree
   */
  void enterLevelWeightList(MySqlParser.LevelWeightListContext ctx);
  /**
   * Exit a parse tree produced by the {@code levelWeightList} labeled alternative in {@link
   * MySqlParser#levelsInWeightString}.
   *
   * @param ctx the parse tree
   */
  void exitLevelWeightList(MySqlParser.LevelWeightListContext ctx);
  /**
   * Enter a parse tree produced by the {@code levelWeightRange} labeled alternative in {@link
   * MySqlParser#levelsInWeightString}.
   *
   * @param ctx the parse tree
   */
  void enterLevelWeightRange(MySqlParser.LevelWeightRangeContext ctx);
  /**
   * Exit a parse tree produced by the {@code levelWeightRange} labeled alternative in {@link
   * MySqlParser#levelsInWeightString}.
   *
   * @param ctx the parse tree
   */
  void exitLevelWeightRange(MySqlParser.LevelWeightRangeContext ctx);
  /**
   * Enter a parse tree produced by {@link MySqlParser#levelInWeightListElement}.
   *
   * @param ctx the parse tree
   */
  void enterLevelInWeightListElement(MySqlParser.LevelInWeightListElementContext ctx);
  /**
   * Exit a parse tree produced by {@link MySqlParser#levelInWeightListElement}.
   *
   * @param ctx the parse tree
   */
  void exitLevelInWeightListElement(MySqlParser.LevelInWeightListElementContext ctx);
  /**
   * Enter a parse tree produced by {@link MySqlParser#scalarFunctionName}.
   *
   * @param ctx the parse tree
   */
  void enterScalarFunctionName(MySqlParser.ScalarFunctionNameContext ctx);
  /**
   * Exit a parse tree produced by {@link MySqlParser#scalarFunctionName}.
   *
   * @param ctx the parse tree
   */
  void exitScalarFunctionName(MySqlParser.ScalarFunctionNameContext ctx);
  /**
   * Enter a parse tree produced by {@link MySqlParser#functionArgs}.
   *
   * @param ctx the parse tree
   */
  void enterFunctionArgs(MySqlParser.FunctionArgsContext ctx);
  /**
   * Exit a parse tree produced by {@link MySqlParser#functionArgs}.
   *
   * @param ctx the parse tree
   */
  void exitFunctionArgs(MySqlParser.FunctionArgsContext ctx);
  /**
   * Enter a parse tree produced by {@link MySqlParser#functionArg}.
   *
   * @param ctx the parse tree
   */
  void enterFunctionArg(MySqlParser.FunctionArgContext ctx);
  /**
   * Exit a parse tree produced by {@link MySqlParser#functionArg}.
   *
   * @param ctx the parse tree
   */
  void exitFunctionArg(MySqlParser.FunctionArgContext ctx);
  /**
   * Enter a parse tree produced by the {@code isExpression} labeled alternative in {@link
   * MySqlParser#expression}.
   *
   * @param ctx the parse tree
   */
  void enterIsExpression(MySqlParser.IsExpressionContext ctx);
  /**
   * Exit a parse tree produced by the {@code isExpression} labeled alternative in {@link
   * MySqlParser#expression}.
   *
   * @param ctx the parse tree
   */
  void exitIsExpression(MySqlParser.IsExpressionContext ctx);
  /**
   * Enter a parse tree produced by the {@code notExpression} labeled alternative in {@link
   * MySqlParser#expression}.
   *
   * @param ctx the parse tree
   */
  void enterNotExpression(MySqlParser.NotExpressionContext ctx);
  /**
   * Exit a parse tree produced by the {@code notExpression} labeled alternative in {@link
   * MySqlParser#expression}.
   *
   * @param ctx the parse tree
   */
  void exitNotExpression(MySqlParser.NotExpressionContext ctx);
  /**
   * Enter a parse tree produced by the {@code logicalExpression} labeled alternative in {@link
   * MySqlParser#expression}.
   *
   * @param ctx the parse tree
   */
  void enterLogicalExpression(MySqlParser.LogicalExpressionContext ctx);
  /**
   * Exit a parse tree produced by the {@code logicalExpression} labeled alternative in {@link
   * MySqlParser#expression}.
   *
   * @param ctx the parse tree
   */
  void exitLogicalExpression(MySqlParser.LogicalExpressionContext ctx);
  /**
   * Enter a parse tree produced by the {@code predicateExpression} labeled alternative in {@link
   * MySqlParser#expression}.
   *
   * @param ctx the parse tree
   */
  void enterPredicateExpression(MySqlParser.PredicateExpressionContext ctx);
  /**
   * Exit a parse tree produced by the {@code predicateExpression} labeled alternative in {@link
   * MySqlParser#expression}.
   *
   * @param ctx the parse tree
   */
  void exitPredicateExpression(MySqlParser.PredicateExpressionContext ctx);
  /**
   * Enter a parse tree produced by the {@code soundsLikePredicate} labeled alternative in {@link
   * MySqlParser#predicate}.
   *
   * @param ctx the parse tree
   */
  void enterSoundsLikePredicate(MySqlParser.SoundsLikePredicateContext ctx);
  /**
   * Exit a parse tree produced by the {@code soundsLikePredicate} labeled alternative in {@link
   * MySqlParser#predicate}.
   *
   * @param ctx the parse tree
   */
  void exitSoundsLikePredicate(MySqlParser.SoundsLikePredicateContext ctx);
  /**
   * Enter a parse tree produced by the {@code expressionAtomPredicate} labeled alternative in
   * {@link MySqlParser#predicate}.
   *
   * @param ctx the parse tree
   */
  void enterExpressionAtomPredicate(MySqlParser.ExpressionAtomPredicateContext ctx);
  /**
   * Exit a parse tree produced by the {@code expressionAtomPredicate} labeled alternative in {@link
   * MySqlParser#predicate}.
   *
   * @param ctx the parse tree
   */
  void exitExpressionAtomPredicate(MySqlParser.ExpressionAtomPredicateContext ctx);
  /**
   * Enter a parse tree produced by the {@code binaryComparisonPredicate} labeled alternative in
   * {@link MySqlParser#predicate}.
   *
   * @param ctx the parse tree
   */
  void enterBinaryComparisonPredicate(MySqlParser.BinaryComparisonPredicateContext ctx);
  /**
   * Exit a parse tree produced by the {@code binaryComparisonPredicate} labeled alternative in
   * {@link MySqlParser#predicate}.
   *
   * @param ctx the parse tree
   */
  void exitBinaryComparisonPredicate(MySqlParser.BinaryComparisonPredicateContext ctx);
  /**
   * Enter a parse tree produced by the {@code inPredicate} labeled alternative in {@link
   * MySqlParser#predicate}.
   *
   * @param ctx the parse tree
   */
  void enterInPredicate(MySqlParser.InPredicateContext ctx);
  /**
   * Exit a parse tree produced by the {@code inPredicate} labeled alternative in {@link
   * MySqlParser#predicate}.
   *
   * @param ctx the parse tree
   */
  void exitInPredicate(MySqlParser.InPredicateContext ctx);
  /**
   * Enter a parse tree produced by the {@code betweenPredicate} labeled alternative in {@link
   * MySqlParser#predicate}.
   *
   * @param ctx the parse tree
   */
  void enterBetweenPredicate(MySqlParser.BetweenPredicateContext ctx);
  /**
   * Exit a parse tree produced by the {@code betweenPredicate} labeled alternative in {@link
   * MySqlParser#predicate}.
   *
   * @param ctx the parse tree
   */
  void exitBetweenPredicate(MySqlParser.BetweenPredicateContext ctx);
  /**
   * Enter a parse tree produced by the {@code isNullPredicate} labeled alternative in {@link
   * MySqlParser#predicate}.
   *
   * @param ctx the parse tree
   */
  void enterIsNullPredicate(MySqlParser.IsNullPredicateContext ctx);
  /**
   * Exit a parse tree produced by the {@code isNullPredicate} labeled alternative in {@link
   * MySqlParser#predicate}.
   *
   * @param ctx the parse tree
   */
  void exitIsNullPredicate(MySqlParser.IsNullPredicateContext ctx);
  /**
   * Enter a parse tree produced by the {@code likePredicate} labeled alternative in {@link
   * MySqlParser#predicate}.
   *
   * @param ctx the parse tree
   */
  void enterLikePredicate(MySqlParser.LikePredicateContext ctx);
  /**
   * Exit a parse tree produced by the {@code likePredicate} labeled alternative in {@link
   * MySqlParser#predicate}.
   *
   * @param ctx the parse tree
   */
  void exitLikePredicate(MySqlParser.LikePredicateContext ctx);
  /**
   * Enter a parse tree produced by the {@code regexpPredicate} labeled alternative in {@link
   * MySqlParser#predicate}.
   *
   * @param ctx the parse tree
   */
  void enterRegexpPredicate(MySqlParser.RegexpPredicateContext ctx);
  /**
   * Exit a parse tree produced by the {@code regexpPredicate} labeled alternative in {@link
   * MySqlParser#predicate}.
   *
   * @param ctx the parse tree
   */
  void exitRegexpPredicate(MySqlParser.RegexpPredicateContext ctx);
  /**
   * Enter a parse tree produced by the {@code unaryExpressionAtom} labeled alternative in {@link
   * MySqlParser#expressionAtom}.
   *
   * @param ctx the parse tree
   */
  void enterUnaryExpressionAtom(MySqlParser.UnaryExpressionAtomContext ctx);
  /**
   * Exit a parse tree produced by the {@code unaryExpressionAtom} labeled alternative in {@link
   * MySqlParser#expressionAtom}.
   *
   * @param ctx the parse tree
   */
  void exitUnaryExpressionAtom(MySqlParser.UnaryExpressionAtomContext ctx);
  /**
   * Enter a parse tree produced by the {@code collateExpressionAtom} labeled alternative in {@link
   * MySqlParser#expressionAtom}.
   *
   * @param ctx the parse tree
   */
  void enterCollateExpressionAtom(MySqlParser.CollateExpressionAtomContext ctx);
  /**
   * Exit a parse tree produced by the {@code collateExpressionAtom} labeled alternative in {@link
   * MySqlParser#expressionAtom}.
   *
   * @param ctx the parse tree
   */
  void exitCollateExpressionAtom(MySqlParser.CollateExpressionAtomContext ctx);
  /**
   * Enter a parse tree produced by the {@code constantExpressionAtom} labeled alternative in {@link
   * MySqlParser#expressionAtom}.
   *
   * @param ctx the parse tree
   */
  void enterConstantExpressionAtom(MySqlParser.ConstantExpressionAtomContext ctx);
  /**
   * Exit a parse tree produced by the {@code constantExpressionAtom} labeled alternative in {@link
   * MySqlParser#expressionAtom}.
   *
   * @param ctx the parse tree
   */
  void exitConstantExpressionAtom(MySqlParser.ConstantExpressionAtomContext ctx);
  /**
   * Enter a parse tree produced by the {@code functionCallExpressionAtom} labeled alternative in
   * {@link MySqlParser#expressionAtom}.
   *
   * @param ctx the parse tree
   */
  void enterFunctionCallExpressionAtom(MySqlParser.FunctionCallExpressionAtomContext ctx);
  /**
   * Exit a parse tree produced by the {@code functionCallExpressionAtom} labeled alternative in
   * {@link MySqlParser#expressionAtom}.
   *
   * @param ctx the parse tree
   */
  void exitFunctionCallExpressionAtom(MySqlParser.FunctionCallExpressionAtomContext ctx);
  /**
   * Enter a parse tree produced by the {@code binaryExpressionAtom} labeled alternative in {@link
   * MySqlParser#expressionAtom}.
   *
   * @param ctx the parse tree
   */
  void enterBinaryExpressionAtom(MySqlParser.BinaryExpressionAtomContext ctx);
  /**
   * Exit a parse tree produced by the {@code binaryExpressionAtom} labeled alternative in {@link
   * MySqlParser#expressionAtom}.
   *
   * @param ctx the parse tree
   */
  void exitBinaryExpressionAtom(MySqlParser.BinaryExpressionAtomContext ctx);
  /**
   * Enter a parse tree produced by the {@code fullColumnNameExpressionAtom} labeled alternative in
   * {@link MySqlParser#expressionAtom}.
   *
   * @param ctx the parse tree
   */
  void enterFullColumnNameExpressionAtom(MySqlParser.FullColumnNameExpressionAtomContext ctx);
  /**
   * Exit a parse tree produced by the {@code fullColumnNameExpressionAtom} labeled alternative in
   * {@link MySqlParser#expressionAtom}.
   *
   * @param ctx the parse tree
   */
  void exitFullColumnNameExpressionAtom(MySqlParser.FullColumnNameExpressionAtomContext ctx);
  /**
   * Enter a parse tree produced by the {@code bitExpressionAtom} labeled alternative in {@link
   * MySqlParser#expressionAtom}.
   *
   * @param ctx the parse tree
   */
  void enterBitExpressionAtom(MySqlParser.BitExpressionAtomContext ctx);
  /**
   * Exit a parse tree produced by the {@code bitExpressionAtom} labeled alternative in {@link
   * MySqlParser#expressionAtom}.
   *
   * @param ctx the parse tree
   */
  void exitBitExpressionAtom(MySqlParser.BitExpressionAtomContext ctx);
  /**
   * Enter a parse tree produced by the {@code nestedExpressionAtom} labeled alternative in {@link
   * MySqlParser#expressionAtom}.
   *
   * @param ctx the parse tree
   */
  void enterNestedExpressionAtom(MySqlParser.NestedExpressionAtomContext ctx);
  /**
   * Exit a parse tree produced by the {@code nestedExpressionAtom} labeled alternative in {@link
   * MySqlParser#expressionAtom}.
   *
   * @param ctx the parse tree
   */
  void exitNestedExpressionAtom(MySqlParser.NestedExpressionAtomContext ctx);
  /**
   * Enter a parse tree produced by the {@code nestedRowExpressionAtom} labeled alternative in
   * {@link MySqlParser#expressionAtom}.
   *
   * @param ctx the parse tree
   */
  void enterNestedRowExpressionAtom(MySqlParser.NestedRowExpressionAtomContext ctx);
  /**
   * Exit a parse tree produced by the {@code nestedRowExpressionAtom} labeled alternative in {@link
   * MySqlParser#expressionAtom}.
   *
   * @param ctx the parse tree
   */
  void exitNestedRowExpressionAtom(MySqlParser.NestedRowExpressionAtomContext ctx);
  /**
   * Enter a parse tree produced by the {@code mathExpressionAtom} labeled alternative in {@link
   * MySqlParser#expressionAtom}.
   *
   * @param ctx the parse tree
   */
  void enterMathExpressionAtom(MySqlParser.MathExpressionAtomContext ctx);
  /**
   * Exit a parse tree produced by the {@code mathExpressionAtom} labeled alternative in {@link
   * MySqlParser#expressionAtom}.
   *
   * @param ctx the parse tree
   */
  void exitMathExpressionAtom(MySqlParser.MathExpressionAtomContext ctx);
  /**
   * Enter a parse tree produced by the {@code intervalExpressionAtom} labeled alternative in {@link
   * MySqlParser#expressionAtom}.
   *
   * @param ctx the parse tree
   */
  void enterIntervalExpressionAtom(MySqlParser.IntervalExpressionAtomContext ctx);
  /**
   * Exit a parse tree produced by the {@code intervalExpressionAtom} labeled alternative in {@link
   * MySqlParser#expressionAtom}.
   *
   * @param ctx the parse tree
   */
  void exitIntervalExpressionAtom(MySqlParser.IntervalExpressionAtomContext ctx);
  /**
   * Enter a parse tree produced by {@link MySqlParser#unaryOperator}.
   *
   * @param ctx the parse tree
   */
  void enterUnaryOperator(MySqlParser.UnaryOperatorContext ctx);
  /**
   * Exit a parse tree produced by {@link MySqlParser#unaryOperator}.
   *
   * @param ctx the parse tree
   */
  void exitUnaryOperator(MySqlParser.UnaryOperatorContext ctx);
  /**
   * Enter a parse tree produced by {@link MySqlParser#comparisonOperator}.
   *
   * @param ctx the parse tree
   */
  void enterComparisonOperator(MySqlParser.ComparisonOperatorContext ctx);
  /**
   * Exit a parse tree produced by {@link MySqlParser#comparisonOperator}.
   *
   * @param ctx the parse tree
   */
  void exitComparisonOperator(MySqlParser.ComparisonOperatorContext ctx);
  /**
   * Enter a parse tree produced by {@link MySqlParser#logicalOperator}.
   *
   * @param ctx the parse tree
   */
  void enterLogicalOperator(MySqlParser.LogicalOperatorContext ctx);
  /**
   * Exit a parse tree produced by {@link MySqlParser#logicalOperator}.
   *
   * @param ctx the parse tree
   */
  void exitLogicalOperator(MySqlParser.LogicalOperatorContext ctx);
  /**
   * Enter a parse tree produced by {@link MySqlParser#bitOperator}.
   *
   * @param ctx the parse tree
   */
  void enterBitOperator(MySqlParser.BitOperatorContext ctx);
  /**
   * Exit a parse tree produced by {@link MySqlParser#bitOperator}.
   *
   * @param ctx the parse tree
   */
  void exitBitOperator(MySqlParser.BitOperatorContext ctx);
  /**
   * Enter a parse tree produced by {@link MySqlParser#mathOperator}.
   *
   * @param ctx the parse tree
   */
  void enterMathOperator(MySqlParser.MathOperatorContext ctx);
  /**
   * Exit a parse tree produced by {@link MySqlParser#mathOperator}.
   *
   * @param ctx the parse tree
   */
  void exitMathOperator(MySqlParser.MathOperatorContext ctx);
  /**
   * Enter a parse tree produced by {@link MySqlParser#charsetNameBase}.
   *
   * @param ctx the parse tree
   */
  void enterCharsetNameBase(MySqlParser.CharsetNameBaseContext ctx);
  /**
   * Exit a parse tree produced by {@link MySqlParser#charsetNameBase}.
   *
   * @param ctx the parse tree
   */
  void exitCharsetNameBase(MySqlParser.CharsetNameBaseContext ctx);
  /**
   * Enter a parse tree produced by {@link MySqlParser#intervalTypeBase}.
   *
   * @param ctx the parse tree
   */
  void enterIntervalTypeBase(MySqlParser.IntervalTypeBaseContext ctx);
  /**
   * Exit a parse tree produced by {@link MySqlParser#intervalTypeBase}.
   *
   * @param ctx the parse tree
   */
  void exitIntervalTypeBase(MySqlParser.IntervalTypeBaseContext ctx);
  /**
   * Enter a parse tree produced by {@link MySqlParser#dataTypeBase}.
   *
   * @param ctx the parse tree
   */
  void enterDataTypeBase(MySqlParser.DataTypeBaseContext ctx);
  /**
   * Exit a parse tree produced by {@link MySqlParser#dataTypeBase}.
   *
   * @param ctx the parse tree
   */
  void exitDataTypeBase(MySqlParser.DataTypeBaseContext ctx);
  /**
   * Enter a parse tree produced by {@link MySqlParser#keywordsCanBeId}.
   *
   * @param ctx the parse tree
   */
  void enterKeywordsCanBeId(MySqlParser.KeywordsCanBeIdContext ctx);
  /**
   * Exit a parse tree produced by {@link MySqlParser#keywordsCanBeId}.
   *
   * @param ctx the parse tree
   */
  void exitKeywordsCanBeId(MySqlParser.KeywordsCanBeIdContext ctx);
  /**
   * Enter a parse tree produced by {@link MySqlParser#functionNameBase}.
   *
   * @param ctx the parse tree
   */
  void enterFunctionNameBase(MySqlParser.FunctionNameBaseContext ctx);
  /**
   * Exit a parse tree produced by {@link MySqlParser#functionNameBase}.
   *
   * @param ctx the parse tree
   */
  void exitFunctionNameBase(MySqlParser.FunctionNameBaseContext ctx);
}
