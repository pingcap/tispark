// Generated from MySqlParser.g4 by ANTLR 4.7.1
package com.pingcap.tikv.parser;

import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced by {@link
 * MySqlParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for operations with no return
 *     type.
 */
public interface MySqlParserVisitor<T> extends ParseTreeVisitor<T> {
  /**
   * Visit a parse tree produced by {@link MySqlParser#intervalType}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitIntervalType(MySqlParser.IntervalTypeContext ctx);
  /**
   * Visit a parse tree produced by {@link MySqlParser#fullId}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitFullId(MySqlParser.FullIdContext ctx);
  /**
   * Visit a parse tree produced by {@link MySqlParser#fullColumnName}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitFullColumnName(MySqlParser.FullColumnNameContext ctx);
  /**
   * Visit a parse tree produced by {@link MySqlParser#charsetName}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitCharsetName(MySqlParser.CharsetNameContext ctx);
  /**
   * Visit a parse tree produced by {@link MySqlParser#collationName}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitCollationName(MySqlParser.CollationNameContext ctx);
  /**
   * Visit a parse tree produced by {@link MySqlParser#engineName}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitEngineName(MySqlParser.EngineNameContext ctx);
  /**
   * Visit a parse tree produced by {@link MySqlParser#uid}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitUid(MySqlParser.UidContext ctx);
  /**
   * Visit a parse tree produced by {@link MySqlParser#simpleId}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSimpleId(MySqlParser.SimpleIdContext ctx);
  /**
   * Visit a parse tree produced by {@link MySqlParser#dottedId}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDottedId(MySqlParser.DottedIdContext ctx);
  /**
   * Visit a parse tree produced by {@link MySqlParser#decimalLiteral}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDecimalLiteral(MySqlParser.DecimalLiteralContext ctx);
  /**
   * Visit a parse tree produced by {@link MySqlParser#stringLiteral}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitStringLiteral(MySqlParser.StringLiteralContext ctx);
  /**
   * Visit a parse tree produced by {@link MySqlParser#booleanLiteral}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitBooleanLiteral(MySqlParser.BooleanLiteralContext ctx);
  /**
   * Visit a parse tree produced by {@link MySqlParser#hexadecimalLiteral}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitHexadecimalLiteral(MySqlParser.HexadecimalLiteralContext ctx);
  /**
   * Visit a parse tree produced by {@link MySqlParser#nullNotnull}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitNullNotnull(MySqlParser.NullNotnullContext ctx);
  /**
   * Visit a parse tree produced by {@link MySqlParser#constant}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitConstant(MySqlParser.ConstantContext ctx);
  /**
   * Visit a parse tree produced by {@link MySqlParser#convertedDataType}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitConvertedDataType(MySqlParser.ConvertedDataTypeContext ctx);
  /**
   * Visit a parse tree produced by {@link MySqlParser#lengthOneDimension}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLengthOneDimension(MySqlParser.LengthOneDimensionContext ctx);
  /**
   * Visit a parse tree produced by {@link MySqlParser#lengthTwoDimension}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLengthTwoDimension(MySqlParser.LengthTwoDimensionContext ctx);
  /**
   * Visit a parse tree produced by {@link MySqlParser#expressions}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitExpressions(MySqlParser.ExpressionsContext ctx);
  /**
   * Visit a parse tree produced by {@link MySqlParser#currentTimestamp}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitCurrentTimestamp(MySqlParser.CurrentTimestampContext ctx);
  /**
   * Visit a parse tree produced by the {@code specificFunctionCall} labeled alternative in {@link
   * MySqlParser#functionCall}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSpecificFunctionCall(MySqlParser.SpecificFunctionCallContext ctx);
  /**
   * Visit a parse tree produced by the {@code scalarFunctionCall} labeled alternative in {@link
   * MySqlParser#functionCall}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitScalarFunctionCall(MySqlParser.ScalarFunctionCallContext ctx);
  /**
   * Visit a parse tree produced by the {@code udfFunctionCall} labeled alternative in {@link
   * MySqlParser#functionCall}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitUdfFunctionCall(MySqlParser.UdfFunctionCallContext ctx);
  /**
   * Visit a parse tree produced by the {@code simpleFunctionCall} labeled alternative in {@link
   * MySqlParser#specificFunction}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSimpleFunctionCall(MySqlParser.SimpleFunctionCallContext ctx);
  /**
   * Visit a parse tree produced by the {@code dataTypeFunctionCall} labeled alternative in {@link
   * MySqlParser#specificFunction}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDataTypeFunctionCall(MySqlParser.DataTypeFunctionCallContext ctx);
  /**
   * Visit a parse tree produced by the {@code valuesFunctionCall} labeled alternative in {@link
   * MySqlParser#specificFunction}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitValuesFunctionCall(MySqlParser.ValuesFunctionCallContext ctx);
  /**
   * Visit a parse tree produced by the {@code caseFunctionCall} labeled alternative in {@link
   * MySqlParser#specificFunction}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitCaseFunctionCall(MySqlParser.CaseFunctionCallContext ctx);
  /**
   * Visit a parse tree produced by the {@code charFunctionCall} labeled alternative in {@link
   * MySqlParser#specificFunction}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitCharFunctionCall(MySqlParser.CharFunctionCallContext ctx);
  /**
   * Visit a parse tree produced by the {@code positionFunctionCall} labeled alternative in {@link
   * MySqlParser#specificFunction}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitPositionFunctionCall(MySqlParser.PositionFunctionCallContext ctx);
  /**
   * Visit a parse tree produced by the {@code substrFunctionCall} labeled alternative in {@link
   * MySqlParser#specificFunction}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSubstrFunctionCall(MySqlParser.SubstrFunctionCallContext ctx);
  /**
   * Visit a parse tree produced by the {@code trimFunctionCall} labeled alternative in {@link
   * MySqlParser#specificFunction}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitTrimFunctionCall(MySqlParser.TrimFunctionCallContext ctx);
  /**
   * Visit a parse tree produced by the {@code weightFunctionCall} labeled alternative in {@link
   * MySqlParser#specificFunction}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitWeightFunctionCall(MySqlParser.WeightFunctionCallContext ctx);
  /**
   * Visit a parse tree produced by the {@code extractFunctionCall} labeled alternative in {@link
   * MySqlParser#specificFunction}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitExtractFunctionCall(MySqlParser.ExtractFunctionCallContext ctx);
  /**
   * Visit a parse tree produced by the {@code getFormatFunctionCall} labeled alternative in {@link
   * MySqlParser#specificFunction}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitGetFormatFunctionCall(MySqlParser.GetFormatFunctionCallContext ctx);
  /**
   * Visit a parse tree produced by {@link MySqlParser#caseFuncAlternative}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitCaseFuncAlternative(MySqlParser.CaseFuncAlternativeContext ctx);
  /**
   * Visit a parse tree produced by the {@code levelWeightList} labeled alternative in {@link
   * MySqlParser#levelsInWeightString}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLevelWeightList(MySqlParser.LevelWeightListContext ctx);
  /**
   * Visit a parse tree produced by the {@code levelWeightRange} labeled alternative in {@link
   * MySqlParser#levelsInWeightString}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLevelWeightRange(MySqlParser.LevelWeightRangeContext ctx);
  /**
   * Visit a parse tree produced by {@link MySqlParser#levelInWeightListElement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLevelInWeightListElement(MySqlParser.LevelInWeightListElementContext ctx);
  /**
   * Visit a parse tree produced by {@link MySqlParser#scalarFunctionName}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitScalarFunctionName(MySqlParser.ScalarFunctionNameContext ctx);
  /**
   * Visit a parse tree produced by {@link MySqlParser#functionArgs}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitFunctionArgs(MySqlParser.FunctionArgsContext ctx);
  /**
   * Visit a parse tree produced by {@link MySqlParser#functionArg}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitFunctionArg(MySqlParser.FunctionArgContext ctx);
  /**
   * Visit a parse tree produced by the {@code isExpression} labeled alternative in {@link
   * MySqlParser#expression}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitIsExpression(MySqlParser.IsExpressionContext ctx);
  /**
   * Visit a parse tree produced by the {@code notExpression} labeled alternative in {@link
   * MySqlParser#expression}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitNotExpression(MySqlParser.NotExpressionContext ctx);
  /**
   * Visit a parse tree produced by the {@code logicalExpression} labeled alternative in {@link
   * MySqlParser#expression}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLogicalExpression(MySqlParser.LogicalExpressionContext ctx);
  /**
   * Visit a parse tree produced by the {@code predicateExpression} labeled alternative in {@link
   * MySqlParser#expression}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitPredicateExpression(MySqlParser.PredicateExpressionContext ctx);
  /**
   * Visit a parse tree produced by the {@code soundsLikePredicate} labeled alternative in {@link
   * MySqlParser#predicate}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSoundsLikePredicate(MySqlParser.SoundsLikePredicateContext ctx);
  /**
   * Visit a parse tree produced by the {@code expressionAtomPredicate} labeled alternative in
   * {@link MySqlParser#predicate}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitExpressionAtomPredicate(MySqlParser.ExpressionAtomPredicateContext ctx);
  /**
   * Visit a parse tree produced by the {@code binaryComparisonPredicate} labeled alternative in
   * {@link MySqlParser#predicate}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitBinaryComparisonPredicate(MySqlParser.BinaryComparisonPredicateContext ctx);
  /**
   * Visit a parse tree produced by the {@code inPredicate} labeled alternative in {@link
   * MySqlParser#predicate}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitInPredicate(MySqlParser.InPredicateContext ctx);
  /**
   * Visit a parse tree produced by the {@code betweenPredicate} labeled alternative in {@link
   * MySqlParser#predicate}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitBetweenPredicate(MySqlParser.BetweenPredicateContext ctx);
  /**
   * Visit a parse tree produced by the {@code isNullPredicate} labeled alternative in {@link
   * MySqlParser#predicate}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitIsNullPredicate(MySqlParser.IsNullPredicateContext ctx);
  /**
   * Visit a parse tree produced by the {@code likePredicate} labeled alternative in {@link
   * MySqlParser#predicate}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLikePredicate(MySqlParser.LikePredicateContext ctx);
  /**
   * Visit a parse tree produced by the {@code regexpPredicate} labeled alternative in {@link
   * MySqlParser#predicate}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitRegexpPredicate(MySqlParser.RegexpPredicateContext ctx);
  /**
   * Visit a parse tree produced by the {@code unaryExpressionAtom} labeled alternative in {@link
   * MySqlParser#expressionAtom}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitUnaryExpressionAtom(MySqlParser.UnaryExpressionAtomContext ctx);
  /**
   * Visit a parse tree produced by the {@code collateExpressionAtom} labeled alternative in {@link
   * MySqlParser#expressionAtom}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitCollateExpressionAtom(MySqlParser.CollateExpressionAtomContext ctx);
  /**
   * Visit a parse tree produced by the {@code constantExpressionAtom} labeled alternative in {@link
   * MySqlParser#expressionAtom}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitConstantExpressionAtom(MySqlParser.ConstantExpressionAtomContext ctx);
  /**
   * Visit a parse tree produced by the {@code functionCallExpressionAtom} labeled alternative in
   * {@link MySqlParser#expressionAtom}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitFunctionCallExpressionAtom(MySqlParser.FunctionCallExpressionAtomContext ctx);
  /**
   * Visit a parse tree produced by the {@code binaryExpressionAtom} labeled alternative in {@link
   * MySqlParser#expressionAtom}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitBinaryExpressionAtom(MySqlParser.BinaryExpressionAtomContext ctx);
  /**
   * Visit a parse tree produced by the {@code fullColumnNameExpressionAtom} labeled alternative in
   * {@link MySqlParser#expressionAtom}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitFullColumnNameExpressionAtom(MySqlParser.FullColumnNameExpressionAtomContext ctx);
  /**
   * Visit a parse tree produced by the {@code bitExpressionAtom} labeled alternative in {@link
   * MySqlParser#expressionAtom}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitBitExpressionAtom(MySqlParser.BitExpressionAtomContext ctx);
  /**
   * Visit a parse tree produced by the {@code nestedExpressionAtom} labeled alternative in {@link
   * MySqlParser#expressionAtom}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitNestedExpressionAtom(MySqlParser.NestedExpressionAtomContext ctx);
  /**
   * Visit a parse tree produced by the {@code nestedRowExpressionAtom} labeled alternative in
   * {@link MySqlParser#expressionAtom}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitNestedRowExpressionAtom(MySqlParser.NestedRowExpressionAtomContext ctx);
  /**
   * Visit a parse tree produced by the {@code mathExpressionAtom} labeled alternative in {@link
   * MySqlParser#expressionAtom}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitMathExpressionAtom(MySqlParser.MathExpressionAtomContext ctx);
  /**
   * Visit a parse tree produced by the {@code intervalExpressionAtom} labeled alternative in {@link
   * MySqlParser#expressionAtom}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitIntervalExpressionAtom(MySqlParser.IntervalExpressionAtomContext ctx);
  /**
   * Visit a parse tree produced by {@link MySqlParser#unaryOperator}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitUnaryOperator(MySqlParser.UnaryOperatorContext ctx);
  /**
   * Visit a parse tree produced by {@link MySqlParser#comparisonOperator}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitComparisonOperator(MySqlParser.ComparisonOperatorContext ctx);
  /**
   * Visit a parse tree produced by {@link MySqlParser#logicalOperator}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLogicalOperator(MySqlParser.LogicalOperatorContext ctx);
  /**
   * Visit a parse tree produced by {@link MySqlParser#bitOperator}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitBitOperator(MySqlParser.BitOperatorContext ctx);
  /**
   * Visit a parse tree produced by {@link MySqlParser#mathOperator}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitMathOperator(MySqlParser.MathOperatorContext ctx);
  /**
   * Visit a parse tree produced by {@link MySqlParser#charsetNameBase}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitCharsetNameBase(MySqlParser.CharsetNameBaseContext ctx);
  /**
   * Visit a parse tree produced by {@link MySqlParser#transactionLevelBase}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitTransactionLevelBase(MySqlParser.TransactionLevelBaseContext ctx);
  /**
   * Visit a parse tree produced by {@link MySqlParser#privilegesBase}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitPrivilegesBase(MySqlParser.PrivilegesBaseContext ctx);
  /**
   * Visit a parse tree produced by {@link MySqlParser#intervalTypeBase}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitIntervalTypeBase(MySqlParser.IntervalTypeBaseContext ctx);
  /**
   * Visit a parse tree produced by {@link MySqlParser#dataTypeBase}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDataTypeBase(MySqlParser.DataTypeBaseContext ctx);
  /**
   * Visit a parse tree produced by {@link MySqlParser#keywordsCanBeId}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitKeywordsCanBeId(MySqlParser.KeywordsCanBeIdContext ctx);
  /**
   * Visit a parse tree produced by {@link MySqlParser#functionNameBase}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitFunctionNameBase(MySqlParser.FunctionNameBaseContext ctx);
}
