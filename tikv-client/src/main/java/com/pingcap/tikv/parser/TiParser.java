package com.pingcap.tikv.parser;

import com.pingcap.tikv.expression.Expression;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.apache.spark.sql.catalyst.parser.UpperCaseCharStream;

public class TiParser extends MySqlParserBaseVisitor {
  private final AstBuilder astBuilder = new AstBuilder();

  public Expression parseExpression(String command) {
    MySqlLexer lexer = new MySqlLexer(new UpperCaseCharStream(CharStreams.fromString(command)));
    CommonTokenStream cmnTokStrm = new CommonTokenStream(lexer);
    MySqlParser parser = new MySqlParser(cmnTokStrm);

    parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
    return astBuilder.visitExpressions(parser.expressions());
  }
}
