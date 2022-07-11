package com.pingcap.tikv.partition;

import com.pingcap.tikv.expression.Expression;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class PartitionExpression implements Serializable {

  private Expression originExpression;

  private Map<String, List<Expression>> rangeColumnRefBoundExpressions;

  private List<Expression> rangePartitionBoundExpressions;
}
