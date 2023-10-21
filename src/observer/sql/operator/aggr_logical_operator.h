//
// Created by NieYang on 2023/10/20.
//

#pragma once

#include "sql/operator/logical_operator.h"
#include "sql/expr/expression.h"

class Expression;
/**
 * @brief 谓词/过滤逻辑算子
 * @ingroup LogicalOperator
 */
class AggregationLogicalOperator : public LogicalOperator 
{
public:
  AggregationLogicalOperator(std::vector<Expression*> expressions);
  virtual ~AggregationLogicalOperator() = default;

  LogicalOperatorType type() const override
  {
    return LogicalOperatorType::AGGR_LOGICAL_T;
  }

  std::vector<Expression*>& select_exprs()
  {
    return select_exprs_;
  }

private:
  std::vector<Expression*> select_exprs_;
};
