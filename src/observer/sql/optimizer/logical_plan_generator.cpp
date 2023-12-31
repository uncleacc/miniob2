/* Copyright (c) 2023 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2023/08/16.
//

#include "sql/optimizer/logical_plan_generator.h"

#include "sql/operator/logical_operator.h"
#include "sql/operator/calc_logical_operator.h"
#include "sql/operator/project_logical_operator.h"
#include "sql/operator/predicate_logical_operator.h"
#include "sql/operator/table_get_logical_operator.h"
#include "sql/operator/insert_logical_operator.h"
#include "sql/operator/delete_logical_operator.h"
#include "sql/operator/join_logical_operator.h"
#include "sql/operator/project_logical_operator.h"
#include "sql/operator/explain_logical_operator.h"
#include "sql/operator/update_logical_operator.h" // new
#include "sql/operator/aggr_logical_operator.h"   // new

#include "sql/stmt/stmt.h"
#include "sql/stmt/calc_stmt.h"
#include "sql/stmt/select_stmt.h"
#include "sql/stmt/filter_stmt.h"
#include "sql/stmt/insert_stmt.h"
#include "sql/stmt/delete_stmt.h"
#include "sql/stmt/explain_stmt.h"
#include "sql/stmt/update_stmt.h" // new
#include "sql/stmt/join_stmt.h"

using namespace std;

RC LogicalPlanGenerator::create(Stmt *stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  RC rc = RC::SUCCESS;
  switch (stmt->type()) {
    case StmtType::CALC: {
      CalcStmt *calc_stmt = static_cast<CalcStmt *>(stmt);
      rc = create_plan(calc_stmt, logical_operator);
    } break;

    case StmtType::SELECT: {
      SelectStmt *select_stmt = static_cast<SelectStmt *>(stmt);
      rc = create_plan(select_stmt, logical_operator);
    } break;

    case StmtType::INSERT: {
      InsertStmt *insert_stmt = static_cast<InsertStmt *>(stmt);
      rc = create_plan(insert_stmt, logical_operator);
    } break;

    case StmtType::UPDATE: {    // new
      UpdateStmt *update_stmt = static_cast<UpdateStmt *>(stmt);
      rc = create_plan(update_stmt, logical_operator);
    } break;

    case StmtType::DELETE: {
      DeleteStmt *delete_stmt = static_cast<DeleteStmt *>(stmt);
      rc = create_plan(delete_stmt, logical_operator);
    } break;

    case StmtType::EXPLAIN: {
      ExplainStmt *explain_stmt = static_cast<ExplainStmt *>(stmt);
      rc = create_plan(explain_stmt, logical_operator);
    } break;
    default: {
      rc = RC::UNIMPLENMENT;
    }
  }
  return rc;
}

RC LogicalPlanGenerator::create_plan(CalcStmt *calc_stmt, std::unique_ptr<LogicalOperator> &logical_operator)
{
  logical_operator.reset(new CalcLogicalOperator(std::move(calc_stmt->expressions())));
  return RC::SUCCESS;
}

  // table_scan算子   table_scan算子
  //    where            where
  //                |
  //         join算子（未实现）
  //                |
  //             过滤算子              ：连接条件
  //                |
  //             group_by             ：分组
  //                |
  //             聚合算子
  //                |
  //             投影算子             ：列选择
RC LogicalPlanGenerator::create_plan(
    SelectStmt *select_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  DEBUG_PRINT("debug: 创建select逻辑算子\n");
  unique_ptr<LogicalOperator> table_oper(nullptr);

  std::vector<Expression*> &query_exprs = select_stmt->query_exprs(); // select部分

  std::vector<Expression*> aggr_exprs;
  bool is_join_by_con = select_stmt->join_stmts().size() > 0;         // 是否为inner join
  std::vector<JoinStmt*> &join_stmts = select_stmt->join_stmts();     // join部分
  int i = 0;

  FilterStmt * filter_stmt = select_stmt->filter_stmt();
  // FROM
  const std::vector<Table *> &tables = select_stmt->tables();
  for (Table *table : tables) {
    std::vector<Field> fields;
    // ================== select ================== //
    for (Expression *expr : query_exprs) {
      switch (expr->type()) {
        case ExprType::FIELD : {
          FieldExpr *field_expr = static_cast<FieldExpr*>(expr);
          if (0 == strcmp(field_expr->field().table_name(), table->name())) {
            fields.push_back(field_expr->field());
          }
        } break;
        case ExprType::AGGREGATION : {
          AggregationExpr *aggr_expr = static_cast<AggregationExpr*>(expr);
          if (0 == strcmp(aggr_expr->field().table_name(), table->name())) {
            fields.push_back(aggr_expr->field());
            aggr_exprs.push_back(expr);    // <----------------------------
          }
        } break;
        default : {
          return RC::INTERNAL;
        }
      }
    }
    // ================== table ================== //
    TableGetLogicalOperator *table_scan_oper = new TableGetLogicalOperator(table, fields, true/*readonly*/);
    #if 1
    // 每次创建一个table scan逻辑算子就下推表达式
    if (filter_stmt) {  // 如果有过滤语句
      std::vector<std::unique_ptr<Expression>> table_scan_exprs;
      std::vector<FilterUnit *> &filter_units = filter_stmt->filter_units();

      for (auto filter_unit = filter_units.rbegin(); filter_unit != filter_units.rend();) {
        const FilterObj &filter_obj_left = (*filter_unit)->left();
        const FilterObj &filter_obj_right = (*filter_unit)->right();
        // 如果是一个列比较一个确定值，则下推表达式
        if (  (
                ( filter_obj_left.is_attr && !filter_obj_right.is_attr ) &&         // 是列与属性比较
                ( 0 == strcmp(filter_obj_left.field.table_name(), table->name()))   // 是当前表
              ) ||
              (
                ( !filter_obj_left.is_attr && filter_obj_right.is_attr) &&
                ( 0 == strcmp(filter_obj_left.field.table_name(), table->name()))
              ) 
           ) {
          unique_ptr<Expression> left(filter_obj_left.is_attr
                                              ? static_cast<Expression *>(new FieldExpr(filter_obj_left.field))
                                              : static_cast<Expression *>(new ValueExpr(filter_obj_left.value)));

          unique_ptr<Expression> right(filter_obj_right.is_attr
                                                ? static_cast<Expression *>(new FieldExpr(filter_obj_right.field))
                                                : static_cast<Expression *>(new ValueExpr(filter_obj_right.value)));

          ComparisonExpr *cmp_expr = new ComparisonExpr((*filter_unit)->comp(), std::move(left), std::move(right));
          table_scan_exprs.emplace_back(cmp_expr);
          filter_unit = decltype(filter_unit)(filter_units.erase(std::next(filter_unit).base())); // 删除并更新迭代器
        } else {
          ++filter_unit;    
        }
      }

      table_scan_oper->set_predicates(std::move(table_scan_exprs));
    }
    #endif
    
    unique_ptr<LogicalOperator> table_get_oper(table_scan_oper);
    // ================== join ================== //
    if (is_join_by_con) {
      // ================== inner join ================== //
      if (table_oper == nullptr) {
        table_oper = std::move(table_get_oper);
      } else {        
        unique_ptr<LogicalOperator> join_oper(new JoinLogicalOperator);
        join_oper->add_child(std::move(table_oper));
        join_oper->add_child(std::move(table_get_oper));
        unique_ptr<LogicalOperator> predicate_oper;
        FilterStmt *filter = join_stmts[i - 1]->join_condition(); // i - 1
        RC rc = create_plan(filter, predicate_oper);
        if (rc != RC::SUCCESS) {
          LOG_WARN("failed to create predicate logical plan. rc=%s", strrc(rc));
          return rc;
        }
        predicate_oper->add_child(std::move(join_oper));

        table_oper = std::move(predicate_oper);
      }
    }
    // ================== natural join ================== //
    else {
      if (table_oper == nullptr) {
        table_oper = std::move(table_get_oper);
      } else {
        JoinLogicalOperator *join_oper = new JoinLogicalOperator;
        join_oper->add_child(std::move(table_oper));
        join_oper->add_child(std::move(table_get_oper));
        table_oper = unique_ptr<LogicalOperator>(join_oper);
      }
    }

    i++;
  }
  // ================== where ================== //
  unique_ptr<LogicalOperator> predicate_oper;
  RC rc = create_plan(select_stmt->filter_stmt(), predicate_oper);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create predicate logical plan. rc=%s", strrc(rc));
    return rc;
  }

  // GROUP_BY

  // HAVING(2023不实现)

  // ================== 聚合 ================== //
  unique_ptr<LogicalOperator> aggr_oper(new AggregationLogicalOperator(aggr_exprs));
  bool aggr_ = true;
  if (aggr_exprs.size() != 0) {     // 如果有投影算子，则将后面的都连好
    if (predicate_oper) {
      if (table_oper) {
        predicate_oper->add_child(std::move(table_oper));
        DEBUG_PRINT("debug: 添加table算子\n");
      }
      aggr_oper->add_child(std::move(predicate_oper));
      DEBUG_PRINT("debug: 添加过滤算子\n");
    } else {
      if (table_oper) {
        DEBUG_PRINT("debug: 添加table算子\n");
        aggr_oper->add_child(std::move(table_oper));
      }
    }
  } else {
    aggr_ = false;
  }
  // ================== project ================== //
  unique_ptr<LogicalOperator> project_oper(new ProjectLogicalOperator(query_exprs));
  if (aggr_) {
    project_oper->add_child(std::move(aggr_oper));

    logical_operator.swap(project_oper);
    return RC::SUCCESS;
  } 
  // ================== OK ================== //
  if (predicate_oper) {
    if (table_oper) {
      predicate_oper->add_child(std::move(table_oper));
    }
    project_oper->add_child(std::move(predicate_oper));
  } else {
    if (table_oper) {
      project_oper->add_child(std::move(table_oper));
    }
  }
  // ================== set result ================== //
  logical_operator.swap(project_oper);
  return RC::SUCCESS;
}

RC LogicalPlanGenerator::create_plan(
    FilterStmt *filter_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  std::vector<unique_ptr<Expression>> cmp_exprs;
  const std::vector<FilterUnit *> &filter_units = filter_stmt->filter_units();
  for (const FilterUnit *filter_unit : filter_units) {
    const FilterObj &filter_obj_left = filter_unit->left();
    const FilterObj &filter_obj_right = filter_unit->right();
    DEBUG_PRINT("debug: 左属性: %d\n", filter_obj_left.is_attr);
    DEBUG_PRINT("debug: 右属性: %d\n", filter_obj_right.is_attr);

    unique_ptr<Expression> left(filter_obj_left.is_attr
                                         ? static_cast<Expression *>(new FieldExpr(filter_obj_left.field))
                                         : static_cast<Expression *>(new ValueExpr(filter_obj_left.value)));

    unique_ptr<Expression> right(filter_obj_right.is_attr
                                          ? static_cast<Expression *>(new FieldExpr(filter_obj_right.field))
                                          : static_cast<Expression *>(new ValueExpr(filter_obj_right.value)));

    ComparisonExpr *cmp_expr = new ComparisonExpr(filter_unit->comp(), std::move(left), std::move(right));
    cmp_exprs.emplace_back(cmp_expr);
  }
  unique_ptr<PredicateLogicalOperator> predicate_oper;
  if (!cmp_exprs.empty()) {
    unique_ptr<ConjunctionExpr> conjunction_expr(new ConjunctionExpr(ConjunctionExpr::Type::AND, cmp_exprs));
    predicate_oper = unique_ptr<PredicateLogicalOperator>(new PredicateLogicalOperator(std::move(conjunction_expr)));
  }

  logical_operator = std::move(predicate_oper);
  return RC::SUCCESS;
}

RC LogicalPlanGenerator::create_plan(
    InsertStmt *insert_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  Table *table = insert_stmt->table();
  vector<Value> values(insert_stmt->values(), insert_stmt->values() + insert_stmt->value_amount());

  InsertLogicalOperator *insert_operator = new InsertLogicalOperator(table, values);
  logical_operator.reset(insert_operator);
  return RC::SUCCESS;
}

// new
RC LogicalPlanGenerator::create_plan(
    UpdateStmt *update_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  Table *table = update_stmt->table();                  // 待更新表
  Value *value = update_stmt->value();
  std::string field_name = update_stmt->field_name();
  FilterStmt *filter_stmt = update_stmt->filter_stmt(); // 过滤条件
  std::vector<Field> fields;                            // 待更新表的字段

  // 获得待更新表的字段
  for (int i = table->table_meta().sys_field_num(); i < table->table_meta().field_num(); i++) {
    const FieldMeta *field_meta = table->table_meta().field(i);
    fields.push_back(Field(table, field_meta));
  }
  // 创建获取表数据的算子，false即可修改
  unique_ptr<LogicalOperator> table_get_oper(new TableGetLogicalOperator(table, fields, false/*readonly*/));
  // 由上面的过滤条件创建谓词/过滤逻辑算子
  unique_ptr<LogicalOperator> predicate_oper;
  RC rc = create_plan(filter_stmt, predicate_oper);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  // 创建更新算子
  unique_ptr<LogicalOperator> update_oper(new UpdateLogicalOperator(table, value, field_name));

  // 如果有过滤算子（where子句），添加到更新算子中
  if (predicate_oper) {
    predicate_oper->add_child(std::move(table_get_oper));
    update_oper->add_child(std::move(predicate_oper));
  } else {
    update_oper->add_child(std::move(table_get_oper));
  }

  logical_operator = std::move(update_oper);
  return RC::SUCCESS;
}

RC LogicalPlanGenerator::create_plan(
    DeleteStmt *delete_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  Table *table = delete_stmt->table();
  FilterStmt *filter_stmt = delete_stmt->filter_stmt();
  std::vector<Field> fields;
  for (int i = table->table_meta().sys_field_num(); i < table->table_meta().field_num(); i++) {
    const FieldMeta *field_meta = table->table_meta().field(i);
    fields.push_back(Field(table, field_meta));
  }
  unique_ptr<LogicalOperator> table_get_oper(new TableGetLogicalOperator(table, fields, false/*readonly*/));

  unique_ptr<LogicalOperator> predicate_oper;
  RC rc = create_plan(filter_stmt, predicate_oper);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  unique_ptr<LogicalOperator> delete_oper(new DeleteLogicalOperator(table));

  if (predicate_oper) {
    predicate_oper->add_child(std::move(table_get_oper));
    delete_oper->add_child(std::move(predicate_oper));
  } else {
    delete_oper->add_child(std::move(table_get_oper));
  }

  logical_operator = std::move(delete_oper);
  return rc;
}

RC LogicalPlanGenerator::create_plan(
    ExplainStmt *explain_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  Stmt *child_stmt = explain_stmt->child();
  unique_ptr<LogicalOperator> child_oper;
  RC rc = create(child_stmt, child_oper);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create explain's child operator. rc=%s", strrc(rc));
    return rc;
  }

  logical_operator = unique_ptr<LogicalOperator>(new ExplainLogicalOperator);
  logical_operator->add_child(std::move(child_oper));
  return rc;
}
