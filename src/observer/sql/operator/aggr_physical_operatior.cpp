//
// Created by NieYang on 2023/10/20.
//

#include "aggr_physical_operatior.h"
#include "storage/table/table.h"
#include "event/sql_debug.h"

RC AggrPhysicalOperator::open(Trx *trx) 
{ 
    DEBUG_PRINT("debug: 聚合算子: open\n");
    if (children_.size() != 1) {
        LOG_WARN("aggregation operator must has one child");
        return RC::INTERNAL;
    }
    RC rc = RC::SUCCESS;
    for (int i = 0; i < children_.size(); i++) {
        if ((rc = children_[i]->open(trx)) != RC::SUCCESS) {
            return rc;
        }
    }
    return rc;
}

RC AggrPhysicalOperator::next() 
{ 
    DEBUG_PRINT("debug: 过滤算子: next\n");
    RC rc = RC::SUCCESS;
    // group by 放置多个在 children_ 中创建分组？
    if (index_ >= children_.size()) {
        return RC::RECORD_EOF;
    }

    PhysicalOperator *oper = children_[index_++].get();
    Tuple *tuple = nullptr;
    // ---begin---
    for (Expression* expr: expressions_) {
        if ( expr->type() == ExprType::AGGREGATION ) {
            AggregationExpr *aggr_expr = static_cast<AggregationExpr *>(expr);
            aggr_expr->begin_aggr();
        } else {
            if (children_.size() <= 1) {  // 没有group by
                return RC::INTERNAL;
            }
        }
    }
    // ---aggr---
    while (RC::SUCCESS == (rc = oper->next())) {
        tuple = oper->current_tuple();
        if (nullptr == tuple) {
            rc = RC::INTERNAL;
            LOG_WARN("failed to get tuple from operator");
            break;
        }
        for (Expression* expr: expressions_) {
            if ( expr->type() == ExprType::AGGREGATION ) {
                AggregationExpr *aggr_expr = static_cast<AggregationExpr *>(expr);
                aggr_expr->aggr_tuple(tuple);
            }
        }
    }
    if (rc != RC::RECORD_EOF) {
        return RC::INTERNAL;
    }
    rc = RC::SUCCESS;
    // 判断tuple
    // ---end---
    std::vector<Value> results;
    std::vector<TupleCellSpec> speces;
    for (Expression* expr: expressions_) {
        Value value;
        if ( expr->type() == ExprType::AGGREGATION ) {  // 聚合
            AggregationExpr *aggr_expr = static_cast<AggregationExpr *>(expr);
            aggr_expr->get_result(value);
            results.emplace_back(value);
            speces.emplace_back(aggr_expr->cell_spec());
        } else if (expr->type() == ExprType::FIELD) {   // 列，group by
            FieldExpr *field_expr = static_cast<FieldExpr *>(expr);
            field_expr->get_value(*tuple, value);
            results.emplace_back(value);
        }
    }
    // over
    aggred_tuple_.set_cells(results);
    aggred_tuple_.set_speces(speces);
    return rc;
}

RC AggrPhysicalOperator::close() 
{ 
    DEBUG_PRINT("debug: 过滤算子: close\n");
    RC rc = RC::SUCCESS;
    for (int i = 0; i < children_.size(); i++) {
        if (children_[i]->close() != RC::SUCCESS) {
            rc = RC::INTERNAL;
        }
    }
    return rc;
}

Tuple *AggrPhysicalOperator::current_tuple() 
{ 
    // 返回聚合后的tuple
    return &aggred_tuple_;
}
