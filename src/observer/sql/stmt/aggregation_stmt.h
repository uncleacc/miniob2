// created by NieYang on 2023/10/19

#pragma once

#include <vector>
#include <unordered_map>
#include "sql/parser/parse_defs.h"
#include "sql/stmt/stmt.h"
#include "sql/expr/expression.h"

class FieldMeta;

class AggrUnit {
public:
    AggrUnit() = default;
    ~AggrUnit() {};

    // 返回聚合函数类型
    AggrFuncType type() const{ 
        return type_; 
    }
    // 设置类型
    void setType(AggrFuncType type) {
        type_ = type; 
    }
    // 添加聚合函数中的列
    RC add_field(Field *&field);
    // 检测列是否存在
    bool exist(Field *&field) {
        if (aggr_field_ == nullptr) {
            return false;
        } else {
            if (0 == strcmp(field->table_name(), aggr_field_->table_name()) && 
                0 == strcmp(field->field_name(), aggr_field_->field_name()) )
            {
                return true;
            } else {
                return false;
            }
        }
        return false;       // 消除警告，不会执行
    }
    // 返回列
    Field* aggr_fields() {
        return aggr_field_;
    }
    // 
    AggregationExpr *get_aggr_expr() {
        return new AggregationExpr(*aggr_field_, type_);
    }

    bool is_star(Field *field);
    bool star() { return star_; }
private:
    AggrFuncType type_;
    Field * aggr_field_ = nullptr;
    bool star_ = false;
};

class AggrStmt
{
public:
    AggrStmt() = default;
    virtual ~AggrStmt() {
        for (AggrUnit * i : aggr_units_)
            delete i;
    }

public:
    const std::vector<AggrUnit *> &aggr_units() const
    {
        return aggr_units_;
    }

    void add_aggr_unit(AggrUnit *aggr_unit) {
        aggr_units_.emplace_back(aggr_unit);
    }

public:
    // 创建聚合单元
    static RC create_aggr_unit(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
                AggrFuncNode &aggr_func_node, AggrUnit *&aggr_unit);

private:
    std::vector<AggrUnit *> aggr_units_;
};
