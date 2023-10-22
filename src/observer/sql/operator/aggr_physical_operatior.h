//
// Created by NieYang on 2023/10/20.
//

#pragma once

#include "sql/operator/physical_operator.h"
#include "storage/record/record_manager.h"
#include "common/rc.h"
#include <memory>

using namespace std;
class Table;

class AggrPhysicalOperator : public PhysicalOperator
{
public:
    AggrPhysicalOperator(std::vector<Expression*> expressions)
        :expressions_(expressions)
    {}

    virtual ~AggrPhysicalOperator()
    {
        for (Expression* ptr : expressions_) {  // 从stmt一直传到这里，删除
            if (ptr != nullptr)
                delete ptr;
        }
    }

    PhysicalOperatorType type() const override
    {
        return PhysicalOperatorType::AGGR_PHYSICAL_T;
    }
    
    RC open(Trx *trx) override;
    RC next() override;
    RC close() override;

    Tuple *current_tuple() override;
private:
    // 聚合
    std::vector<Expression*> expressions_;  // 多个聚合
    int index_ = 0;
    ValueListTuple aggred_tuple_;
};