/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by WangYunlai on 2023/06/28.
//

#include <sstream>
#include <iomanip>
#include <regex>
#include "sql/parser/value.h"
#include "storage/field/field.h"
#include "common/log/log.h"
#include "common/lang/comparator.h"
#include "common/lang/string.h"
#include "value.h"

const char *ATTR_TYPE_NAME[] = {"undefined", "chars", "ints", "floats", "dates", "booleans"};

const char *attr_type_to_string(AttrType type)
{
  if (type >= UNDEFINED && type <= DATES) {     // modify
    return ATTR_TYPE_NAME[type];
  }
  return "unknown";
}
AttrType attr_type_from_string(const char *s)   // modify
{
  for (unsigned int i = 0; i < sizeof(ATTR_TYPE_NAME) / sizeof(ATTR_TYPE_NAME[0]); i++) {
    if (0 == strcmp(ATTR_TYPE_NAME[i], s)) {
      return (AttrType)i;
    }
  }
  return UNDEFINED;
}

Value::Value(int val)
{
  set_int(val);
}

Value::Value(float val)
{
  set_float(val);
}

Value::Value(bool val)
{
  set_boolean(val);
}

Value::Value(date val)  // ---------添加DATE类型--------- //
{
  set_date(val);
}

Value::Value(const char *s, int len /*= 0*/) { set_string(s, len); }

void Value::set_data(char *data, int length)
{
  switch (attr_type_) {
    case CHARS: {
      set_string(data, length);
    } break;
    case INTS: {
      num_value_.int_value_ = *(int *)data;
      length_ = length;
    } break;
    case FLOATS: {
      num_value_.float_value_ = *(float *)data;
      length_ = length;
    } break;
    case DATES: {     // new
      num_value_.date_value_ = *(date *)data;
      length_ = length;
    } break;
    case BOOLEANS: {
      num_value_.bool_value_ = *(int *)data != 0;
      length_ = length;
    } break;
    default: {
      LOG_WARN("unknown data type: %d", attr_type_);
    } break;
  }
}
void Value::set_int(int val)
{
  attr_type_ = INTS;
  num_value_.int_value_ = val;
  length_ = sizeof(val);
}
void Value::set_float(float val)
{
  attr_type_ = FLOATS;
  num_value_.float_value_ = val;
  length_ = sizeof(val);
}
void Value::set_date(date val)    // ---------添加DATE类型--------- //
{
  attr_type_ = DATES;
  num_value_.date_value_ = val;
  length_ = sizeof(val);
}
void Value::set_boolean(bool val)
{
  attr_type_ = BOOLEANS;
  num_value_.bool_value_ = val;
  length_ = sizeof(val);
}

void Value::set_string(const char *s, int len /*= 0*/)
{
  attr_type_ = CHARS;
  if (len > 0) {
    len = strnlen(s, len);
    str_value_.assign(s, len);
  } else {
    str_value_.assign(s);
  }
  length_ = str_value_.length();
}

void Value::set_value(const Value &value)
{
  switch (value.attr_type_) {
    case INTS: {
      set_int(value.get_int());
    } break;
    case FLOATS: {
      set_float(value.get_float());
    } break;
    case CHARS: {
      set_string(value.get_string().c_str());
    } break;
    case BOOLEANS: {
      set_boolean(value.get_boolean());
    } break;
    case DATES: {
      set_date(value.get_date());   // new
    } break;
    case UNDEFINED: {
      ASSERT(false, "got an invalid value type");
    } break;
  }
}

const char *Value::data() const
{
  switch (attr_type_) {
    case CHARS: {
      return str_value_.c_str();
    } break;
    default: {
      return (const char *)&num_value_;
    } break;
  }
}

std::string Value::to_string() const
{
  std::stringstream os;
  switch (attr_type_) {
    case INTS: {
      os << num_value_.int_value_;
    } break;
    case FLOATS: {
      os << common::double_to_str(num_value_.float_value_);
    } break;
    case BOOLEANS: {
      os << num_value_.bool_value_;
    } break;
    case DATES: {     // new
      unsigned year = 0, month = 0, day = 0;
      date t = num_value_.date_value_;

      year = (t >> 16);
      month = (t >> 8) & 0xff;
      day = t & 0xff;

      os << std::setw(4) << std::setfill('0') << year << "-";
      os << std::setw(2) << std::setfill('0') << month << "-";
      os << std::setw(2) << std::setfill('0') << day;
    } break;
    case CHARS: {
      os << str_value_;
    } break;
    default: {
      LOG_WARN("unsupported attr type: %d", attr_type_);
    } break;
  }
  return os.str();
}

int Value::compare(const Value &other) const
{
  DEBUG_PRINT("debug: Value::compare\n");
  // TODO
  // 代码冗余，需要优化
  if (this->attr_type_ == other.attr_type_) {
    switch (this->attr_type_) {
      case INTS: {
        return common::compare_int((void *)&this->num_value_.int_value_, (void *)&other.num_value_.int_value_);
      } break;
      case FLOATS: {
        return common::compare_float((void *)&this->num_value_.float_value_, (void *)&other.num_value_.float_value_);
      } break;
      case DATES: {   // new
        return common::compare_date((void *)&this->num_value_.date_value_, (void *)&other.num_value_.date_value_);
      } break;
      case CHARS: {
        return common::compare_string((void *)this->str_value_.c_str(),
            this->str_value_.length(),
            (void *)other.str_value_.c_str(),
            other.str_value_.length());
      } break;
      case BOOLEANS: {
        return common::compare_int((void *)&this->num_value_.bool_value_, (void *)&other.num_value_.bool_value_);
      }
      default: {
        LOG_WARN("unsupported type: %d", this->attr_type_);
      }
    }
  } else{ // 左右类型不同
    if (this->attr_type_ == INTS) {    // 整数与其它类型比较
      // modify
      int left_data = this->num_value_.int_value_;
      int right_data = 0;

      switch (other.attr_type_)
      {
      case FLOATS:
        right_data = (int)other.num_value_.float_value_;
        break;
      case BOOLEANS:
        right_data = other.get_boolean()?1:0;
        break;
      case CHARS: {
        const char *right_str = other.str_value_.c_str();
        right_data = atoi(right_str);
      } break;
      default:
        LOG_WARN("not supported");
        return -1;
      }

      return common::compare_int((void *)&left_data, (void *)&right_data);
    } else if (this->attr_type_ == FLOATS) {  // 浮点与其它类型比较
      float left_data = this->num_value_.float_value_;
      int right_data = (float)other.num_value_.float_value_;

      switch (other.attr_type_)
      {
      case INTS:
        right_data = (float)other.num_value_.int_value_;
        break;
      case BOOLEANS:
        right_data = other.get_boolean()?1.0:0.0;
        break;
      case CHARS: {
        const char *right_str = other.str_value_.c_str();
        std::cout << "debug: " << right_str << std::endl;
        right_data = atof(right_str);
      }
        break;
      default:
        LOG_WARN("not supported");
        return -1;
      }

        return common::compare_float((void *)&left_data, (void *)&right_data);
    } 
    else if (this->attr_type_ == CHARS) {  // 左边为字符串
      switch (other.attr_type_)
      {
      case INTS: {
        int left_data = atoi(this->get_string().c_str());
        return common::compare_int((void *)&left_data, (void *)&(other.num_value_.int_value_));
      } break;
      case FLOATS: {
        float left_data = atof(this->get_string().c_str());
        return common::compare_float((void *)&left_data, (void *)&(other.num_value_.float_value_));
      } break;
      // TODO 字符串为空==False，非空为TRUE，大于布尔值时转换为01
      // case BOOLEANS: {
      //   bool left_b = this->get_string().empty();
      //   return (left_b != other.get_boolean());
      // } break;
      case CHARS: {
        std::string other_str = other.to_string();
        return common::compare_string((void *)this->str_value_.c_str(),
                this->str_value_.length(),
                (void *)other_str.c_str(),
                other_str.length() );
      } break;
      default:
        LOG_WARN("not supported");
        return -1;
      }
    }
  }
  LOG_WARN("not supported");
  return -1;  // TODO return rc?
}

// new
// 0 为错误
// 1 为正确
int Value::compare_like(const Value &other) const 
{ 
  DEBUG_PRINT("debug: Value::compare_like\n");
  if (this->attr_type_ != CHARS || other.attr_type_ != CHARS) {
    return 0;
  }
  std::string regex_str = other.get_string();
  
  DEBUG_PRINT("debug: left_str %s\n", this->get_string().c_str());
  DEBUG_PRINT("debug: regex_str %s\n", regex_str.c_str());

  if (std::regex_match(this->get_string(), std::regex(regex_str))) {
    DEBUG_PRINT("debug: Value::compare_like: result 1\n");
    return 1;
  } else {
    DEBUG_PRINT("debug: Value::compare_like: result 0\n");
    return 0;
  }
}
// new
int Value::compare_not_like(const Value &other) const 
{
  DEBUG_PRINT("debug: Value::compare_not_like\n");
  return compare_like(other)?0:1;
}

int Value::get_int() const
{
  switch (attr_type_) {
    case CHARS: {
      try {
        return (int)(std::stol(str_value_));
      } catch (std::exception const &ex) {
        LOG_TRACE("failed to convert string to number. s=%s, ex=%s", str_value_.c_str(), ex.what());
        return 0;
      }
    }
    case INTS: {
      return num_value_.int_value_;
    }
    case FLOATS: {
      return (int)(num_value_.float_value_);
    }
    case BOOLEANS: {
      return (int)(num_value_.bool_value_);
    }
    case DATES: {   // new
      return (int)(num_value_.date_value_);
    }
    default: {
      LOG_WARN("unknown data type. type=%d", attr_type_);
      return 0;
    }
  }
  return 0;
}

float Value::get_float() const
{
  switch (attr_type_) {
    case CHARS: {
      try {
        return std::stof(str_value_);
      } catch (std::exception const &ex) {
        LOG_TRACE("failed to convert string to float. s=%s, ex=%s", str_value_.c_str(), ex.what());
        return 0.0;
      }
    } break;
    case INTS: {
      return float(num_value_.int_value_);
    } break;
    case FLOATS: {
      return num_value_.float_value_;
    } break;
    case BOOLEANS: {
      return float(num_value_.bool_value_);
    } break;
    case DATES: {   // new
      LOG_TRACE("failed to convert date to float.");
      return 0.0;
    } break;
    default: {
      LOG_WARN("unknown data type. type=%d", attr_type_);
      return 0;
    }
  }
  return 0;
}

std::string Value::get_string() const
{
  return this->to_string();
}

bool Value::get_boolean() const
{
  switch (attr_type_) {
    case CHARS: {
      try {
        float val = std::stof(str_value_);
        if (val >= EPSILON || val <= -EPSILON) {
          return true;
        }

        int int_val = std::stol(str_value_);
        if (int_val != 0) {
          return true;
        }

        return !str_value_.empty();
      } catch (std::exception const &ex) {
        LOG_TRACE("failed to convert string to float or integer. s=%s, ex=%s", str_value_.c_str(), ex.what());
        return !str_value_.empty();
      }
    } break;
    case INTS: {
      return num_value_.int_value_ != 0;
    } break;
    case FLOATS: {
      float val = num_value_.float_value_;
      return val >= EPSILON || val <= -EPSILON;
    } break;
    case BOOLEANS: {
      return num_value_.bool_value_;
    } break;
    case DATES: {         // new
      return num_value_.date_value_ != 0;
    } break;
    default: {
      LOG_WARN("unknown data type. type=%d", attr_type_);
      return false;
    }
  }
  return false;
}

date Value::get_date() const  // ---------添加DATE类型--------- //
{ 
  switch (attr_type_) {
    case CHARS: {
      LOG_TRACE("failed to convert string to date.");
      return (date)0;
    } break;
    case INTS: {
      LOG_TRACE("failed to convert int to date.");
      return (date)0;
    } break;
    case FLOATS: {
      LOG_TRACE("failed to convert float to date.");
      return (date)0;
    } break;
    case BOOLEANS: {
      LOG_TRACE("failed to convert bool to date.");
      return (date)0;
    } break;
    case DATES: {         // new
      return num_value_.date_value_;
    } break;
    default: {
      LOG_WARN("unknown data type. type=%d", attr_type_);
      return false;
    }
  }
}
