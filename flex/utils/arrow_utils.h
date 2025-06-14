/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef UTILS_ARROW_UTILS_H_
#define UTILS_ARROW_UTILS_H_

#include <arrow/api.h>
#include <arrow/util/value_parsing.h>
#include <memory>
#include "flex/utils/property/types.h"

namespace gs {

// arrow related;
class LDBCTimeStampParser : public arrow::TimestampParser {
 public:
  LDBCTimeStampParser() = default;

  ~LDBCTimeStampParser() override {}

  bool operator()(const char* s, size_t length, arrow::TimeUnit::type out_unit,
                  int64_t* out) const override {
    using seconds_type = std::chrono::duration<arrow::TimestampType::c_type>;

    // We allow the following formats for all units:
    // - "YYYY-MM-DD"
    // - "YYYY-MM-DD[ T]hhZ?"
    // - "YYYY-MM-DD[ T]hh:mmZ?"
    // - "YYYY-MM-DD[ T]hh:mm:ssZ?"
    //
    // We allow the following formats for unit == MILLI, MICRO, or NANO:
    // - "YYYY-MM-DD[ T]hh:mm:ss.s{1,3}Z?"
    //
    // We allow the following formats for unit == MICRO, or NANO:
    // - "YYYY-MM-DD[ T]hh:mm:ss.s{4,6}Z?"
    //
    // We allow the following formats for unit == NANO:
    // - "YYYY-MM-DD[ T]hh:mm:ss.s{7,9}Z?"
    //
    // UTC is always assumed, and the DataType's timezone is ignored.
    //

    if (ARROW_PREDICT_FALSE(length < 10))
      return false;

    seconds_type seconds_since_epoch;
    if (ARROW_PREDICT_FALSE(!arrow::internal::detail::ParseYYYY_MM_DD(
            s, &seconds_since_epoch))) {
      return false;
    }

    if (length == 10) {
      *out =
          arrow::util::CastSecondsToUnit(out_unit, seconds_since_epoch.count());
      return true;
    }

    if (ARROW_PREDICT_FALSE(s[10] != ' ') &&
        ARROW_PREDICT_FALSE(s[10] != 'T')) {
      return false;
    }

    if (s[length - 1] == 'Z') {
      --length;
    }

    // if the back the s is '+0000', we should ignore it
    auto time_zones = std::string_view(s + length - 5, 5);
    if (time_zones == "+0000") {
      length -= 5;
    }

    seconds_type seconds_since_midnight;
    switch (length) {
    case 13:  // YYYY-MM-DD[ T]hh
      if (ARROW_PREDICT_FALSE(!arrow::internal::detail::ParseHH(
              s + 11, &seconds_since_midnight))) {
        return false;
      }
      break;
    case 16:  // YYYY-MM-DD[ T]hh:mm
      if (ARROW_PREDICT_FALSE(!arrow::internal::detail::ParseHH_MM(
              s + 11, &seconds_since_midnight))) {
        return false;
      }
      break;
    case 19:  // YYYY-MM-DD[ T]hh:mm:ss
    case 21:  // YYYY-MM-DD[ T]hh:mm:ss.s
    case 22:  // YYYY-MM-DD[ T]hh:mm:ss.ss
    case 23:  // YYYY-MM-DD[ T]hh:mm:ss.sss
    case 24:  // YYYY-MM-DD[ T]hh:mm:ss.ssss
    case 25:  // YYYY-MM-DD[ T]hh:mm:ss.sssss
    case 26:  // YYYY-MM-DD[ T]hh:mm:ss.ssssss
    case 27:  // YYYY-MM-DD[ T]hh:mm:ss.sssssss
    case 28:  // YYYY-MM-DD[ T]hh:mm:ss.ssssssss
    case 29:  // YYYY-MM-DD[ T]hh:mm:ss.sssssssss
      if (ARROW_PREDICT_FALSE(!arrow::internal::detail::ParseHH_MM_SS(
              s + 11, &seconds_since_midnight))) {
        return false;
      }
      break;
    default:
      LOG(ERROR) << "unsupported length: " << length;
      return false;
    }

    seconds_since_epoch += seconds_since_midnight;

    if (length <= 19) {
      *out =
          arrow::util::CastSecondsToUnit(out_unit, seconds_since_epoch.count());
      return true;
    }

    if (ARROW_PREDICT_FALSE(s[19] != '.')) {
      return false;
    }

    uint32_t subseconds = 0;
    if (ARROW_PREDICT_FALSE(!arrow::internal::detail::ParseSubSeconds(
            s + 20, length - 20, out_unit, &subseconds))) {
      return false;
    }

    *out =
        arrow::util::CastSecondsToUnit(out_unit, seconds_since_epoch.count()) +
        subseconds;
    return true;
  }

  virtual const char* kind() const override { return "LDBC timestamp parser"; }

  virtual const char* format() const override { return "EmptyFormat"; }
};

// convert c++ type to arrow type. support other types likes emptyType, Date
template <typename T>
struct CppTypeToArrowType {};

template <>
struct CppTypeToArrowType<int64_t> {
  using Type = arrow::Int64Type;
  using ArrayType = arrow::Int64Array;
  static std::shared_ptr<arrow::DataType> TypeValue() { return arrow::int64(); }
};

template <>
struct CppTypeToArrowType<int32_t> {
  using Type = arrow::Int32Type;
  using ArrayType = arrow::Int32Array;
  static std::shared_ptr<arrow::DataType> TypeValue() { return arrow::int32(); }
};

template <>
struct CppTypeToArrowType<double> {
  using Type = arrow::DoubleType;
  using ArrayType = arrow::DoubleArray;
  static std::shared_ptr<arrow::DataType> TypeValue() {
    return arrow::float64();
  }
};

template <>
struct CppTypeToArrowType<Date> {
  using Type = arrow::Int64Type;
  using ArrayType = arrow::Int64Array;
  static std::shared_ptr<arrow::DataType> TypeValue() {
    return arrow::timestamp(arrow::TimeUnit::MILLI);
  }
};

template <typename T>
struct CppTypeToPropertyType;

template <>
struct CppTypeToPropertyType<int32_t> {
  static constexpr PropertyType value = PropertyType::kInt32;
};

template <>
struct CppTypeToPropertyType<int64_t> {
  static constexpr PropertyType value = PropertyType::kInt64;
};

template <>
struct CppTypeToPropertyType<double> {
  static constexpr PropertyType value = PropertyType::kDouble;
};

template <>
struct CppTypeToPropertyType<std::string> {
  static constexpr PropertyType value = PropertyType::kString;
};

template <>
struct CppTypeToPropertyType<std::string_view> {
  static constexpr PropertyType value = PropertyType::kString;
};

std::shared_ptr<arrow::DataType> PropertyTypeToArrowType(PropertyType type);
}  // namespace gs

#endif  // UTILS_ARROW_UTILS_H_
