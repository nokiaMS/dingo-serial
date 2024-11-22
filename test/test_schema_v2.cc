// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <byteswap.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <any>
#include <bitset>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "serial/recordV2/record_decoder.h"
#include "serial/recordV2/record_encoder.h"
#include "serial/schemaV2/base_schema.h"

using namespace dingodb::V2;

class SchemaTest : public testing::Test {};

TEST_F(SchemaTest, boolType) {
  {
    /*
     * throw exception.
     */
    auto schema = std::make_shared<DingoSchema<bool>>();
    schema->SetAllowNull(false);

    std::any data;

    // for key.
    Buf buf_key(1024);
    EXPECT_THROW(schema->EncodeKey(data, buf_key), std::runtime_error);

    // for value.
    Buf buf_value(1024);
    EXPECT_THROW(schema->EncodeValue(data, buf_value), std::runtime_error);
  }

  {
    /*
     * allowNull: false; value: true;
     */
    auto schema = std::make_shared<DingoSchema<bool>>();
    schema->SetAllowNull(false);

    std::any data = std::make_any<bool>(true);

    // for key.
    Buf buf_key(1024);
    EXPECT_EQ(schema->GetLengthForKey(), schema->EncodeKey(data, buf_key));
    auto actual_key_data = schema->DecodeKey(buf_key);
    EXPECT_EQ(std::any_cast<bool>(actual_key_data), std::any_cast<bool>(data));

    // for vlaue.
    Buf buf_value(1024);
    EXPECT_EQ(schema->GetLengthForValue(),
              schema->EncodeValue(data, buf_value));
    auto actual_value_data = schema->DecodeValue(buf_value);
    EXPECT_EQ(std::any_cast<bool>(actual_value_data),
              std::any_cast<bool>(data));
  }

  {
    /*
     * allowNull: false; value: false;
     */
    auto schema = std::make_shared<DingoSchema<bool>>();
    schema->SetAllowNull(false);

    std::any data = std::make_any<bool>(false);

    // for key.
    Buf buf_key(1024);
    EXPECT_EQ(schema->GetLengthForKey(), schema->EncodeKey(data, buf_key));

    auto actual_key_data = schema->DecodeKey(buf_key);
    EXPECT_EQ(std::any_cast<bool>(actual_key_data), std::any_cast<bool>(data));

    // for value.
    Buf buf_value(1024);
    EXPECT_EQ(schema->GetLengthForValue(),
              schema->EncodeValue(data, buf_value));

    auto actual_value_data = schema->DecodeValue(buf_value);
    EXPECT_EQ(std::any_cast<bool>(actual_value_data),
              std::any_cast<bool>(data));
  }

  {
    /*
     * allowNull: true; value: true;
     */
    auto schema = std::make_shared<DingoSchema<bool>>();
    schema->SetAllowNull(true);

    std::any data = std::make_any<bool>(true);

    // for key.
    Buf buf_key(1024);
    EXPECT_EQ(schema->GetLengthForKey(), schema->EncodeKey(data, buf_key));

    auto actual_key_data = schema->DecodeKey(buf_key);
    EXPECT_EQ(std::any_cast<bool>(actual_key_data), std::any_cast<bool>(data));

    // for value.
    Buf buf_value(1024);
    EXPECT_EQ(schema->GetLengthForValue(),
              schema->EncodeValue(data, buf_value));

    auto actual_value_data = schema->DecodeValue(buf_value);
    EXPECT_EQ(std::any_cast<bool>(actual_value_data),
              std::any_cast<bool>(data));
  }

  {
    /*
     * allowNull: true; value: false;
     */
    auto schema = std::make_shared<DingoSchema<bool>>();
    schema->SetAllowNull(true);

    std::any data = std::make_any<bool>(false);

    // for key.
    Buf buf_key(1024);
    EXPECT_EQ(schema->GetLengthForKey(), schema->EncodeKey(data, buf_key));

    auto actual_key_data = schema->DecodeKey(buf_key);
    EXPECT_EQ(std::any_cast<bool>(actual_key_data), std::any_cast<bool>(data));

    // for value.
    Buf buf_value(1024);
    EXPECT_EQ(schema->GetLengthForValue(),
              schema->EncodeValue(data, buf_value));

    auto actual_value_data = schema->DecodeValue(buf_value);
    EXPECT_EQ(std::any_cast<bool>(actual_value_data),
              std::any_cast<bool>(data));
  }

  {
    /*
     * allowNull: true; value: null;
     */
    auto schema = std::make_shared<DingoSchema<bool>>();
    schema->SetAllowNull(true);

    std::any data;

    // for key.
    Buf buf_key(1024);

    // length should be 2.
    EXPECT_EQ(schema->GetLengthForKey(), schema->EncodeKey(data, buf_key));

    auto actual_key_data = schema->DecodeKey(buf_key);
    EXPECT_EQ(actual_key_data.has_value(), false);  // does not contain value.

    // for value.
    Buf buf_value(1024);

    // return 0 as the value is null.
    // when null value, the DecodeValue should not be called,
    // so we do not test DecodeValue here but only DecodeKey.
    EXPECT_EQ(0, schema->EncodeValue(data, buf_value));
  }
}

TEST_F(SchemaTest, boolListType) {
  {
    /*
     * Throw exception.
     */
    auto schema = std::make_shared<DingoSchema<std::vector<bool>>>();
    schema->SetAllowNull(false);

    Buf buf(1024);
    std::any data;
    EXPECT_THROW(schema->EncodeValue(data, buf), std::runtime_error);
  }

  {
    /*
     * allowNull:false; with values.
     */
    auto schema = std::make_shared<DingoSchema<std::vector<bool>>>();
    schema->SetAllowNull(false);

    Buf buf(1024);
    std::vector<bool> data = {true, false, true};
    schema->EncodeValue(std::make_any<std::vector<bool>>(data), buf);

    auto decode_value = schema->DecodeValue(buf);
    auto actual_data = std::any_cast<std::vector<bool>>(decode_value);
    EXPECT_EQ(actual_data.size(), data.size());
    for (uint32_t i = 0; i < actual_data.size(); ++i) {
      EXPECT_EQ(actual_data[i], data[i]);
    }
  }

  {
    /*
     * allowNull: true; with value {}.
     */
    auto schema = std::make_shared<DingoSchema<std::vector<bool>>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    std::vector<bool> data = {};
    int size = schema->EncodeValue(std::make_any<std::vector<bool>>(data), buf);
    EXPECT_EQ(size, 4);
  }

  {
    /*
     * allowNull: true; with no value.
     */
    auto schema = std::make_shared<DingoSchema<std::vector<bool>>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    int size = schema->EncodeValue(std::any(), buf);
    EXPECT_EQ(size, 0);
  }

  {
    /*
     * allowNull: true; with two bool list; test skip.
     */
    auto schema = std::make_shared<DingoSchema<std::vector<bool>>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    std::vector<bool> data1 = {true, false, true};
    schema->EncodeValue(std::make_any<std::vector<bool>>(data1), buf);
    std::vector<bool> data2 = {false, true, false};
    schema->EncodeValue(std::make_any<std::vector<bool>>(data2), buf);

    int size = schema->SkipValue(buf);
    EXPECT_EQ(7, size);

    auto decode_value = schema->DecodeValue(buf);
    auto actual_data = std::any_cast<std::vector<bool>>(decode_value);
    EXPECT_EQ(actual_data.size(), data2.size());
    for (uint32_t i = 0; i < actual_data.size(); ++i) {
      EXPECT_EQ(actual_data[i], data2[i]);
    }
  }
}

TEST_F(SchemaTest, intType) {
  {
    /*
     * Throw exception.
     */
    auto schema = std::make_shared<DingoSchema<int32_t>>();
    schema->SetAllowNull(false);

    Buf buf(1024);
    std::any data;
    EXPECT_THROW(schema->EncodeKey(data, buf), std::runtime_error);
  }

  {
    /*
     * allowNull: false; with value.
     */
    auto schema = std::make_shared<DingoSchema<int32_t>>();
    schema->SetAllowNull(false);

    std::any data = std::make_any<int32_t>(101);

    Buf buf_key(1024);
    EXPECT_EQ(schema->GetLengthForKey(), schema->EncodeKey(data, buf_key));
    auto actual_data_key = schema->DecodeKey(buf_key);
    EXPECT_EQ(std::any_cast<int32_t>(actual_data_key),
              std::any_cast<int32_t>(data));

    Buf buf_value(1024);
    EXPECT_EQ(schema->GetLengthForValue(),
              schema->EncodeValue(data, buf_value));
    auto actual_data_value = schema->DecodeValue(buf_value);
    EXPECT_EQ(std::any_cast<int32_t>(actual_data_value),
              std::any_cast<int32_t>(data));
  }

  {
    /*
     * allowNull: true; with value.
     */
    auto schema = std::make_shared<DingoSchema<int32_t>>();
    schema->SetAllowNull(true);

    std::any data = std::make_any<int32_t>(101);

    Buf buf_key(1024);
    EXPECT_EQ(schema->GetLengthForKey(), schema->EncodeKey(data, buf_key));
    auto actual_data_key = schema->DecodeKey(buf_key);
    EXPECT_EQ(std::any_cast<int32_t>(actual_data_key),
              std::any_cast<int32_t>(data));

    Buf buf_value(1024);
    std::any data1 = std::make_any<int32_t>(102);
    EXPECT_EQ(schema->GetLengthForValue(),
              schema->EncodeValue(data, buf_value));
    EXPECT_EQ(schema->GetLengthForValue(),
              schema->EncodeValue(data1, buf_value));

    // skip the first value.
    int size = schema->SkipValue(buf_value);
    EXPECT_EQ(size, 4);

    // read the second value.
    auto actual_data_value = schema->DecodeValue(buf_value);
    EXPECT_EQ(std::any_cast<int32_t>(actual_data_value),
              std::any_cast<int32_t>(data1));
  }

  {
    /*
     * allowNull: true; with null value.
     */
    auto schema = std::make_shared<DingoSchema<int32_t>>();
    schema->SetAllowNull(true);

    std::any data;

    Buf buf_key(1024);
    EXPECT_EQ(schema->GetLengthForKey(), schema->EncodeKey(data, buf_key));
    auto actual_data_key = schema->DecodeKey(buf_key);
    EXPECT_EQ(false, actual_data_key.has_value());

    Buf buf_value(1024);
    // encoding null with return 0 len.
    // null will not be encoded into buf, so we never call DecodeValue for null.
    EXPECT_EQ(0, schema->EncodeValue(data, buf_value));
  }
}

TEST_F(SchemaTest, intListType) {
  {
    /*
     * Throw exception.
     */
    auto schema = std::make_shared<DingoSchema<std::vector<int32_t>>>();
    schema->SetAllowNull(false);

    Buf buf(1024);
    std::any data;
    EXPECT_THROW(schema->EncodeValue(data, buf), std::runtime_error);
  }

  {
    /*
     * allowNull: false; with values.
     */
    auto schema = std::make_shared<DingoSchema<std::vector<int32_t>>>();
    schema->SetAllowNull(false);

    std::vector<int32_t> data1 = {1, 2, 3};
    std::vector<int32_t> data2 = {4, 5, 6};

    Buf buf(1024);
    schema->EncodeValue(std::make_any<std::vector<int32_t>>(data1), buf);
    schema->EncodeValue(std::make_any<std::vector<int32_t>>(data2), buf);

    // skip the first value.
    int size = schema->SkipValue(buf);
    EXPECT_EQ(16, size);

    // decode the second value.
    auto decode_value = schema->DecodeValue(buf);
    auto actual_data = std::any_cast<std::vector<int32_t>>(decode_value);
    EXPECT_EQ(actual_data.size(), data2.size());
    for (uint32_t i = 0; i < actual_data.size(); ++i) {
      EXPECT_EQ(actual_data[i], data2[i]);
    }
  }

  {
    /*
     * allowNull: true; with empty value.
     */
    auto schema = std::make_shared<DingoSchema<std::vector<int32_t>>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    std::vector<int32_t> data = {};

    // null value will never be encoded in value, so we do not test decoding
    // null value here.
    int size =
        schema->EncodeValue(std::make_any<std::vector<int32_t>>(data), buf);
    EXPECT_EQ(4, size);
  }

  {
    /*
     * allowNull: true; value: null.
     */
    auto schema = std::make_shared<DingoSchema<std::vector<int32_t>>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    int size = schema->EncodeValue(std::any(), buf);
    EXPECT_EQ(0, size);
  }

  {
    auto schema = std::make_shared<DingoSchema<std::vector<int32_t>>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    std::vector<int32_t> data = {3, 6, 9};
    schema->EncodeValue(std::make_any<std::vector<int32_t>>(data), buf);

    auto decode_value = schema->DecodeValue(buf);
    auto actual_data = std::any_cast<std::vector<int32_t>>(decode_value);
    EXPECT_EQ(actual_data.size(), data.size());
    for (uint32_t i = 0; i < actual_data.size(); ++i) {
      EXPECT_EQ(actual_data[i], data[i]);
    }
  }
}

TEST_F(SchemaTest, longType) {
  {
    /*
     * Throw exception.
     */
    auto schema = std::make_shared<DingoSchema<int64_t>>();
    schema->SetAllowNull(false);

    Buf buf(1024);
    std::any data;
    EXPECT_THROW(schema->EncodeKey(data, buf), std::runtime_error);
  }

  {
    /*
     * allowNull: false; with value.
     */
    auto schema = std::make_shared<DingoSchema<int64_t>>();
    schema->SetAllowNull(false);

    std::any data1 = std::make_any<int64_t>(101);
    std::any data2 = std::make_any<int64_t>(102);

    // for key.
    Buf buf_key(1024);
    EXPECT_EQ(schema->GetLengthForKey(), schema->EncodeKey(data1, buf_key));
    EXPECT_EQ(schema->GetLengthForKey(), schema->EncodeKey(data2, buf_key));

    int size = schema->SkipKey(buf_key);
    EXPECT_EQ(9, size);

    auto actual_data_key = schema->DecodeKey(buf_key);
    EXPECT_EQ(std::any_cast<int64_t>(actual_data_key),
              std::any_cast<int64_t>(data2));

    // for value.
    Buf buf_value(1024);
    EXPECT_EQ(schema->GetLengthForValue(),
              schema->EncodeValue(data1, buf_value));
    EXPECT_EQ(schema->GetLengthForValue(),
              schema->EncodeValue(data2, buf_value));

    size = schema->SkipValue(buf_value);
    EXPECT_EQ(8, size);

    auto actual_data_value = schema->DecodeValue(buf_value);
    EXPECT_EQ(std::any_cast<int64_t>(actual_data_value),
              std::any_cast<int64_t>(data2));
  }

  {
    /*
     * allowNull: true; with value.
     */
    auto schema = std::make_shared<DingoSchema<int64_t>>();
    schema->SetAllowNull(true);

    std::any data1 = std::make_any<int64_t>(101);
    std::any data2 = std::make_any<int64_t>(102);

    // for key.
    Buf buf_key(1024);
    EXPECT_EQ(schema->GetLengthForKey(), schema->EncodeKey(data1, buf_key));
    EXPECT_EQ(schema->GetLengthForKey(), schema->EncodeKey(data2, buf_key));

    int size = schema->SkipKey(buf_key);
    EXPECT_EQ(9, size);

    auto actual_data_key = schema->DecodeKey(buf_key);
    EXPECT_EQ(std::any_cast<int64_t>(actual_data_key),
              std::any_cast<int64_t>(data2));

    // for value.
    Buf buf_value(1024);
    EXPECT_EQ(schema->GetLengthForValue(),
              schema->EncodeValue(data1, buf_value));
    EXPECT_EQ(schema->GetLengthForValue(),
              schema->EncodeValue(data2, buf_value));

    size = schema->SkipValue(buf_value);
    EXPECT_EQ(8, size);

    auto actual_data_value = schema->DecodeValue(buf_value);
    EXPECT_EQ(std::any_cast<int64_t>(actual_data_value),
              std::any_cast<int64_t>(data2));
  }

  {
    /*
     * allowNull: true; with null.
     */
    auto schema = std::make_shared<DingoSchema<int64_t>>();
    schema->SetAllowNull(true);

    std::any data;

    Buf buf_key(1024);
    EXPECT_EQ(schema->GetLengthForKey(), schema->EncodeKey(data, buf_key));
    auto actual_data = schema->DecodeKey(buf_key);
    EXPECT_EQ(false, actual_data.has_value());

    Buf buf_value(1024);
    // DecodeValue only be called when value is not null, so we do check it here
    // but only EncodeValue.
    EXPECT_EQ(0, schema->EncodeValue(data, buf_value));
  }
}

TEST_F(SchemaTest, longListType) {
  {
    /*
     * Throw exception.
     */
    auto schema = std::make_shared<DingoSchema<std::vector<int64_t>>>();
    schema->SetAllowNull(false);

    Buf buf(1024);
    std::any data;
    EXPECT_THROW(schema->EncodeValue(data, buf), std::runtime_error);
  }

  {
    /*
     * allowNull: false; with values;
     */
    auto schema = std::make_shared<DingoSchema<std::vector<int64_t>>>();
    schema->SetAllowNull(false);

    Buf buf(1024);

    std::vector<int64_t> data1 = {1, 2, 3};
    std::vector<int64_t> data2 = {4, 5, 6};

    int size =
        schema->EncodeValue(std::make_any<std::vector<int64_t>>(data1), buf);
    EXPECT_EQ(28, size);
    size = schema->EncodeValue(std::make_any<std::vector<int64_t>>(data2), buf);
    EXPECT_EQ(28, size);

    size = schema->SkipValue(buf);
    EXPECT_EQ(28, size);

    auto decode_value = schema->DecodeValue(buf);
    auto actual_data = std::any_cast<std::vector<int64_t>>(decode_value);
    EXPECT_EQ(actual_data.size(), data2.size());
    for (uint32_t i = 0; i < actual_data.size(); ++i) {
      EXPECT_EQ(actual_data[i], data2[i]);
    }
  }

  {
    /*
     * allowNull: true; with null value.
     */
    auto schema = std::make_shared<DingoSchema<std::vector<int64_t>>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    std::vector<int64_t> data = {};
    int size =
        schema->EncodeValue(std::make_any<std::vector<int64_t>>(data), buf);
    EXPECT_EQ(4, size);
  }

  {
    /*
     * allowNull: true; with null value.
     */
    auto schema = std::make_shared<DingoSchema<std::vector<int64_t>>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    int size = schema->EncodeValue(std::any(), buf);
    EXPECT_EQ(0, size);  // Do not need to check DecodeValue when null.
  }

  {
    /*
     * allowNull: true; with values.
     */
    auto schema = std::make_shared<DingoSchema<std::vector<int64_t>>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    std::vector<int64_t> data = {3, 6, 9};
    int size =
        schema->EncodeValue(std::make_any<std::vector<int64_t>>(data), buf);
    EXPECT_EQ(28, size);

    auto decode_value = schema->DecodeValue(buf);
    auto actual_data = std::any_cast<std::vector<int64_t>>(decode_value);
    EXPECT_EQ(actual_data.size(), data.size());
    for (uint32_t i = 0; i < actual_data.size(); ++i) {
      EXPECT_EQ(actual_data[i], data[i]);
    }
  }
}

TEST_F(SchemaTest, floatType) {
  {
    /*
     * Throw exception.
     */
    auto schema = std::make_shared<DingoSchema<float>>();
    schema->SetAllowNull(false);

    Buf buf(1024);
    std::any data;
    EXPECT_THROW(schema->EncodeKey(data, buf), std::runtime_error);
  }

  {
    /*
     * allowNull: false; with value.
     */
    auto schema = std::make_shared<DingoSchema<float>>();
    schema->SetAllowNull(false);

    std::any data1 = std::make_any<float>(101.12);
    std::any data2 = std::make_any<float>(102.13);

    // for key.
    Buf buf_key(1024);
    EXPECT_EQ(schema->GetLengthForKey(), schema->EncodeKey(data1, buf_key));
    EXPECT_EQ(schema->GetLengthForKey(), schema->EncodeKey(data2, buf_key));

    int size = schema->SkipKey(buf_key);
    EXPECT_EQ(5, size);

    auto actual_data_key = schema->DecodeKey(buf_key);
    EXPECT_EQ(std::any_cast<float>(actual_data_key),
              std::any_cast<float>(data2));

    // for value.
    Buf buf_value(1024);
    EXPECT_EQ(schema->GetLengthForValue(),
              schema->EncodeValue(data1, buf_value));
    EXPECT_EQ(schema->GetLengthForValue(),
              schema->EncodeValue(data2, buf_value));

    size = schema->SkipValue(buf_value);
    EXPECT_EQ(4, size);

    auto actual_data_value = schema->DecodeValue(buf_value);
    EXPECT_EQ(std::any_cast<float>(actual_data_value),
              std::any_cast<float>(data2));
  }

  {
    /*
     * allowNull: true; with value.
     */
    auto schema = std::make_shared<DingoSchema<float>>();
    schema->SetAllowNull(true);

    std::any data1 = std::make_any<float>(101.2132);
    std::any data2 = std::make_any<float>(102.2234);

    // for key.
    Buf buf_key(1024);
    EXPECT_EQ(schema->GetLengthForKey(), schema->EncodeKey(data1, buf_key));
    EXPECT_EQ(schema->GetLengthForKey(), schema->EncodeKey(data2, buf_key));

    int size = schema->SkipKey(buf_key);
    EXPECT_EQ(5, size);

    auto actual_data_key = schema->DecodeKey(buf_key);
    EXPECT_EQ(std::any_cast<float>(actual_data_key),
              std::any_cast<float>(data2));

    // for value.
    Buf buf_value(1024);
    EXPECT_EQ(schema->GetLengthForValue(),
              schema->EncodeValue(data1, buf_value));
    EXPECT_EQ(schema->GetLengthForValue(),
              schema->EncodeValue(data2, buf_value));

    size = schema->SkipValue(buf_value);
    EXPECT_EQ(4, size);

    auto actual_data_value = schema->DecodeValue(buf_value);
    EXPECT_EQ(std::any_cast<float>(actual_data_value),
              std::any_cast<float>(data2));
  }

  {
    /*
     * allowNull: true; with null value.
     */
    auto schema = std::make_shared<DingoSchema<float>>();
    schema->SetAllowNull(true);

    std::any data;

    Buf buf_key(1024);
    EXPECT_EQ(5, schema->EncodeKey(data, buf_key));
    auto actual_data_key = schema->DecodeKey(buf_key);
    EXPECT_EQ(false, actual_data_key.has_value());

    Buf buf_value(1024);
    // No need to test DecodeValue.
    EXPECT_EQ(0, schema->EncodeValue(data, buf_value));
  }
}

TEST_F(SchemaTest, floatListType) {
  {
    /*
     * Throw exception.
     */
    auto schema = std::make_shared<DingoSchema<std::vector<float>>>();
    schema->SetAllowNull(false);

    Buf buf(1024);
    std::any data;
    EXPECT_THROW(schema->EncodeValue(data, buf), std::runtime_error);
  }

  {
    /*
     * allowNull: false; with values.
     */
    auto schema = std::make_shared<DingoSchema<std::vector<float>>>();
    schema->SetAllowNull(false);

    std::vector<float> data1 = {1.1, 2.2, 3.3};
    std::vector<float> data2 = {4.4, 5.5, 6.6};

    Buf buf(1024);
    int size =
        schema->EncodeValue(std::make_any<std::vector<float>>(data1), buf);
    EXPECT_EQ(16, size);
    size = schema->EncodeValue(std::make_any<std::vector<float>>(data2), buf);
    EXPECT_EQ(16, size);

    size = schema->SkipValue(buf);
    EXPECT_EQ(16, size);

    auto decode_value = schema->DecodeValue(buf);
    auto actual_data = std::any_cast<std::vector<float>>(decode_value);
    EXPECT_EQ(actual_data.size(), data2.size());
    for (uint32_t i = 0; i < actual_data.size(); ++i) {
      EXPECT_EQ(actual_data[i], data2[i]);
    }
  }

  {
    /*
     * allowNull: true; with null value.
     */
    auto schema = std::make_shared<DingoSchema<std::vector<float>>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    std::vector<float> data = {};
    int size =
        schema->EncodeValue(std::make_any<std::vector<float>>(data), buf);
    EXPECT_EQ(4, size);
  }

  {
    /*
     * allowNull: true; with null value.
     */
    auto schema = std::make_shared<DingoSchema<std::vector<float>>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    int size = schema->EncodeValue(std::any(), buf);
    EXPECT_EQ(0, size);
  }

  {
    /*
     * allowNull: true; with values.
     */
    auto schema = std::make_shared<DingoSchema<std::vector<float>>>();
    schema->SetAllowNull(true);

    std::vector<float> data1 = {1.1, 2.2, 3.3};
    std::vector<float> data2 = {4.4, 5.5, 6.6};

    Buf buf(1024);
    int size =
        schema->EncodeValue(std::make_any<std::vector<float>>(data1), buf);
    EXPECT_EQ(16, size);
    size = schema->EncodeValue(std::make_any<std::vector<float>>(data2), buf);
    EXPECT_EQ(16, size);

    size = schema->SkipValue(buf);
    EXPECT_EQ(16, size);

    auto decode_value = schema->DecodeValue(buf);
    auto actual_data = std::any_cast<std::vector<float>>(decode_value);
    EXPECT_EQ(actual_data.size(), data2.size());
    for (uint32_t i = 0; i < actual_data.size(); ++i) {
      EXPECT_EQ(actual_data[i], data2[i]);
    }
  }
}

TEST_F(SchemaTest, doubleType) {
  {
    /*
     * Throw exception.
     */
    auto schema = std::make_shared<DingoSchema<double>>();
    schema->SetAllowNull(false);

    Buf buf(1024);
    std::any data;
    EXPECT_THROW(schema->EncodeKey(data, buf), std::runtime_error);
  }

  {
    /*
     * allowNull: false; with value.
     */
    auto schema = std::make_shared<DingoSchema<double>>();
    schema->SetAllowNull(false);

    std::any data1 = std::make_any<double>(101.12);
    std::any data2 = std::make_any<double>(102.34);

    Buf buf_key(1024);
    EXPECT_EQ(schema->GetLengthForKey(), schema->EncodeKey(data1, buf_key));
    EXPECT_EQ(schema->GetLengthForKey(), schema->EncodeKey(data2, buf_key));

    int size = schema->SkipKey(buf_key);
    EXPECT_EQ(9, size);

    auto actual_data_key = schema->DecodeKey(buf_key);
    EXPECT_EQ(std::any_cast<double>(actual_data_key),
              std::any_cast<double>(data2));

    Buf buf_value(1024);
    EXPECT_EQ(schema->GetLengthForValue(),
              schema->EncodeValue(data1, buf_value));
    EXPECT_EQ(schema->GetLengthForValue(),
              schema->EncodeValue(data2, buf_value));

    size = schema->SkipValue(buf_value);
    EXPECT_EQ(8, size);

    auto actual_data_value = schema->DecodeValue(buf_value);
    EXPECT_EQ(std::any_cast<double>(actual_data_value),
              std::any_cast<double>(data2));
  }

  {
    /*
     * allowNull: true; with value.
     */
    auto schema = std::make_shared<DingoSchema<double>>();
    schema->SetAllowNull(true);

    std::any data1 = std::make_any<double>(101.12);
    std::any data2 = std::make_any<double>(102.34);

    Buf buf_key(1024);
    EXPECT_EQ(schema->GetLengthForKey(), schema->EncodeKey(data1, buf_key));
    EXPECT_EQ(schema->GetLengthForKey(), schema->EncodeKey(data2, buf_key));

    int size = schema->SkipKey(buf_key);
    EXPECT_EQ(9, size);

    auto actual_data_key = schema->DecodeKey(buf_key);
    EXPECT_EQ(std::any_cast<double>(actual_data_key),
              std::any_cast<double>(data2));

    Buf buf_value(1024);
    EXPECT_EQ(schema->GetLengthForValue(),
              schema->EncodeValue(data1, buf_value));
    EXPECT_EQ(schema->GetLengthForValue(),
              schema->EncodeValue(data2, buf_value));

    size = schema->SkipValue(buf_value);
    EXPECT_EQ(8, size);

    auto actual_data_value = schema->DecodeValue(buf_value);
    EXPECT_EQ(std::any_cast<double>(actual_data_value),
              std::any_cast<double>(data2));
  }

  {
    /*
     * allowNull: true; with null value.
     */
    auto schema = std::make_shared<DingoSchema<double>>();
    schema->SetAllowNull(true);

    std::any data;

    Buf buf_key(1024);
    EXPECT_EQ(9, schema->EncodeKey(data, buf_key));
    auto actual_data = schema->DecodeKey(buf_key);
    EXPECT_EQ(0x0, actual_data.has_value());

    Buf buf_value(1024);
    // not need to check DecodeValue for null.
    EXPECT_EQ(0, schema->EncodeValue(data, buf_value));
  }
}

TEST_F(SchemaTest, doubleListType) {
  {
    /*
     * Throw exception.
     */
    auto schema = std::make_shared<DingoSchema<std::vector<double>>>();
    schema->SetAllowNull(false);

    Buf buf(1024);
    std::any data;
    EXPECT_THROW(schema->EncodeValue(data, buf), std::runtime_error);
  }

  {
    /*
     * allowNull: false; with values.
     */
    auto schema = std::make_shared<DingoSchema<std::vector<double>>>();
    schema->SetAllowNull(false);

    Buf buf(1024);
    std::vector<double> data1 = {1.11, 2.22, 3.33};
    std::vector<double> data2 = {4.44, 5.55, 6.66};
    schema->EncodeValue(std::make_any<std::vector<double>>(data1), buf);
    schema->EncodeValue(std::make_any<std::vector<double>>(data2), buf);

    int size = schema->SkipValue(buf);
    EXPECT_EQ(28, size);

    auto decode_value = schema->DecodeValue(buf);
    auto actual_data = std::any_cast<std::vector<double>>(decode_value);
    EXPECT_EQ(actual_data.size(), data2.size());
    for (uint32_t i = 0; i < actual_data.size(); ++i) {
      EXPECT_EQ(actual_data[i], data2[i]);
    }
  }

  {
    /*
     * allowNull: true; with null value.
     */
    auto schema = std::make_shared<DingoSchema<std::vector<double>>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    std::vector<double> data = {};
    // Need not to test DecodeValue when value is null.
    int size =
        schema->EncodeValue(std::make_any<std::vector<double>>(data), buf);
    EXPECT_EQ(4, size);
  }

  {
    auto schema = std::make_shared<DingoSchema<std::vector<double>>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    int size = schema->EncodeValue(std::any(), buf);
    EXPECT_EQ(0, size);
  }

  {
    /*
     * allowNull: true; with values.
     */
    auto schema = std::make_shared<DingoSchema<std::vector<double>>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    std::vector<double> data1 = {1.11, 2.22, 3.33};
    std::vector<double> data2 = {4.44, 5.55, 6.66};
    schema->EncodeValue(std::make_any<std::vector<double>>(data1), buf);
    schema->EncodeValue(std::make_any<std::vector<double>>(data2), buf);

    int size = schema->SkipValue(buf);
    EXPECT_EQ(28, size);

    auto decode_value = schema->DecodeValue(buf);
    auto actual_data = std::any_cast<std::vector<double>>(decode_value);
    EXPECT_EQ(actual_data.size(), data2.size());
    for (uint32_t i = 0; i < actual_data.size(); ++i) {
      EXPECT_EQ(actual_data[i], data2[i]);
    }
  }
}

TEST_F(SchemaTest, stringType) {
  {
    /*
     * Throw exception.
     */
    auto schema = std::make_shared<DingoSchema<std::string>>();
    schema->SetAllowNull(false);

    Buf buf(1024);
    std::any data;
    EXPECT_THROW(schema->EncodeKey(data, buf), std::runtime_error);
  }

  {
    auto schema = std::make_shared<DingoSchema<std::string>>();
    schema->SetAllowNull(false);

    std::any data1 = std::make_any<std::string>("hello");
    std::any data2 = std::make_any<std::string>("world");

    // for key.
    Buf buf_key(1024);
    EXPECT_EQ(10, schema->EncodeKey(data1, buf_key));  // with null flag in key.
    EXPECT_EQ(10, schema->EncodeKey(data2, buf_key));  // with null flag in key.

    int size = schema->SkipKey(buf_key);
    EXPECT_EQ(10, size);  // null flag | 8 bytes | 250

    auto actual_data_key = schema->DecodeKey(buf_key);
    EXPECT_EQ(std::any_cast<std::string>(actual_data_key),
              std::any_cast<std::string>(data2));

    // for value.
    Buf buf_value(1024);
    EXPECT_EQ(9, schema->EncodeValue(
                     data1, buf_value));  // with no null flag in value.
    EXPECT_EQ(9, schema->EncodeValue(
                     data2, buf_value));  // with no null flag in value.

    size = schema->SkipValue(buf_value);
    EXPECT_EQ(9, size);  // len | 'hello'

    auto actual_data_value = schema->DecodeValue(buf_value);
    EXPECT_EQ(std::any_cast<std::string>(actual_data_value),
              std::any_cast<std::string>(data2));
  }

  {
    /*
     * allowNull: false; with value.
     */
    auto schema = std::make_shared<DingoSchema<std::string>>();
    schema->SetAllowNull(false);

    std::any data1 = std::make_any<std::string>("hello world");
    std::any data2 = std::make_any<std::string>("abcde edckf");

    // for key.
    Buf buf_key(1024);
    EXPECT_EQ(19, schema->EncodeKey(data1, buf_key));
    EXPECT_EQ(19, schema->EncodeKey(data2, buf_key));

    int size = schema->SkipKey(buf_key);
    EXPECT_EQ(19, size);

    auto actual_data_key = schema->DecodeKey(buf_key);
    EXPECT_EQ(std::any_cast<std::string>(actual_data_key),
              std::any_cast<std::string>(data2));

    // for value.
    Buf buf_value(1024);
    EXPECT_EQ(15, schema->EncodeValue(data1, buf_value));  // len | string data.
    EXPECT_EQ(15, schema->EncodeValue(data2, buf_value));

    size = schema->SkipValue(buf_value);
    EXPECT_EQ(15, size);

    auto actual_data_value = schema->DecodeValue(buf_value);
    EXPECT_EQ(std::any_cast<std::string>(actual_data_value),
              std::any_cast<std::string>(data2));
  }

  {
    auto schema = std::make_shared<DingoSchema<std::string>>();
    schema->SetAllowNull(true);

    std::any data1 = std::make_any<std::string>("hello");
    std::any data2 = std::make_any<std::string>("world");

    // for key.
    Buf buf_key(1024);
    EXPECT_EQ(10, schema->EncodeKey(data1, buf_key));  // with null flag in key.
    EXPECT_EQ(10, schema->EncodeKey(data2, buf_key));  // with null flag in key.

    int size = schema->SkipKey(buf_key);
    EXPECT_EQ(10, size);  // null flag | 8 bytes | 250

    auto actual_data_key = schema->DecodeKey(buf_key);
    EXPECT_EQ(std::any_cast<std::string>(actual_data_key),
              std::any_cast<std::string>(data2));

    // for value.
    Buf buf_value(1024);
    EXPECT_EQ(9, schema->EncodeValue(
                     data1, buf_value));  // with no null flag in value.
    EXPECT_EQ(9, schema->EncodeValue(
                     data2, buf_value));  // with no null flag in value.

    size = schema->SkipValue(buf_value);
    EXPECT_EQ(9, size);  // len | 'hello'

    auto actual_data_value = schema->DecodeValue(buf_value);
    EXPECT_EQ(std::any_cast<std::string>(actual_data_value),
              std::any_cast<std::string>(data2));
  }

  {
    auto schema = std::make_shared<DingoSchema<std::string>>();
    schema->SetAllowNull(true);

    std::any data1 = std::make_any<std::string>("hello world");
    std::any data2 = std::make_any<std::string>("abcde edckf");

    // for key.
    Buf buf_key(1024);
    EXPECT_EQ(19, schema->EncodeKey(data1, buf_key));
    EXPECT_EQ(19, schema->EncodeKey(data2, buf_key));

    int size = schema->SkipKey(buf_key);
    EXPECT_EQ(19, size);

    auto actual_data_key = schema->DecodeKey(buf_key);
    EXPECT_EQ(std::any_cast<std::string>(actual_data_key),
              std::any_cast<std::string>(data2));

    // for value.
    Buf buf_value(1024);
    EXPECT_EQ(15, schema->EncodeValue(data1, buf_value));  // len | string data.
    EXPECT_EQ(15, schema->EncodeValue(data2, buf_value));

    size = schema->SkipValue(buf_value);
    EXPECT_EQ(15, size);

    auto actual_data_value = schema->DecodeValue(buf_value);
    EXPECT_EQ(std::any_cast<std::string>(actual_data_value),
              std::any_cast<std::string>(data2));
  }

  {
    auto schema = std::make_shared<DingoSchema<std::string>>();
    schema->SetAllowNull(true);

    std::any data;

    // for key.
    Buf buf_key(1024);
    EXPECT_EQ(1, schema->EncodeKey(data, buf_key));
    auto actual_data_key = schema->DecodeKey(buf_key);
    EXPECT_EQ(false, actual_data_key.has_value());

    // for value.
    Buf buf_value(1024);
    EXPECT_EQ(0, schema->EncodeValue(data, buf_value));
  }

  // encode/decode value
  {
    /*
     * Throw exception.
     */
    auto schema = std::make_shared<DingoSchema<std::string>>();
    schema->SetAllowNull(false);

    Buf buf(1024);
    std::any data;
    EXPECT_THROW(schema->EncodeValue(data, buf), std::runtime_error);
  }
}

TEST_F(SchemaTest, stringListType) {
  {
    /*
     * Throw exception.
     */
    auto schema = std::make_shared<DingoSchema<std::vector<std::string>>>();
    schema->SetAllowNull(false);

    Buf buf(1024);
    std::any data;
    EXPECT_THROW(schema->EncodeValue(data, buf), std::runtime_error);
  }

  {
    /*
     * allowNull: false; with values.
     */
    auto schema = std::make_shared<DingoSchema<std::vector<std::string>>>();
    schema->SetAllowNull(false);

    Buf buf(1024);
    std::vector<std::string> data1 = {"hello", "world", "nihao"};
    std::vector<std::string> data2 = {"12345", "6789a", "bcdef"};

    int size = schema->EncodeValue(
        std::make_any<std::vector<std::string>>(data1), buf);
    EXPECT_EQ(31, size);
    size = schema->EncodeValue(std::make_any<std::vector<std::string>>(data2),
                               buf);
    EXPECT_EQ(31, size);

    size = schema->SkipValue(buf);
    EXPECT_EQ(31, size);

    auto decode_value = schema->DecodeValue(buf);
    auto actual_data = std::any_cast<std::vector<std::string>>(decode_value);
    EXPECT_EQ(actual_data.size(), data2.size());
    for (uint32_t i = 0; i < actual_data.size(); ++i) {
      EXPECT_EQ(actual_data[i], data2[i]);
    }
  }

  {
    /*
     * allowNull: true; with null value.
     */
    auto schema = std::make_shared<DingoSchema<std::vector<std::string>>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    std::vector<std::string> data = {};
    // Do not test DecodeValue when the value is null.
    int size =
        schema->EncodeValue(std::make_any<std::vector<std::string>>(data), buf);
    EXPECT_EQ(4, size);
  }

  {
    /*
     * allowNull: true; with null value.
     */
    auto schema = std::make_shared<DingoSchema<std::vector<std::string>>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    int size = schema->EncodeValue(std::any(), buf);
    EXPECT_EQ(0, size);
  }

  {
    /*
     * allowNull: true; with value.
     */
    auto schema = std::make_shared<DingoSchema<std::vector<std::string>>>();
    schema->SetAllowNull(true);

    Buf buf(1024);
    std::vector<std::string> data1 = {"hello", "world", "nihao"};
    std::vector<std::string> data2 = {"12345", "6789a", "bcdef"};

    int size = schema->EncodeValue(
        std::make_any<std::vector<std::string>>(data1), buf);
    EXPECT_EQ(31, size);
    size = schema->EncodeValue(std::make_any<std::vector<std::string>>(data2),
                               buf);
    EXPECT_EQ(31, size);

    size = schema->SkipValue(buf);
    EXPECT_EQ(31, size);

    auto decode_value = schema->DecodeValue(buf);
    auto actual_data = std::any_cast<std::vector<std::string>>(decode_value);
    EXPECT_EQ(actual_data.size(), data2.size());
    for (uint32_t i = 0; i < actual_data.size(); ++i) {
      EXPECT_EQ(actual_data[i], data2[i]);
    }
  }
}
