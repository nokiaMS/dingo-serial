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
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "serial/record/V2/record_decoder.h"
#include "serial/record/record_decoder.h"
#include "serial/record/record_encoder.h"
#include "serial/schema/base_schema.h"

// using namespace dingodb;

const char kAlphabet[] = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i',
                          'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r',
                          's', 't', 'o', 'v', 'w', 'x', 'y', 'z', '0',
                          '1', '2', '3', '4', '5', '6', '7', '8', '9'};

static int64_t TimestampMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

// rand string
static std::string GenRandomString(int len) {
  std::string result;
  int alphabet_len = sizeof(kAlphabet);

  std::mt19937 rng;
  rng.seed(std::random_device()());
  std::uniform_int_distribution<std::mt19937::result_type> distrib(1,
                                                                   1000000000);
  for (int i = 0; i < len; ++i) {
    result.append(1, kAlphabet[distrib(rng) % alphabet_len]);
  }

  return result;
}

std::shared_ptr<std::vector<std::shared_ptr<dingodb::BaseSchema>>>
GenerateSchemasV1() {
  std::shared_ptr<std::vector<std::shared_ptr<dingodb::BaseSchema>>> schemas =
      std::make_shared<std::vector<std::shared_ptr<dingodb::BaseSchema>>>(11);

  auto id = std::make_shared<dingodb::DingoSchema<std::optional<int32_t>>>();
  id->SetIndex(0);
  id->SetAllowNull(false);
  id->SetIsKey(true);
  schemas->at(0) = id;

  auto name = std::make_shared<
      dingodb::DingoSchema<std::optional<std::shared_ptr<std::string>>>>();
  name->SetIndex(1);
  name->SetAllowNull(false);
  name->SetIsKey(true);
  schemas->at(1) = name;

  auto gender = std::make_shared<
      dingodb::DingoSchema<std::optional<std::shared_ptr<std::string>>>>();
  gender->SetIndex(2);
  gender->SetAllowNull(false);
  gender->SetIsKey(true);
  schemas->at(2) = gender;

  auto score = std::make_shared<dingodb::DingoSchema<std::optional<int64_t>>>();
  score->SetIndex(3);
  score->SetAllowNull(false);
  score->SetIsKey(true);
  schemas->at(3) = score;

  auto addr = std::make_shared<
      dingodb::DingoSchema<std::optional<std::shared_ptr<std::string>>>>();
  addr->SetIndex(4);
  addr->SetAllowNull(true);
  addr->SetIsKey(false);
  schemas->at(4) = addr;

  auto exist = std::make_shared<dingodb::DingoSchema<std::optional<bool>>>();
  exist->SetIndex(5);
  exist->SetAllowNull(false);
  exist->SetIsKey(false);
  schemas->at(5) = exist;

  auto pic = std::make_shared<
      dingodb::DingoSchema<std::optional<std::shared_ptr<std::string>>>>();
  pic->SetIndex(6);
  pic->SetAllowNull(true);
  pic->SetIsKey(false);
  schemas->at(6) = pic;

  auto test_null =
      std::make_shared<dingodb::DingoSchema<std::optional<int32_t>>>();
  test_null->SetIndex(7);
  test_null->SetAllowNull(true);
  test_null->SetIsKey(false);
  schemas->at(7) = test_null;

  auto age = std::make_shared<dingodb::DingoSchema<std::optional<int32_t>>>();
  age->SetIndex(8);
  age->SetAllowNull(false);
  age->SetIsKey(false);
  schemas->at(8) = age;

  auto prev = std::make_shared<dingodb::DingoSchema<std::optional<int64_t>>>();
  prev->SetIndex(9);
  prev->SetAllowNull(false);
  prev->SetIsKey(false);
  schemas->at(9) = prev;

  auto salary = std::make_shared<dingodb::DingoSchema<std::optional<double>>>();
  salary->SetIndex(10);
  salary->SetAllowNull(true);
  salary->SetIsKey(false);
  schemas->at(10) = salary;

  return schemas;
}

std::vector<std::any> GenerateRecord(int32_t id) {
  std::vector<std::any> record;
  record.resize(11);

  std::string name = GenRandomString(128);
  std::string gender = GenRandomString(32);
  int64_t score = 214748364700L;
  std::string addr = GenRandomString(256);
  bool exist = false;

  int32_t age = -20;
  int64_t prev = -214748364700L;
  double salary = 873485.4234;

  std::optional<std::shared_ptr<std::string>> pic = std::nullopt;
  std::optional<int32_t> test_null = std::nullopt;

  record.at(0) = std::optional<int32_t>(id);
  record.at(1) = std::optional<std::shared_ptr<std::string>>{
      std::make_shared<std::string>(name)};
  record.at(2) = std::optional<std::shared_ptr<std::string>>{
      std::make_shared<std::string>(gender)};
  record.at(3) = std::optional<int64_t>(score);
  record.at(4) = std::optional<std::shared_ptr<std::string>>{
      std::make_shared<std::string>(addr)};
  record.at(5) = std::optional<bool>(exist);
  record.at(6) = pic;
  record.at(7) = test_null;
  record.at(8) = std::optional<int32_t>(age);
  record.at(9) = std::optional<int64_t>(prev);
  record.at(10) = std::optional<double>(salary);

  return record;
}

class PerformanceTest : public testing::Test {};
constexpr int loop_times = 100000;

TEST_F(PerformanceTest, perfV1) {
  /*
   * test v1 encoder and decoder.
   */
  uint64_t start_time = TimestampMs();
  std::vector<std::vector<std::any>> records;
  records.reserve(loop_times);
  for (int32_t i = 0; i < loop_times; ++i) {
    records.push_back(GenerateRecord(i));
  }

  std::cout << "Generate record elapsed time: " << TimestampMs() - start_time
            << "ms" << std::endl;
  auto schemas = GenerateSchemasV1();

  start_time = TimestampMs();
  std::cout << "Start testing..., count: " << loop_times << " ms" << std::endl;

  dingodb::RecordEncoderV1 encoder(1, schemas, 100);
  dingodb::RecordDecoderV1 decoder(1, schemas, 100);

  for (int i = 0; i < 1; i++) {
    for (const auto& record : records) {
      std::string key;
      std::string value;
      encoder.Encode('r', record, key, value);

      std::vector<std::any> decode_record;
      decoder.Decode(std::move(key), std::move(value), decode_record);
    }
  }

  std::cout << "Encode/Decode elapsed time: " << TimestampMs() - start_time
            << "ms" << std::endl;
}
