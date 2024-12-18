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

#include "serial/schema/V2/base_schema.h"
#include "serial/record/V2/common.h"
#include "serial/record/V2/record_decoder.h"
#include "serial/record/V2/record_encoder.h"
#include "serial/record_decoder.h"
#include "serial/record_encoder.h"

//using namespace dingodb::serialV2;

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

std::vector<dingodb::serialV2::BaseSchemaPtr> GenerateSchemas() {
  std::vector<dingodb::serialV2::BaseSchemaPtr> schemas;
  schemas.resize(11);

  auto id = std::make_shared<dingodb::serialV2::DingoSchema<int32_t>>();
  id->SetIndex(0);
  id->SetAllowNull(false);
  id->SetIsKey(true);
  schemas.at(0) = id;

  auto name = std::make_shared<dingodb::serialV2::DingoSchema<std::string>>();
  name->SetIndex(1);
  name->SetAllowNull(false);
  name->SetIsKey(true);
  schemas.at(1) = name;

  auto gender = std::make_shared<dingodb::serialV2::DingoSchema<std::string>>();
  gender->SetIndex(2);
  gender->SetAllowNull(false);
  gender->SetIsKey(true);
  schemas.at(2) = gender;

  auto score = std::make_shared<dingodb::serialV2::DingoSchema<int64_t>>();
  score->SetIndex(3);
  score->SetAllowNull(false);
  score->SetIsKey(true);
  schemas.at(3) = score;

  auto addr = std::make_shared<dingodb::serialV2::DingoSchema<std::string>>();
  addr->SetIndex(4);
  addr->SetAllowNull(true);
  addr->SetIsKey(false);
  schemas.at(4) = addr;

  auto exist = std::make_shared<dingodb::serialV2::DingoSchema<bool>>();
  exist->SetIndex(5);
  exist->SetAllowNull(false);
  exist->SetIsKey(false);
  schemas.at(5) = exist;

  auto pic = std::make_shared<dingodb::serialV2::DingoSchema<std::string>>();
  pic->SetIndex(6);
  pic->SetAllowNull(true);
  pic->SetIsKey(false);
  schemas.at(6) = pic;

  auto test_null = std::make_shared<dingodb::serialV2::DingoSchema<int32_t>>();
  test_null->SetIndex(7);
  test_null->SetAllowNull(true);
  test_null->SetIsKey(false);
  schemas.at(7) = test_null;

  auto age = std::make_shared<dingodb::serialV2::DingoSchema<int32_t>>();
  age->SetIndex(8);
  age->SetAllowNull(false);
  age->SetIsKey(false);
  schemas.at(8) = age;

  auto prev = std::make_shared<dingodb::serialV2::DingoSchema<int64_t>>();
  prev->SetIndex(9);
  prev->SetAllowNull(false);
  prev->SetIsKey(false);
  schemas.at(9) = prev;

  auto salary = std::make_shared<dingodb::serialV2::DingoSchema<double>>();
  salary->SetIndex(10);
  salary->SetAllowNull(true);
  salary->SetIsKey(false);
  schemas.at(10) = salary;

  return schemas;
}

std::vector<dingodb::serialV2::BaseSchemaPtr> GenerateSchemas1Column() {
  std::vector<dingodb::serialV2::BaseSchemaPtr> schemas;
  schemas.resize(2);

  auto name = std::make_shared<dingodb::serialV2::DingoSchema<std::string>>();
  name->SetIndex(0);
  name->SetAllowNull(false);
  name->SetIsKey(true);
  schemas.at(0) = name;

  auto author_id = std::make_shared<dingodb::serialV2::DingoSchema<int32_t>>();
  author_id->SetIndex(1);
  author_id->SetAllowNull(true);
  author_id->SetIsKey(false);
  schemas.at(1) = author_id;

  return schemas;
}

std::shared_ptr<std::vector<std::shared_ptr<dingodb::BaseSchema>>> GenerateSchemasV1() {
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

  auto test_null = std::make_shared<dingodb::DingoSchema<std::optional<int32_t>>>();
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
  //int64_t score = 214748364700L;
  int64_t score = 1004;
  //std::string addr = GenRandomString(256);
  std::string addr = "";
  bool exist = false;

  int32_t age = -20;
  int64_t prev = -214748364700L;
  double salary = 873485.4234;

  record.at(0) = id;
  record.at(1) = name;
  record.at(2) = gender;
  record.at(3) = score;
  record.at(4) = addr;
  record.at(5) = exist;
  record.at(8) = age;
  record.at(9) = prev;
  record.at(10) = salary;

  return record;
}

std::vector<std::any> GenerateRecord1Column() {
  std::vector<std::any> record;
  record.resize(2);

  std::string name = "abcd";


  record.at(0) = name;
  record.at(1) = std::any();

  return record;
}

std::vector<std::any> GenerateRecordV1(int32_t id) {
  std::vector<std::any> record;
  record.resize(11);

  std::string name = GenRandomString(128);
  //std::string name = "1234567890abcde";
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

class PerformanceTestV2 : public testing::Test {};

TEST_F(PerformanceTestV2, wrapperPerf) {
  /*
   * Use the wrapperred decoder and recoder and V2 records.
   */
  constexpr int loop_times = 100000;
  uint64_t start_time = TimestampMs();
  std::vector<std::vector<std::any>> records;
  records.reserve(loop_times);
  for (int32_t i = 0; i < loop_times; ++i) {
    records.push_back(GenerateRecord(i));
  }

  std::cout << "Generate record elapsed time: " << TimestampMs() - start_time
            << "ms" << std::endl;
  auto schemas = GenerateSchemas();

  start_time = TimestampMs();
  std::cout << "Start testing..., count: " << loop_times << " ms" << std::endl;

  dingodb::RecordEncoder encoder(1, schemas, 100);
  dingodb::RecordDecoder decoder(1, schemas, 100);

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

TEST_F(PerformanceTestV2, wrapperPerf_eq) {
  /*
   * Use the wrapperred decoder and recoder and V2 records.
   */
  std::vector<std::any> record = GenerateRecord(123);
  auto schemas = GenerateSchemas();

  dingodb::RecordEncoder encoder(1, schemas, 100);
  dingodb::RecordDecoder decoder(1, schemas, 100);

  std::string key;
  std::string value;
  encoder.Encode('r', record, key, value);
  EXPECT_EQ(encoder.GetCodecVersion(), 0x2);

  std::vector<std::any> decode_record;
  decoder.Decode(std::move(key), std::move(value), decode_record);

  //id.
  auto id1 = std::any_cast<int32_t>(record.at(0));
  auto id2 = std::any_cast<int32_t>(decode_record.at(0));
  EXPECT_EQ(id1, id2);
  //name.
  auto name1 = std::any_cast<std::string>(record.at(1));
  auto name2 = std::any_cast<std::string>(decode_record.at(1));
  EXPECT_EQ(name1, name2);

  //gender.
  auto gender1 = std::any_cast<std::string>(record.at(2));
  auto gender2 = std::any_cast<std::string>(decode_record.at(2));
  EXPECT_EQ(gender1, gender2);

  //score.
  auto score1 = std::any_cast<int64_t>(record.at(3));
  auto score2 = std::any_cast<int64_t>(decode_record.at(3));
  EXPECT_EQ(score1, score2);

  //addr
  auto addr1 = std::any_cast<std::string>(record.at(4));
  auto addr2 = std::any_cast<std::string>(decode_record.at(4));
  EXPECT_EQ(addr1, addr2);

  //exist.
  auto exist1 = std::any_cast<bool>(record.at(5));
  auto exist2 = std::any_cast<bool>(decode_record.at(5));
  EXPECT_EQ(exist1, exist2);

  //pic
  auto pic1 = record.at(6).has_value();
  auto pic2 = decode_record.at(6).has_value();
  EXPECT_EQ(pic1, pic2);
  EXPECT_EQ(pic1, false);


  //test_null.
  auto test_null1 = record.at(7).has_value();
  auto test_null2 = decode_record.at(7).has_value();
  EXPECT_EQ(test_null1, test_null2);
  EXPECT_EQ(test_null1, false);

  //age
  auto age1 = std::any_cast<int32_t>(record.at(8));
  auto age2 = std::any_cast<int32_t>(decode_record.at(8));
  EXPECT_EQ(age1, age2);

  //prev
  auto prev1 = std::any_cast<int64_t>(record.at(9));
  auto prev2 = std::any_cast<int64_t>(decode_record.at(9));
  EXPECT_EQ(prev1, prev2);

  //double
  auto double1 = std::any_cast<double>(record.at(10));
  auto double2 = std::any_cast<double>(record.at(10));
  EXPECT_EQ(double1, double2);

  /*
  //set v1 encoder.
  std::vector<std::any> record_v1 = GenerateRecordV1(123);

  encoder.SetCodecVersion(0x1);
  std::string key1;
  std::string value1;
  encoder.Encode('r', record_v1, key1, value1);
  EXPECT_EQ(encoder.GetCodecVersion(), 0x1);

  std::vector<std::any> decode_record_v1;
  decoder.Decode(std::move(key1), std::move(value1), decode_record_v1);

  //id.
  auto id3 = std::any_cast<std::optional<int32_t>>(record_v1.at(0)).value();
  auto id4 = std::any_cast<std::optional<int32_t>>(decode_record_v1.at(0)).value();
  EXPECT_EQ(id3, id4);

  //name.
  auto name3 = *(std::any_cast<std::optional<std::shared_ptr<std::string>>>(record_v1.at(1)).value());
  auto name4 = *(std::any_cast<std::optional<std::shared_ptr<std::string>>>(decode_record_v1.at(1)).value());
  EXPECT_EQ(name3, name4);

  //gender.
  auto gender3 = *(std::any_cast<std::optional<std::shared_ptr<std::string>>>(record_v1.at(2)).value());
  auto gender4 = *(std::any_cast<std::optional<std::shared_ptr<std::string>>>(decode_record_v1.at(2)).value());
  EXPECT_EQ(gender3, gender4);

  //score.
  auto score3 = std::any_cast<std::optional<int64_t>>(record_v1.at(3)).value();
  auto score4 = std::any_cast<std::optional<int64_t>>(decode_record_v1.at(3)).value();
  EXPECT_EQ(score3, score4);

  //addr
  auto addr3 = *(std::any_cast<std::optional<std::shared_ptr<std::string>>>(record_v1.at(4)).value());
  auto addr4 = *(std::any_cast<std::optional<std::shared_ptr<std::string>>>(decode_record_v1.at(4)).value());
  EXPECT_EQ(addr3, addr4);

  //exist.
  auto exist3 = std::any_cast<std::optional<bool>>(record_v1.at(5)).value();
  auto exist4 = std::any_cast<std::optional<bool>>(decode_record_v1.at(5)).value();
  EXPECT_EQ(exist3, exist4);

  //pic
  auto pic3 = std::any_cast<std::optional<std::shared_ptr<std::string>>>(record_v1.at(6)).has_value();
  auto pic4 = std::any_cast<std::optional<std::shared_ptr<std::string>>>(decode_record_v1.at(6)).has_value();
  EXPECT_EQ(pic3, pic4);
  EXPECT_EQ(pic3, false);

  //test_null.
  auto test_null3 = std::any_cast<std::optional<int32_t>>(record_v1.at(7)).has_value();
  auto test_null4 = std::any_cast<std::optional<int32_t>>(decode_record_v1.at(7)).has_value();
  EXPECT_EQ(test_null3, test_null4);
  EXPECT_EQ(pic3, false);

  //age
  auto age3 = std::any_cast<std::optional<int32_t>>(record_v1.at(8)).value();
  auto age4 = std::any_cast<std::optional<int32_t>>(decode_record_v1.at(8)).value();
  EXPECT_EQ(age3, age4);

  //prev
  auto prev3 = std::any_cast<std::optional<int64_t>>(record_v1.at(9)).value();
  auto prev4 = std::any_cast<std::optional<int64_t>>(decode_record_v1.at(9)).value();
  EXPECT_EQ(prev3, prev4);

  //double
  auto double3 = std::any_cast<std::optional<double>>(record_v1.at(10)).value();
  auto double4 = std::any_cast<std::optional<double>>(decode_record_v1.at(10)).value();
  EXPECT_EQ(double3, double4);
  */
}

TEST_F(PerformanceTestV2, wrapperPerf_value_only_1_column_and_is_null) {
  /*
   * Use the wrapperred decoder and recoder and V2 records.
   */
  std::vector<std::any> record = GenerateRecord1Column();
  auto schemas = GenerateSchemas1Column();

  dingodb::RecordEncoder encoder(1, schemas, 100);
  //encoder.SetCodecVersion(0x02);

  dingodb::RecordDecoder decoder(1, schemas, 100);

  std::string key;
  std::string value;
  encoder.Encode('r', record, key, value);
  EXPECT_EQ(encoder.GetCodecVersion(), 0x02);

  std::vector<std::any> decode_record;
  decoder.Decode(std::move(key), std::move(value), decode_record);
  EXPECT_EQ(decoder.GetCodecVersion(key), 0X02);

  //name.
  auto name1 = std::any_cast<std::string>(record.at(0));
  auto name2 = std::any_cast<std::string>(decode_record.at(0));
  EXPECT_EQ(name1, name2);

  //id.
  auto id1 = (record.at(1).has_value()) ? std::any_cast<int32_t>(record.at(1)) : std::any();
  auto id2 = (record.at(1).has_value()) ? std::any_cast<int32_t>(decode_record.at(1)) : std::any();
  EXPECT_EQ(id1.has_value(), false);
  EXPECT_EQ(id2.has_value(), false);
}

TEST_F(PerformanceTestV2, wrapperPerf_v1_schemas_to_v2_schemas) {
  /*
   * Use the wrapperred decoder and recoder and V2 records.
   */
  std::vector<std::any> record = GenerateRecord(123);
  auto schemas = GenerateSchemasV1();

  dingodb::RecordEncoder encoder(1, schemas, 100);
  //encoder.SetCodecVersion(0x02);

  dingodb::RecordDecoder decoder(1, schemas, 100);

  std::string key;
  std::string value;
  encoder.Encode('r', record, key, value);
  EXPECT_EQ(encoder.GetCodecVersion(), 0x02);

  std::vector<std::any> decode_record;
  decoder.Decode(std::move(key), std::move(value), decode_record);
  EXPECT_EQ(decoder.GetCodecVersion(key), 0X02);

  //id.
  auto id1 = std::any_cast<int32_t>(record.at(0));
  auto id2 = std::any_cast<int32_t>(decode_record.at(0));
  EXPECT_EQ(id1, id2);
  //name.
  auto name1 = std::any_cast<std::string>(record.at(1));
  auto name2 = std::any_cast<std::string>(decode_record.at(1));
  EXPECT_EQ(name1, name2);

  //gender.
  auto gender1 = std::any_cast<std::string>(record.at(2));
  auto gender2 = std::any_cast<std::string>(decode_record.at(2));
  EXPECT_EQ(gender1, gender2);

  //score.
  auto score1 = std::any_cast<int64_t>(record.at(3));
  auto score2 = std::any_cast<int64_t>(decode_record.at(3));
  EXPECT_EQ(score1, score2);

  //addr
  auto addr1 = std::any_cast<std::string>(record.at(4));
  auto addr2 = std::any_cast<std::string>(decode_record.at(4));
  EXPECT_EQ(addr1, addr2);

  //exist.
  auto exist1 = std::any_cast<bool>(record.at(5));
  auto exist2 = std::any_cast<bool>(decode_record.at(5));
  EXPECT_EQ(exist1, exist2);

  //pic
  auto pic1 = record.at(6).has_value();
  auto pic2 = decode_record.at(6).has_value();
  EXPECT_EQ(pic1, pic2);
  EXPECT_EQ(pic1, false);


  //test_null.
  auto test_null1 = record.at(7).has_value();
  auto test_null2 = decode_record.at(7).has_value();
  EXPECT_EQ(test_null1, test_null2);
  EXPECT_EQ(test_null1, false);

  //age
  auto age1 = std::any_cast<int32_t>(record.at(8));
  auto age2 = std::any_cast<int32_t>(decode_record.at(8));
  EXPECT_EQ(age1, age2);

  //prev
  auto prev1 = std::any_cast<int64_t>(record.at(9));
  auto prev2 = std::any_cast<int64_t>(decode_record.at(9));
  EXPECT_EQ(prev1, prev2);

  //double
  auto double1 = std::any_cast<double>(record.at(10));
  auto double2 = std::any_cast<double>(record.at(10));
  EXPECT_EQ(double1, double2);

  /*
  //set v1 encoder.
  std::vector<std::any> record_v1 = GenerateRecordV1(123);

  encoder.SetCodecVersion(0x01);
  std::string key1;
  std::string value1;
  encoder.Encode('r', record_v1, key1, value1);
  EXPECT_EQ(encoder.GetCodecVersion(), 0x01);

  std::vector<std::any> decode_record_v1;
  decoder.Decode(std::move(key1), std::move(value1), decode_record_v1);
  EXPECT_EQ(decoder.GetCodecVersion(key1), 0X01);

  //id.
  auto id3 = std::any_cast<std::optional<int32_t>>(record_v1.at(0)).value();
  auto id4 = std::any_cast<std::optional<int32_t>>(decode_record_v1.at(0)).value();
  EXPECT_EQ(id3, id4);

  //name.
  auto name3 = *(std::any_cast<std::optional<std::shared_ptr<std::string>>>(record_v1.at(1)).value());
  auto name4 = *(std::any_cast<std::optional<std::shared_ptr<std::string>>>(decode_record_v1.at(1)).value());
  EXPECT_EQ(name3, name4);

  //gender.
  auto gender3 = *(std::any_cast<std::optional<std::shared_ptr<std::string>>>(record_v1.at(2)).value());
  auto gender4 = *(std::any_cast<std::optional<std::shared_ptr<std::string>>>(decode_record_v1.at(2)).value());
  EXPECT_EQ(gender3, gender4);

  //score.
  auto score3 = std::any_cast<std::optional<int64_t>>(record_v1.at(3)).value();
  auto score4 = std::any_cast<std::optional<int64_t>>(decode_record_v1.at(3)).value();
  EXPECT_EQ(score3, score4);

  //addr
  auto addr3 = *(std::any_cast<std::optional<std::shared_ptr<std::string>>>(record_v1.at(4)).value());
  auto addr4 = *(std::any_cast<std::optional<std::shared_ptr<std::string>>>(decode_record_v1.at(4)).value());
  EXPECT_EQ(addr3, addr4);

  //exist.
  auto exist3 = std::any_cast<std::optional<bool>>(record_v1.at(5)).value();
  auto exist4 = std::any_cast<std::optional<bool>>(decode_record_v1.at(5)).value();
  EXPECT_EQ(exist3, exist4);

  //pic
  auto pic3 = std::any_cast<std::optional<std::shared_ptr<std::string>>>(record_v1.at(6)).has_value();
  auto pic4 = std::any_cast<std::optional<std::shared_ptr<std::string>>>(decode_record_v1.at(6)).has_value();
  EXPECT_EQ(pic3, pic4);
  EXPECT_EQ(pic3, false);

  //test_null.
  auto test_null3 = std::any_cast<std::optional<int32_t>>(record_v1.at(7)).has_value();
  auto test_null4 = std::any_cast<std::optional<int32_t>>(decode_record_v1.at(7)).has_value();
  EXPECT_EQ(test_null3, test_null4);
  EXPECT_EQ(pic3, false);

  //age
  auto age3 = std::any_cast<std::optional<int32_t>>(record_v1.at(8)).value();
  auto age4 = std::any_cast<std::optional<int32_t>>(decode_record_v1.at(8)).value();
  EXPECT_EQ(age3, age4);

  //prev
  auto prev3 = std::any_cast<std::optional<int64_t>>(record_v1.at(9)).value();
  auto prev4 = std::any_cast<std::optional<int64_t>>(decode_record_v1.at(9)).value();
  EXPECT_EQ(prev3, prev4);

  //double
  auto double3 = std::any_cast<std::optional<double>>(record_v1.at(10)).value();
  auto double4 = std::any_cast<std::optional<double>>(decode_record_v1.at(10)).value();
  EXPECT_EQ(double3, double4);
  */
}

TEST_F(PerformanceTestV2, perf) {
  /*
   * Use the V2 decoder and recoder directly.
   */
  constexpr int loop_times = 100000;
  uint64_t start_time = TimestampMs();
  std::vector<std::vector<std::any>> records;
  records.reserve(loop_times);
  for (int32_t i = 0; i < loop_times; ++i) {
    records.push_back(GenerateRecord(i));
  }

  std::cout << "Generate record elapsed time: " << TimestampMs() - start_time
            << "ms" << std::endl;
  auto schemas = GenerateSchemas();

  start_time = TimestampMs();
  std::cout << "Start testing..., count: " << loop_times << " ms" << std::endl;

  dingodb::serialV2::RecordEncoderV2 encoder(1, schemas, 100);
  dingodb::serialV2::RecordDecoderV2 decoder(1, schemas, 100);

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
