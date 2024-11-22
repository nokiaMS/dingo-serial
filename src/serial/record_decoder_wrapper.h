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

#ifndef DINGO_RECORD_DECODER_WRAPPER_H_
#define DINGO_RECORD_DECODER_WRAPPER_H_

#include <memory>
#include <string>

#include "serial/record/record_decoder.h"
#include "serial/recordV2/record_decoder.h"
#include "serial/schema/base_schema.h"
#include "utils/keyvalue.h"
#include "utilsV2/keyvalue.h"

namespace dingodb {

class RecordDecoder {
 private:
  int codec_version_;
  std::shared_ptr<std::vector<std::shared_ptr<dingodb::BaseSchema>>> schemas_v1_;

  dingodb::RecordDecoderV1* re_v1_;
  dingodb::V2::RecordDecoderV2* re_v2_;

  // converter from v2 schemas to v1 schemas.
  //std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> ConvertSchemas2V1(
  //    const std::vector<V2::BaseSchemaPtr>& schemas);

 public:
  // constructors for v1.
  RecordDecoder(
      int schema_version,
      std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> schemas,
      long common_id);
  RecordDecoder(
      int schema_version,
      std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> schemas,
      long common_id, bool le);

  // constructors for v2.
  RecordDecoder(int schema_version,
                const std::vector<V2::BaseSchemaPtr>& schemas, long common_id);
  RecordDecoder(int schema_version,
                const std::vector<V2::BaseSchemaPtr>& schemas, long common_id,
                bool le);

  ~RecordDecoder() {
    delete re_v1_;
    delete re_v2_;
  }

  void Init(int schema_version,
            std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> schemas,
            long common_id) {
    re_v1_->Init(schema_version, schemas, common_id);
  }

  // decode for v1.
  int Decode(const KeyValue& key_value,
             std::vector<std::any>& record /*output*/) {
    return re_v1_->Decode(key_value, record);
  }

  // decode for v2.
  int Decode(const V2::KeyValue& key_value,
             std::vector<std::any>& record /*output*/) {
    if (DINGO_UNLIKELY(key_value.GetVersion() == V2::CODEC_VERSION_V1)) {
      // This code branch is inefficient, but factly the code will not run here except
      // the scenario that we are running the new verison(v2) on old data(v1), so we just
      // keep the code like this.
      auto key_value_v1 =
          KeyValue(std::make_shared<std::string>(key_value.GetKey()),
                   std::make_shared<std::string>(key_value.GetValue()));
      return re_v1_->Decode(key_value_v1, record);
    } else {
      return re_v2_->Decode(key_value, record);
    }
  }

  int Decode(const std::string& key, const std::string& value,
             std::vector<std::any>& record /*output*/) {
    auto key_len = key.size();
    const char * p = key.data();
    char a = key.at(key_len - 1);
    if (key.at(key.size() - 1) == dingodb::V2::CODEC_VERSION_V1) {
      return re_v1_->Decode(key, value, record);
    } else {
      return re_v2_->Decode(key, value, record);
    }
  }

  int DecodeKey(const std::string& key,
                std::vector<std::any>& record /*output*/) {
    if (key.at(key.size() - 1) == dingodb::V2::CODEC_VERSION_V1) {
      return re_v1_->DecodeKey(key, record);
    } else {
      return re_v2_->DecodeKey(key, record);
    }
  }

  // decode for v1.
  int Decode(const KeyValue& key_value, const std::vector<int>& column_indexes,
             std::vector<std::any>& record /*output*/) {
    return re_v1_->Decode(key_value, column_indexes, record);
  }

  // decode for v2.
  int Decode(const V2::KeyValue& key_value,
             const std::vector<int>& column_indexes,
             std::vector<std::any>& record /*output*/) {
    return re_v2_->Decode(key_value, column_indexes, record);
  }

  int Decode(const std::string& key, const std::string& value,
             const std::vector<int>& column_indexes,
             std::vector<std::any>& record /*output*/) {
    if (key.at(key.size() - 1) == dingodb::V2::CODEC_VERSION_V1) {
      return re_v1_->Decode(key, value, column_indexes, record);
    } else {
      return re_v2_->Decode(key, value, column_indexes, record);
    }
  }
};

}  // namespace dingodb

#endif  // DINGO_RECORD_DECODER_WRAPPER_H_
