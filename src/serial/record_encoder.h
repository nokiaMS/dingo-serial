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

#ifndef DINGO_RECORD_ENCODER_WRAPPER_H_
#define DINGO_RECORD_ENCODER_WRAPPER_H_

#include <any>
#include <memory>
#include <string>
#include <vector>

#include "serial/record/V2/record_encoder.h"
#include "serial/record/record_encoder.h"
#include "serial/schema/base_schema.h"
#include "serial/utils/V2/compiler.h"
#include "serial/utils/V2/schema_converter.h"

namespace dingodb {

class RecordEncoder {
 private:
  int codec_version_;
  std::shared_ptr<std::vector<std::shared_ptr<dingodb::BaseSchema>>>
      schemas_v1_;
  std::vector<serialV2::BaseSchemaPtr> schemas_v2_;

  dingodb::RecordEncoderV1* re_v1_;
  dingodb::serialV2::RecordEncoderV2* re_v2_;

 public:
  // constructors for version 1.
  RecordEncoder(
      int schema_version,
      std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> schemas,
      long common_id);
  RecordEncoder(
      int schema_version,
      std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> schemas,
      long common_id, bool le);

  // constructors for version 2.
  RecordEncoder(int schema_version,
                const std::vector<serialV2::BaseSchemaPtr>& schemas,
                long common_id);
  RecordEncoder(int schema_version,
                const std::vector<serialV2::BaseSchemaPtr>& schemas,
                long common_id, bool le);

  ~RecordEncoder() {
    delete re_v1_;
    delete re_v2_;
  }

  void SetCodecVersion(int v) { this->codec_version_ = v; }

  int GetCodecVersion() { return this->codec_version_; }

  void Init(int schema_version,
            std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> schemas,
            long common_id);

  int Encode(char prefix, const std::vector<std::any>& record, std::string& key,
             std::string& value) {
    if (DINGO_UNLIKELY(codec_version_ == serialV2::CODEC_VERSION_V1)) {
      return re_v1_->Encode(prefix, record, key, value);
    } else {
      return re_v2_->Encode(prefix, record, key, value);
    }
  }

  int EncodeKey(char prefix, const std::vector<std::any>& record,
                std::string& output) {
    if (DINGO_UNLIKELY(codec_version_ == serialV2::CODEC_VERSION_V1)) {
      return re_v1_->EncodeKey(prefix, record, output);
    } else {
      return re_v2_->EncodeKey(prefix, record, output);
    }
  }

  int EncodeValue(const std::vector<std::any>& record, std::string& output) {
    if (DINGO_UNLIKELY(codec_version_ == serialV2::CODEC_VERSION_V1)) {
      return re_v1_->EncodeValue(record, output);
    } else {
      return re_v2_->EncodeValue(record, output);
    }
  }

  int EncodeKeyPrefix(char prefix, const std::vector<std::any>& record,
                      int column_count, std::string& output) {
    if (DINGO_UNLIKELY(codec_version_ == serialV2::CODEC_VERSION_V1)) {
      return re_v1_->EncodeKeyPrefix(prefix, record, column_count, output);
    } else {
      throw std::runtime_error("Unsupport function EncodeKeyPrefix.");
    }
  }

  int EncodeKeyPrefix(char prefix, const std::vector<std::string>& keys,
                      std::string& output) {
    if (DINGO_UNLIKELY(codec_version_ == serialV2::CODEC_VERSION_V1)) {
      return re_v1_->EncodeKeyPrefix(prefix, keys, output);
    } else {
      throw std::runtime_error("Unsupport function EncodeKeyPrefix.");
    }
  }

  int EncodeMaxKeyPrefix(char prefix, std::string& output) const {
    if (DINGO_UNLIKELY(codec_version_ == serialV2::CODEC_VERSION_V1)) {
      return re_v1_->EncodeMaxKeyPrefix(prefix, output);
    } else {
      return re_v2_->EncodeMaxKeyPrefix(prefix, output);
    }
  }

  int EncodeMinKeyPrefix(char prefix, std::string& output) const {
    if (DINGO_UNLIKELY(codec_version_ == serialV2::CODEC_VERSION_V1)) {
      return re_v1_->EncodeMaxKeyPrefix(prefix, output);
    } else {
      return re_v2_->EncodeMaxKeyPrefix(prefix, output);
    }
  }
};

}  // namespace dingodb

#endif  // DINGO_RECORD_ENCODER_WRAPPER_H_
