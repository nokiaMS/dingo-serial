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

#include "serial/schema/decimal_schema.h"

#include <memory>
#include <string>

namespace dingodb {

int DingoSchema<std::optional<std::shared_ptr<DecimalString>>>::GetDataLength() {
  return 0;
}

int DingoSchema<
    std::optional<std::shared_ptr<DecimalString>>>::GetWithNullTagLength() {
  return 0;
}

int DingoSchema<std::optional<std::shared_ptr<DecimalString>>>::InternalEncodeKey(
    Buf* buf, std::shared_ptr<DecimalString> data) {
  int group_num = (*data).str.length() / 8;
  int size = (group_num + 1) * 9;
  int remainder_size = (*data).str.length() % 8;
  int remainder_zero;
  if (remainder_size == 0) {
    remainder_size = 8;
    remainder_zero = 8;
  } else {
    remainder_zero = 8 - remainder_size;
  }
  buf->EnsureRemainder(size + 4);
  int curr = 0;
  for (int i = 0; i < group_num; i++) {
    for (int j = 0; j < 8; j++) {
      buf->Write((*data).str.at(curr++));
    }
    buf->Write((uint8_t)255);
  }
  if (remainder_size < 8) {
    for (int j = 0; j < remainder_size; j++) {
      buf->Write((*data).str.at(curr++));
    }
  }
  for (int i = 0; i < remainder_zero; i++) {
    buf->Write((uint8_t)0);
  }
  buf->Write((uint8_t)(255 - remainder_zero));
  return size;
}

void DingoSchema<std::optional<std::shared_ptr<DecimalString>>>::
    InternalEncodeValue(Buf* buf, std::shared_ptr<DecimalString> data) {
  buf->EnsureRemainder((*data).str.length() + 4);
  buf->WriteInt((*data).str.length());
  buf->Write((*data).str);
}

BaseSchema::Type
DingoSchema<std::optional<std::shared_ptr<DecimalString>>>::GetType() {
  return kDecimal;
}

void DingoSchema<std::optional<std::shared_ptr<DecimalString>>>::SetIndex(
    int index) {
  this->index_ = index;
}

int DingoSchema<std::optional<std::shared_ptr<DecimalString>>>::GetIndex() {
  return this->index_;
}

void DingoSchema<std::optional<std::shared_ptr<DecimalString>>>::SetIsKey(
    bool key) {
  this->key_ = key;
}

bool DingoSchema<std::optional<std::shared_ptr<DecimalString>>>::IsKey() {
  return this->key_;
}

int DingoSchema<std::optional<std::shared_ptr<DecimalString>>>::GetLength() {
  if (this->allow_null_) {
    return GetWithNullTagLength();
  }
  return GetDataLength();
}

void DingoSchema<std::optional<std::shared_ptr<DecimalString>>>::SetAllowNull(
    bool allow_null) {
  this->allow_null_ = allow_null;
}

bool DingoSchema<std::optional<std::shared_ptr<DecimalString>>>::AllowNull() {
  return this->allow_null_;
}

void DingoSchema<std::optional<std::shared_ptr<DecimalString>>>::EncodeKey(
    Buf* buf, std::optional<std::shared_ptr<DecimalString>> data) {
  if (this->allow_null_) {
    if (data.has_value()) {
      buf->EnsureRemainder(1);
      buf->Write(k_not_null);
      int size = InternalEncodeKey(buf, data.value());
      buf->EnsureRemainder(4);
      buf->ReverseWriteInt(size);
    } else {
      buf->EnsureRemainder(5);
      buf->Write(k_null);
      buf->ReverseWriteInt(0);
    }
  } else {
    if (data.has_value()) {
      int size = InternalEncodeKey(buf, data.value());
      buf->EnsureRemainder(4);
      buf->ReverseWriteInt(size);
    } else {
      // WRONG EMPTY DATA
    }
  }
}

void DingoSchema<std::optional<std::shared_ptr<DecimalString>>>::EncodeKeyPrefix(
    Buf* buf, std::optional<std::shared_ptr<DecimalString>> data) {
  if (this->allow_null_) {
    if (data.has_value()) {
      buf->EnsureRemainder(1);
      buf->Write(k_not_null);
      InternalEncodeKey(buf, data.value());
    } else {
      buf->EnsureRemainder(1);
      buf->Write(k_null);
    }
  } else {
    if (data.has_value()) {
      InternalEncodeKey(buf, data.value());
    } else {
      // WRONG EMPTY DATA
    }
  }
}

std::optional<std::shared_ptr<DecimalString>>
DingoSchema<std::optional<std::shared_ptr<DecimalString>>>::DecodeKey(Buf* buf) {
  if (this->allow_null_) {
    if (buf->Read() == this->k_null) {
      buf->ReverseSkipInt();
      return std::nullopt;
    }
  }
  int length = buf->ReverseReadInt();
  int group_num = length / 9;
  buf->Skip(length - 1);
  int remainder_zero = 255 - (buf->Read() & 0xFF);
  buf->Skip(0 - length);
  int ori_length = group_num * 8 - remainder_zero;
  std::shared_ptr<DecimalString> data = std::make_shared<DecimalString>(ori_length, 0);

  if (ori_length != 0) {
    int curr = 0;
    group_num--;
    for (int i = 0; i < group_num; i++) {
      for (int j = 0; j < 8; j++) {
        (*data).str[curr++] = buf->Read();
      }
      buf->Skip(1);
    }
    if (remainder_zero != 8) {
      int non_zero_count = 8 - remainder_zero;
      for (int j = 0; j < non_zero_count; j++) {
        (*data).str[curr++] = buf->Read();
      }
    }
  }

  buf->Skip(remainder_zero + 1);

  return std::optional<std::shared_ptr<DecimalString>>(data);
}

void DingoSchema<std::optional<std::shared_ptr<DecimalString>>>::SkipKey(
    Buf* buf) const {
  if (this->allow_null_) {
    buf->Skip(buf->ReverseReadInt() + 1);
  } else {
    buf->Skip(buf->ReverseReadInt());
  }
}

void DingoSchema<std::optional<std::shared_ptr<DecimalString>>>::EncodeValue(
    Buf* buf, std::optional<std::shared_ptr<DecimalString>> data) {
  if (this->allow_null_) {
    buf->EnsureRemainder(1);
    if (data.has_value()) {
      buf->Write(k_not_null);
      InternalEncodeValue(buf, data.value());
    } else {
      buf->Write(k_null);
    }
  } else {
    if (data.has_value()) {
      InternalEncodeValue(buf, data.value());
    } else {
      // WRONG EMPTY DATA
    }
  }
}

std::optional<std::shared_ptr<DecimalString>> DingoSchema<
    std::optional<std::shared_ptr<DecimalString>>>::DecodeValue(Buf* buf) {
  if (this->allow_null_) {
    if (buf->Read() == this->k_null) {
      return std::nullopt;
    }
  }
  int length = buf->ReadInt();
  auto su8 = std::make_shared<DecimalString>(length, 0);

  for (int i = 0; i < length; i++) {
    (*su8).str[i] = buf->Read();
  }

  return std::optional<std::shared_ptr<DecimalString>>{su8};
}

void DingoSchema<std::optional<std::shared_ptr<DecimalString>>>::SkipValue(
    Buf* buf) const {
  if (this->allow_null_) {
    if (buf->Read() == this->k_null) {
      return;
    }
  }
  buf->Skip(buf->ReadInt());
}

}  // namespace dingodb