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

#include "buf.h"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <string>

#include "serial/utils/V2/compiler.h"

namespace dingodb {
namespace serialV2 {

Buf::Buf(size_t capacity, bool le) : le_(le) { buf_.reserve(capacity); }

Buf::Buf(size_t capacity) : Buf(capacity, true) {}

Buf::Buf(const std::string& s, bool le) : le_(le) { buf_ = s; }

Buf::Buf(const std::string& s) : Buf(s, true) {}

Buf::Buf(std::string&& s, bool le) : le_(le) { buf_.swap(s); }

Buf::Buf(std::string&& s) : Buf(s, true) {}

void Buf::Write(uint8_t data) { buf_.push_back(data); }

void Buf::WriteWithNegation(uint8_t data) { buf_.push_back(~data); }

void Buf::Enlarge(size_t len) { this->buf_.resize(buf_.size() + len); }

void Buf::WriteInt(int32_t data) {
  size_t curr_size = buf_.size();
  buf_.resize(curr_size + 4);

  char* buf = buf_.data();
  uint8_t* i = (uint8_t*)&data;
  if (DINGO_LIKELY(this->le_)) {
    buf[curr_size] = *(i + 3);
    buf[curr_size + 1] = *(i + 2);
    buf[curr_size + 2] = *(i + 1);
    buf[curr_size + 3] = *i;
  } else {
    buf[curr_size] = *i;
    buf[curr_size + 1] = *(i + 1);
    buf[curr_size + 2] = *(i + 2);
    buf[curr_size + 3] = *(i + 3);
  }
}

void Buf::WriteByte(size_t pos, uint8_t data) {
  if (DINGO_UNLIKELY(pos + 1 > buf_.size())) {
    throw std::runtime_error("Out of range.");
  }

  char* buf = buf_.data();
  buf[pos] = (char)data;
}

void Buf::WriteShort(int16_t data) {
  size_t curr_size = buf_.size();
  buf_.resize(curr_size + 2);

  char* buf = buf_.data();
  uint8_t* i = (uint8_t*)&data;
  if (DINGO_LIKELY(this->le_)) {
    buf[curr_size] = *(i + 1);
    buf[curr_size + 1] = *i;
  } else {
    buf[curr_size] = *i;
    buf[curr_size + 1] = *(i + 1);
  }
}

void Buf::WriteShort(size_t pos, int16_t data) {
  if (DINGO_UNLIKELY(pos + 2 > buf_.size())) {
    throw std::runtime_error("Out of range.");
  }

  char* buf = buf_.data();
  uint8_t* i = (uint8_t*)&data;
  if (DINGO_LIKELY(this->le_)) {
    buf[pos] = *(i + 1);
    buf[pos + 1] = *i;
  } else {
    buf[pos] = *i;
    buf[pos + 1] = *(i + 1);
  }
}

void Buf::WriteInt(size_t pos, int32_t data) {
  if (DINGO_UNLIKELY(pos + 4 > buf_.size())) {
    throw std::runtime_error("Out of range.");
  }

  char* buf = buf_.data();
  uint8_t* i = (uint8_t*)&data;
  if (DINGO_LIKELY(this->le_)) {
    buf[pos] = *(i + 3);
    buf[pos + 1] = *(i + 2);
    buf[pos + 2] = *(i + 1);
    buf[pos + 3] = *i;
  } else {
    buf[pos] = *i;
    buf[pos + 1] = *(i + 1);
    buf[pos + 2] = *(i + 2);
    buf[pos + 3] = *(i + 3);
  }
}

void Buf::WriteLong(int64_t data) {
  uint8_t* i = (uint8_t*)&data;
  if (DINGO_LIKELY(this->le_)) {
    buf_.push_back(*(i + 7));
    buf_.push_back(*(i + 6));
    buf_.push_back(*(i + 5));
    buf_.push_back(*(i + 4));
    buf_.push_back(*(i + 3));
    buf_.push_back(*(i + 2));
    buf_.push_back(*(i + 1));
    buf_.push_back(*i);
  } else {
    buf_.push_back(*i);
    buf_.push_back(*(i + 1));
    buf_.push_back(*(i + 2));
    buf_.push_back(*(i + 3));
    buf_.push_back(*(i + 4));
    buf_.push_back(*(i + 5));
    buf_.push_back(*(i + 6));
    buf_.push_back(*(i + 7));
  }
}

void Buf::WriteLongWithNegation(int64_t data) {
  uint8_t* i = (uint8_t*)&data;
  if (DINGO_LIKELY(this->le_)) {
    buf_.push_back(~*(i + 7));
    buf_.push_back(~*(i + 6));
    buf_.push_back(~*(i + 5));
    buf_.push_back(~*(i + 4));
    buf_.push_back(~*(i + 3));
    buf_.push_back(~*(i + 2));
    buf_.push_back(~*(i + 1));
    buf_.push_back(~*i);

  } else {
    buf_.push_back(~*i);
    buf_.push_back(~*(i + 1));
    buf_.push_back(~*(i + 2));
    buf_.push_back(~*(i + 3));
    buf_.push_back(~*(i + 4));
    buf_.push_back(~*(i + 5));
    buf_.push_back(~*(i + 6));
    buf_.push_back(~*(i + 7));
  }
}

void Buf::WriteLongWithFirstBitNegation(int64_t data) {
  uint8_t* i = (uint8_t*)&data;
  if (DINGO_LIKELY(this->le_)) {
    buf_.push_back(*(i + 7) ^ 0x80);
    buf_.push_back(*(i + 6));
    buf_.push_back(*(i + 5));
    buf_.push_back(*(i + 4));
    buf_.push_back(*(i + 3));
    buf_.push_back(*(i + 2));
    buf_.push_back(*(i + 1));
    buf_.push_back(*i);

  } else {
    buf_.push_back(*i ^ 0x80);
    buf_.push_back(*(i + 1));
    buf_.push_back(*(i + 2));
    buf_.push_back(*(i + 3));
    buf_.push_back(*(i + 4));
    buf_.push_back(*(i + 5));
    buf_.push_back(*(i + 6));
    buf_.push_back(*(i + 7));
  }
}

void Buf::WriteString(const std::string& data) {
  size_t curr_size = buf_.size();
  buf_.resize(curr_size + data.size());

  memcpy(buf_.data() + curr_size, data.data(), data.size());
}

uint8_t Buf::Peek() { return buf_.at(read_offset_); }

int32_t Buf::PeekInt() {
  if (DINGO_LIKELY(this->le_)) {
    char* buf = buf_.data();
    return ((buf[read_offset_] & 0xFF) << 24) |
           ((buf[read_offset_ + 1] & 0xFF) << 16) |
           ((buf[read_offset_ + 2] & 0xFF) << 8) |
           (buf[read_offset_ + 3] & 0xFF);
  } else {
    char* buf = buf_.data();
    return (buf[read_offset_] & 0xFF) | ((buf[read_offset_ + 1] & 0xFF) << 8) |
           ((buf[read_offset_ + 2] & 0xFF) << 16) |
           ((buf[read_offset_ + 3] & 0xFF) << 24);
  }
}

int64_t Buf::PeekLong() {
  char* buf = buf_.data();
  int64_t l = 0;
  if (DINGO_LIKELY(this->le_)) {
    l |= (buf[read_offset_] & 0xFF);
    l <<= 8;
    l |= (buf[read_offset_ + 1] & 0xFF);
    l <<= 8;
    l |= (buf[read_offset_ + 2] & 0xFF);
    l <<= 8;
    l |= (buf[read_offset_ + 3] & 0xFF);
    l <<= 8;
    l |= (buf[read_offset_ + 4] & 0xFF);
    l <<= 8;
    l |= (buf[read_offset_ + 5] & 0xFF);
    l <<= 8;
    l |= (buf[read_offset_ + 6] & 0xFF);
    l <<= 8;
    l |= (buf[read_offset_ + 7] & 0xFF);

  } else {
    l |= (buf[read_offset_ + 7] & 0xFF);
    l <<= 8;
    l |= (buf[read_offset_ + 6] & 0xFF);
    l <<= 8;
    l |= (buf[read_offset_ + 5] & 0xFF);
    l <<= 8;
    l |= (buf[read_offset_ + 4] & 0xFF);
    l <<= 8;
    l |= (buf[read_offset_ + 3] & 0xFF);
    l <<= 8;
    l |= (buf[read_offset_ + 2] & 0xFF);
    l <<= 8;
    l |= (buf[read_offset_ + 1] & 0xFF);
    l <<= 8;
    l |= (buf[read_offset_] & 0xFF);
  }
  return l;
}

uint8_t Buf::Read() { return buf_.at(read_offset_++); }

uint8_t Buf::Read(size_t pos) {
  uint8_t ret = buf_.at(pos);
  return ret;
}

int16_t Buf::ReadShort() {
  if (DINGO_LIKELY(this->le_)) {
    int a = Read();
    int b = Read();
    return ((a & 0xFF) << 8) | (b & 0xFF);
  } else {
    return (Read() & 0xFF) | ((Read() & 0xFF) << 8);
  }
}

int16_t Buf::ReadShort(int pos) {
  int ret = 0;
  if (DINGO_LIKELY(this->le_)) {
    ret = ((buf_.at(pos) & 0xFF) << 8) | (buf_.at(pos + 1) & 0xFF);
  } else {
    ret = (buf_.at(pos) & 0xFF) | ((buf_.at(pos + 1) & 0xFF) << 8);
  }
  return ret;
}

int32_t Buf::ReadInt() {
  if (DINGO_LIKELY(this->le_)) {
    return ((Read() & 0xFF) << 24) | ((Read() & 0xFF) << 16) |
           ((Read() & 0xFF) << 8) | (Read() & 0xFF);
  } else {
    return (Read() & 0xFF) | ((Read() & 0xFF) << 8) | ((Read() & 0xFF) << 16) |
           ((Read() & 0xFF) << 24);
  }
}

int32_t Buf::ReadInt(int pos) {
  if (DINGO_LIKELY(this->le_)) {
    return ((buf_.at(pos) & 0xFF) << 24) | ((buf_.at(pos + 1) & 0xFF) << 16) |
           ((buf_.at(pos + 2) & 0xFF) << 8) | (buf_.at(pos + 3) & 0xFF);
  } else {
    return (buf_.at(pos) & 0xFF) | ((buf_.at(pos + 1) & 0xFF) << 8) |
           ((buf_.at(pos + 2) & 0xFF) << 16) |
           ((buf_.at(pos + 3) & 0xFF) << 24);
  }
}

int64_t Buf::ReadLong() {
  uint64_t l = Read() & 0xFF;
  if (DINGO_LIKELY(this->le_)) {
    for (int i = 0; i < 7; i++) {
      l <<= 8;
      l |= Read() & 0xFF;
    }
  } else {
    for (int i = 1; i < 8; i++) {
      l |= (((uint64_t)Read() & 0xFF) << (8 * i));
    }
  }
  return l;
}

int64_t Buf::ReadLong(int pos) {
  char* buf = buf_.data();
  int64_t l = 0;
  if (DINGO_LIKELY(this->le_)) {
    l |= (buf[pos] & 0xFF);
    l <<= 8;
    l |= (buf[pos + 1] & 0xFF);
    l <<= 8;
    l |= (buf[pos + 2] & 0xFF);
    l <<= 8;
    l |= (buf[pos + 3] & 0xFF);
    l <<= 8;
    l |= (buf[pos + 4] & 0xFF);
    l <<= 8;
    l |= (buf[pos + 5] & 0xFF);
    l <<= 8;
    l |= (buf[pos + 6] & 0xFF);
    l <<= 8;
    l |= (buf[pos + 7] & 0xFF);

  } else {
    l |= (buf[pos + 7] & 0xFF);
    l <<= 8;
    l |= (buf[pos + 6] & 0xFF);
    l <<= 8;
    l |= (buf[pos + 5] & 0xFF);
    l <<= 8;
    l |= (buf[pos + 4] & 0xFF);
    l <<= 8;
    l |= (buf[pos + 3] & 0xFF);
    l <<= 8;
    l |= (buf[pos + 2] & 0xFF);
    l <<= 8;
    l |= (buf[pos + 1] & 0xFF);
    l <<= 8;
    l |= (buf[pos] & 0xFF);
  }
  return l;
}

int64_t Buf::ReadLongWithFirstBitNegation() {
  uint64_t l = (Read() & 0xFF) ^ 0x80;
  if (IsLe()) {
    for (int i = 0; i < 7; i++) {
      l <<= 8;
      l |= Read() & 0xFF;
    }
  } else {
    for (int i = 1; i < 8; i++) {
      l |= (((uint64_t)Read() & 0xFF) << (8 * i));
    }
  }

  return l;
}

void Buf::Skip(size_t size) {
  if (DINGO_UNLIKELY(read_offset_ + size > buf_.size())) {
    throw std::runtime_error("Out of range.");
  }

  read_offset_ += size;
}

const std::string& Buf::GetString() { return buf_; }

void Buf::GetString(std::string& s) { s.swap(buf_); }

void Buf::GetString(std::string* s) { s->swap(buf_); }

}  // namespace serialV2
}  // namespace dingodb