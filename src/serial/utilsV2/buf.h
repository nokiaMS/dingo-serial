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

#ifndef DINGO_SERIAL_BUF_V2_H_
#define DINGO_SERIAL_BUF_V2_H_

#include <cstddef>
#include <cstdint>
#include <iostream>
#include <string>

#include "serial/utilsV2/compiler.h"

namespace dingodb {
namespace V2 {

/*
 * Comparing memory is from low addr to high addr, so must by big endian.
 * little endian int32/int64 must transform to big endian(high num locate low
 * addr). e.g.
 *   number:                      1234567(Ox12d687)   <     2234500(0x221884)
 *   addr:                        0     1     2             0     1     2
 *   little endian:               0x87  0xd6  0x12    >     0x84  0x18  0x22 --
 * compare wrong big endian:                  0x12  0xd6  0x87    <     0x22
 * 0x18  0x84    -- compare right
 */
class Buf {
 public:
  Buf(size_t capacity, bool le);
  Buf(size_t capacity);

  Buf(const std::string& s, bool le);
  Buf(const std::string& s);

  Buf(std::string&& s, bool le);
  Buf(std::string&& s);
  Buf() = default;

  ~Buf() = default;

  // le and end checker.
  bool IsLe() const { return le_; }
  bool IsEnd() const { return read_offset_ == buf_.size(); }

  /**
   * For reader function with position parameters will not update read_offset_
   * field. Only the reader with no position parameters will increase
   * read_offset_ field.
   */

  // byte writter and getter.
  void Write(uint8_t data);
  void WriteByte(size_t pos, uint8_t data);
  void WriteWithNegation(uint8_t data);
  uint8_t Peek();
  uint8_t Read();
  uint8_t Read(size_t pos);
  // uint8_t ReverseReadByte(size_t pos);

  // short writter and getter.
  void WriteShort(int16_t data);
  void WriteShort(size_t pos, int16_t data);
  int16_t ReadShort();
  int16_t ReadShort(int pos);
  // uint16_t ReverseReadShort(int pos);

  // int writter and getter.
  void WriteInt(int32_t data);
  void WriteInt(size_t pos, int32_t data);
  int32_t PeekInt();
  int32_t ReadInt();
  int32_t ReadInt(int pos);
  // int32_t ReverseReadInt(int pos);

  // long writter and getter.
  void WriteLong(int64_t data);
  void WriteLongWithNegation(int64_t data);
  void WriteLongWithFirstBitNegation(int64_t data);
  int64_t PeekLong();
  int64_t ReadLong();
  int64_t ReadLong(int pos);
  int64_t ReadLongWithFirstBitNegation();

  // string writter and getter.
  void WriteString(const std::string& data);
  const std::string& GetString();
  void GetString(std::string& s);
  void GetString(std::string* s);

  // skip.
  void Skip(size_t size);

  // clear.
  void Clear() {
    read_offset_ = 0;
    buf_.clear();
  }

  // Reserve
  void Reserve(int cap) { buf_.reserve(cap); }

  // size.
  size_t Size() const { return buf_.size(); }
  void ReSize(size_t size) { buf_.resize(size); }
  void Enlarge(size_t len);

  // offset
  size_t RestReadableSize() const { return buf_.size() - read_offset_; }
  size_t ReadOffset() const { return read_offset_; }
  void SetReadOffset(size_t offset) {
    if (DINGO_UNLIKELY(offset >= buf_.size())) {
      throw std::runtime_error("Out of range.");
    }
    read_offset_ = offset;
  }

 private:
  bool le_{true};

  size_t read_offset_{0};

  // for memory comparable buf_ is big endian
  std::string buf_;
};

}  // namespace V2
}  // namespace dingodb

#endif