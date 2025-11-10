//
// Created by guoxu on 2025/11/11.
//

#include "keyBuf.h"

#include <cstring>

namespace dingodb {
namespace serialV2 {

void KeyBuf::Init(int size) {
  this->buf_.resize(size);
  this->reverse_pos_ = size - 1;
}

void KeyBuf::Init(const std::string& buf) {
  this->buf_.resize(buf.size());
  this->buf_.assign(buf.begin(), buf.end());
  this->reverse_pos_ = this->buf_.size() - 1;
}

KeyBuf::KeyBuf(int size, bool le) {
  Init(size);
  this->le_ = le;
}

KeyBuf::KeyBuf(const std::string& buf, bool le) {
  Init(buf);
  this->le_ = le;
}

void KeyBuf::Write(uint8_t data) { buf_.at(forward_pos_++) = data; }

void KeyBuf::WriteShort(int16_t data) {
  uint16_t* ii = (uint16_t*)&data;
  if (DINGO_LIKELY(this->le_)) {
    Write(*ii >> 8);
    Write(*ii);
  } else {
    Write(*ii);
    Write(*ii >> 8);
  }
}

void KeyBuf::WriteInt(int32_t data) {
  uint32_t* ii = (uint32_t*)&data;
  if (DINGO_LIKELY(this->le_)) {
    Write(*ii >> 24);
    Write(*ii >> 16);
    Write(*ii >> 8);
    Write(*ii);
  } else {
    Write(*ii);
    Write(*ii >> 8);
    Write(*ii >> 16);
    Write(*ii >> 24);
  }
}

void KeyBuf::WriteLong(int64_t data) {
  uint64_t* ll = (uint64_t*)&data;
  if (this->le_) {
    Write(*ll >> 56);
    Write(*ll >> 48);
    Write(*ll >> 40);
    Write(*ll >> 32);
    Write(*ll >> 24);
    Write(*ll >> 16);
    Write(*ll >> 8);
    Write(*ll);
  } else {
    Write(*ll);
    Write(*ll >> 8);
    Write(*ll >> 16);
    Write(*ll >> 24);
    Write(*ll >> 32);
    Write(*ll >> 40);
    Write(*ll >> 48);
    Write(*ll >> 56);
  }
}

void KeyBuf::WriteString(const std::string& data) {
  std::memcpy(buf_.data() + forward_pos_, data.data(), data.size());
  forward_pos_ += data.size();
}

void KeyBuf::WriteWithNegation(uint8_t data) { buf_.at(forward_pos_++) = ~data; }

void KeyBuf::WriteLongWithNegation(int64_t data) {
  uint8_t* i = (uint8_t*)&data;
  if (DINGO_LIKELY(this->le_)) {
    Write(~*(i + 7));
    Write(~*(i + 6));
    Write(~*(i + 5));
    Write(~*(i + 4));
    Write(~*(i + 3));
    Write(~*(i + 2));
    Write(~*(i + 1));
    Write(~*i);
  } else {
    Write(~*i);
    Write(~*(i + 1));
    Write(~*(i + 2));
    Write(~*(i + 3));
    Write(~*(i + 4));
    Write(~*(i + 5));
    Write(~*(i + 6));
    Write(~*(i + 7));
  }
}

void KeyBuf::WriteLongWithFirstBitNegation(int64_t data) {
  uint8_t* i = (uint8_t*)&data;
  if (DINGO_LIKELY(this->le_)) {
    Write(*(i + 7) ^ 0x80);
    Write(*(i + 6));
    Write(*(i + 5));
    Write(*(i + 4));
    Write(*(i + 3));
    Write(*(i + 2));
    Write(*(i + 1));
    Write(*i);
  } else {
    Write(*i ^ 0x80);
    Write(*(i + 1));
    Write(*(i + 2));
    Write(*(i + 3));
    Write(*(i + 4));
    Write(*(i + 5));
    Write(*(i + 6));
    Write(*(i + 7));
  }
}

void KeyBuf::EnsureRemainder(int length) {
  if ((forward_pos_ + length ) > reverse_pos_) {
    int new_size;
    if (length > 100) {
      new_size = buf_.size() + length;
    } else {
      new_size = buf_.size() + 100;
    }
    std::string new_buf;
    new_buf.resize(new_size);
    for (int i = 0; i < forward_pos_; i++) {
      new_buf.at(i) = buf_.at(i);
    }
    int reverse_size = buf_.size() - reverse_pos_ - 1;
    int buf_start = reverse_pos_ + 1;
    int new_buf_start = new_size - reverse_size;
    for (int i = 0; i < reverse_size; i++) {
      new_buf.at(new_buf_start + i) = buf_.at(buf_start + i);
    }
    reverse_pos_ = new_size - reverse_size - 1;
    buf_ = new_buf;
  }
}

}
}
