//
// Created by guoxu on 2025/11/10.
//

#ifndef DINGO_SERIAL_MYDECIMAL_H
#define DINGO_SERIAL_MYDECIMAL_H

#include "DecimalContext.h"
#include <string>

class MyDecimal {
private:
  int precision = 0;
  int scale = 0;
  int digitsInt = 0;
  int digitsFrac = 0;
  int resultFrac = 0;
  bool negative = false;

  int wordBuf[MaxWordBufLen] = {};

  int digitsToWords(int digits);
  int countLeadingZeroes(int i, int word);
  DecimalContext removeLeadingZeros();
  DecimalContext DecimalBinSize(int precision, int frac);
  int readWord(std::string& b, int index, int size);
  void writeWord(std::string& b, int start,  int word, int size);
  DecimalContext fixWordCntError(int wordsInt, int wordsFrac);
  std::string dealWithE(std::string string);
  std::string writeBin(int precision, int frac);
  std::string toBin(int precision, int frac);
  void FromBin(std::string& bin, int precision, int frac);
  int stringSize();
public:
  MyDecimal(std::string& string, int prec, int scal);
  MyDecimal(std::string& buf, int prec, int scal, bool isBin);
  std::string toBin();
  std::string decimalToString();
};

#endif  // DINGO_SERIAL_MYDECIMAL_H
