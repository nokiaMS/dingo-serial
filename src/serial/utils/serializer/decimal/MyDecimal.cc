//
// Created by guoxu on 2025/11/10.
//

#include "MyDecimal.h"

#include <cctype>
#include <stdexcept>

#include "DecimalContext.h"
#include "algorithm"

int MyDecimal::digitsToWords(int digits) {
  if(digits+digitsPerWord-1 >= 0 && digits+digitsPerWord-1 < div9Len) {
    return div9[digits+digitsPerWord-1];
  }
  return (digits + digitsPerWord - 1) / digitsPerWord;
}

int MyDecimal::countLeadingZeroes(int i, int word) {
  int leading = 0;
  while(word < powers10[i]) {
    i--;
    leading++;
  }
  return leading;
}

DecimalContext MyDecimal::removeLeadingZeros() {
  DecimalContext decimalInProcess = DecimalContext();
  decimalInProcess.digitsIntLocal = digitsInt;
  int i = ((decimalInProcess.digitsIntLocal - 1) % digitsPerWord) + 1;
  while (decimalInProcess.digitsIntLocal > 0 && wordBuf[decimalInProcess.wordIdx] == 0) {
    decimalInProcess.digitsIntLocal -= i;
    i = digitsPerWord;
    decimalInProcess.wordIdx++;
  }
  if (decimalInProcess.digitsIntLocal > 0) {
    decimalInProcess.digitsIntLocal -= countLeadingZeroes((decimalInProcess.digitsIntLocal-1)%digitsPerWord, wordBuf[decimalInProcess.wordIdx]);
  } else {
    decimalInProcess.digitsIntLocal = 0;
  }
  return decimalInProcess;
}

DecimalContext MyDecimal::DecimalBinSize(int precision, int frac) {
  DecimalContext decimalContext = DecimalContext();
  int digitsInt = precision - frac;
  int wordsInt = digitsInt / digitsPerWord;
  int wordsFrac = frac / digitsPerWord;
  int xInt = digitsInt - wordsInt*digitsPerWord;
  int xFrac = frac - wordsFrac*digitsPerWord;
  if (xInt < 0 || xInt >= dig2bytesLength || xFrac < 0 || xFrac >= dig2bytesLength) {
    decimalContext.decimalBinSize = 0;
    decimalContext.err = ErrBadNumber;
    return decimalContext;
  }

  decimalContext.decimalBinSize = wordsInt*wordSize + dig2bytes[xInt] + wordsFrac*wordSize + dig2bytes[xFrac];
  decimalContext.err = None;
  return decimalContext;
}

int MyDecimal::readWord(std::string& b, int index, int size) {
  int x = 0;
  switch (size) {
    case 1: {
      x = b[index];
      break;
    }
    case 2: {
      int a1 = b[index] << 8;
      int a2 = b[index + 1] & 0xFF;
      x = a1 + a2;

      //x = (b[index] << 8) & 0xFFFF + b[index + 1] & 0xFF;
      break;
    }
    case 3: {
      if ((b[index] & 128) > 0) {
        x = (255) << 24 | ((b[index]) << 16) & 0xFFFFFF | ((b[index + 1]) << 8) & 0xFFFF | (b[index + 2] & 0xFF);
      } else {
        x = ((b[index]) << 16) & 0xFFFFFF | ((b[index + 1]) << 8) & 0xFFFF | (b[index + 2] & 0xFF);
      }
      break;
    }
    case 4: {
      x = (b[index + 3] & 0xFF) +
              ((b[index + 2] << 8) & 0xFFFF) +
              ((b[index + 1] << 16) & 0xFFFFFF) +
              ((b[index] << 24) & 0xFFFFFFFF);
      break;
    }
  }
  return x;
}

void MyDecimal::writeWord(std::string& b, int start,  int word, int size) {
  int v = word;
  switch(size) {
    case 1: {
      b[start] = (char) word;
      break;
    }
    case 2: {
      b[start] = (char) (v >> 8);
      b[start + 1] = (char) (v);
      break;
    }
    case 3: {
      b[start] = (char) (v >> 16);
      b[start + 1] = (char) (v >> 8);
      b[start + 2] = (char) (v);
      break;
    }
    case 4: {
      b[start] = (char) (v >> 24);
      b[start + 1] = (char) (v >> 16);
      b[start + 2] = (char) (v >> 8);
      b[start + 3] = (char) (v);
      break;
    }
  }
}

DecimalContext MyDecimal::fixWordCntError(int wordsInt, int wordsFrac) {
  DecimalContext decimalContext = DecimalContext();
  if(wordsInt + wordsFrac > wordBufLen) {
    if(wordsInt > wordBufLen) {
      decimalContext.newWordInt = wordBufLen;
      decimalContext.newWordFrac = 0;
      decimalContext.err = ErrOverflow;

      return decimalContext;
    }

    decimalContext.newWordInt = wordsInt;
    decimalContext.newWordFrac = wordBufLen - wordsInt;
    decimalContext.err = ErrTruncate;

    return decimalContext;
  }

  decimalContext.newWordInt = wordsInt;
  decimalContext.newWordFrac = wordsFrac;
  decimalContext.err = None;

  return decimalContext;
}

 std::string MyDecimal::dealWithE(std::string string) {
        int ePos = string.find("e");
        if (ePos == std::string::npos) {
          ePos = string.find("E");
        }

        int eVal = std::stoi(string.substr(ePos+1, string.length()));

        int negative = -1;  //no flag, default +.
        if( string.at(0) == '-' ) {
            negative = 1;   // flag for '-'.
        } else if( string.at(0) == '+' ) {
            negative = 0;   // flag for '+'.
        }

        int valueStart = (negative == -1) ? 0 : 1;
        bool hasPoint = false;
        int pointPos = string.find(".");
        if (pointPos != std::string::npos) {
          hasPoint = true;
        }

        if(hasPoint) {
            //int pointPos = string.indexOf(".");
            std::string intPartString = string.substr(valueStart, pointPos);
            std::string fracPartString = string.substr(pointPos + 1, ePos);

            std::string builder;
            if (negative == 1) {
                builder.append("-");
            } else if(negative == 0) {
                builder.append("+");
            }

            int newPointPos = pointPos + eVal;
            if(newPointPos < 0 ) {
                int appendingZeroCount = std::abs(newPointPos);
                if(negative != -1 && newPointPos < 0) {
                    appendingZeroCount++;
                }
                return builder.append("0.")
                        .append(std::string(appendingZeroCount, '0'))
                        .append(intPartString)
                        .append(fracPartString);
            } else if(newPointPos > intPartString.length() + fracPartString.length()) {
                int appendingZeroCount = newPointPos - intPartString.length() - fracPartString.length();
                if(negative != -1) {
                    appendingZeroCount--;
                }
                return builder.append(intPartString)
                        .append(fracPartString)
                        .append(std::string(appendingZeroCount, '0'));
            } else {
                return builder.append(intPartString)
                        .append(fracPartString)
                        .insert(newPointPos, ".");
            }
        } else {
            std::string builder;
            if (negative == 1) {
                builder.append("-");
            } else if(negative == 0) {
                builder.append("+");
            }

            std::string finalString;
            int newPointPos = eVal;
            if(newPointPos < 0 ) {
                int appendingZeroCount = std::abs(newPointPos);
                return builder.append(std::string(std::abs(newPointPos), '0'))
                        .append(string);
            } else if(newPointPos > string.length()) {
                int appendingZeroCount = newPointPos - string.length();
                return builder.append(string).append(std::string((std::abs(newPointPos)), '0'));
            } else {
                int finalPointPos = ((negative == -1) ? 0 : 1) + newPointPos;
                return builder.append(string).insert(finalPointPos, ".");
            }
        }
    }

/*
std::string& rtrim(std::string& s) {
  s.erase(
      std::find_if(s.rbegin(), s.rend(),
                   std::not1(std::function<int, int>(std::isspace))).base(),
      s.end()
  );
  return s;
}
*/

void rtrim(std::string& str) {
  if (str.empty()) return;
  size_t i = str.size() - 1;

  while (i != std::string::npos && isspace(static_cast<unsigned char>(str[i]))) {
    if (i == 0) {
      str.clear();
      return;
    }
    --i;
  }
  str.erase(i + 1);
}

MyDecimal::MyDecimal(std::string& string, int prec, int scal) {
        rtrim(string);
        precision = prec;
        scale = scal;

        if(string.empty()) {
            throw std::runtime_error("Decimal type: Empty string is not allowed.");
        }

        if((string.find('e') != std::string::npos) || (string.find('E') != std::string::npos)) {
            string = dealWithE(string);
        }

        int curStringPos = 0;
        char flag = string.at(curStringPos);
        switch (flag) {
            case '-':
            {
                negative = true;
                //fallthrough
            }
            case '+':
            {
                curStringPos++;
            }
        }

        int strIdx = curStringPos;
        while (strIdx < string.length() && isdigit(string.at(strIdx))) {
            strIdx++;
        }
        int digitsIntLocal = strIdx - curStringPos;
        int digitsFracLocal = 0;
        int endIdx = 0;

        if(strIdx < string.length() && string.at(strIdx) == '.') {
            endIdx = strIdx + 1;
            while(endIdx < string.length() && isdigit(string.at(endIdx))) {
                endIdx++;
            }

            digitsFracLocal = endIdx - strIdx - 1;
        } else {
            endIdx = strIdx;
        }

        if(digitsIntLocal + digitsFracLocal == 0) {
            throw std::runtime_error("Decimal type: No digits in string.");
        }

        //To be compatible with MySQL rounding for decimal.
        if(digitsFracLocal != 0 && scal < digitsFracLocal) {
            int lastDigitPos = 1 - ((negative) ? 1 : 0);
            int lastDigit = string.at(digitsIntLocal + digitsFracLocal - lastDigitPos);
            lastDigit = lastDigit < 5 ? lastDigit : lastDigit + 1;
            string = string.substr(0, digitsIntLocal + digitsFracLocal - lastDigitPos) + std::to_string(lastDigit);
            digitsFracLocal = scal;
        }

        int wordsInt = digitsToWords(digitsIntLocal);
        int wordsFrac = digitsToWords(digitsFracLocal);

        DecimalContext decimalContext = fixWordCntError(wordsInt, wordsFrac);
        wordsInt = decimalContext.newWordInt;
        wordsFrac = decimalContext.newWordFrac;
        if(decimalContext.err != None) {
            digitsFracLocal = wordsFrac * digitsPerWord;
            if(decimalContext.err == ErrOverflow) {
                digitsIntLocal = wordsInt * digitsPerWord;
            }
        }

        digitsInt = digitsIntLocal;
        digitsFrac = digitsFracLocal;

        int wordIdx = wordsInt;
        int strIdxTmp = strIdx;
        int word = 0;
        int innerIdx = 0;
        while(digitsIntLocal > 0) {
            digitsIntLocal--;
            strIdx--;
            word += (int)(string.at(strIdx) - '0') * powers10[innerIdx];
            innerIdx++;
            if(innerIdx == digitsPerWord) {
                wordIdx--;
                wordBuf[wordIdx] = word;
                word = 0;
                innerIdx = 0;
            }
        }
        if(innerIdx != 0) {
            wordIdx--;
            wordBuf[wordIdx] = word;
        }

        wordIdx = wordsInt;
        strIdx = strIdxTmp;
        word = 0;
        innerIdx = 0;
        while (digitsFracLocal > 0) {
            digitsFracLocal--;
            strIdx++;
            word = (int)(string.at(strIdx) - '0') + word * 10;
            innerIdx++;
            if(innerIdx == digitsPerWord) {
                wordBuf[wordIdx] = word;
                wordIdx++;
                word = 0;
                innerIdx = 0;
            }
        }
        if(innerIdx != 0) {
            wordBuf[wordIdx] = word * powers10[digitsPerWord - innerIdx];
        }

        if(endIdx+1 <= string.length()) {
            throw std::runtime_error("Decimal type: String contained invalid characters or E at the tail.");
        }

        bool allZero = true;
        for (int i = 0; i < wordBufLen; i++) {
            if (wordBuf[i] != 0) {
                allZero = false;
                break;
            }
        }
        if(allZero) {
            negative = false;
        }
        resultFrac = digitsFrac;
        return;
    }

std::string MyDecimal::writeBin(int precision, int frac) {
        if(precision > digitsPerWord * MaxWordBufLen || precision < 0 || frac > MaxDecimalScale || frac < 0) {
            throw std::runtime_error("Decimal type: Precision or frac out of range.");
        }

        int mask = 0;
        if(negative) {
            mask = -1;
        }

        int digitsIntLocal = precision - frac;
        int wordsInt = digitsIntLocal / digitsPerWord;
        int leadingDigits = digitsIntLocal - wordsInt*digitsPerWord;
        int wordsFracLocal = frac / digitsPerWord;
        int trailingDigits = frac - wordsFracLocal*digitsPerWord;

        int wordsFracFrom = digitsFrac / digitsPerWord;
        int trailingDigitsFrom = digitsFrac - wordsFracFrom*digitsPerWord;
        int intSize = wordsInt*wordSize + dig2bytes[leadingDigits];
        int fracSize = wordsFracLocal*wordSize + dig2bytes[trailingDigits];
        int fracSizeFrom = wordsFracFrom*wordSize + dig2bytes[trailingDigitsFrom];
        int originIntSize = intSize;
        int originFracSize = fracSize;

        std::string buf;
        buf.resize(intSize+fracSize);  //= new char[intSize+fracSize];
        int binIdx = 0;

        DecimalContext decimalContext = removeLeadingZeros();
        int wordIdxFrom = decimalContext.wordIdx;
        int digitsIntFrom = decimalContext.digitsIntLocal;

        if(digitsIntFrom+fracSizeFrom == 0) {
            mask = 0;
            digitsIntLocal = 1;
        }

        int wordsIntFrom = digitsIntFrom / digitsPerWord;
        int leadingDigitsFrom = digitsIntFrom - wordsIntFrom*digitsPerWord;
        int iSizeFrom = wordsIntFrom*wordSize + dig2bytes[leadingDigitsFrom];

        if (digitsIntLocal < digitsIntFrom) {
            wordIdxFrom += wordsIntFrom - wordsInt;
            if (leadingDigitsFrom > 0) {
                wordIdxFrom++;
            }
            if (leadingDigits > 0) {
                wordIdxFrom--;
            }
            wordsIntFrom = wordsInt;
            leadingDigitsFrom = leadingDigits;
            throw std::runtime_error("Decimal type: overflow.");
        } else if (intSize > iSizeFrom) {
            while (intSize > iSizeFrom) {
                intSize--;
                buf[binIdx] = (char)mask;
                binIdx++;
            }
        }

        if ((fracSize < fracSizeFrom) ||
                (fracSize == fracSizeFrom && (trailingDigits <= trailingDigitsFrom || wordsFracLocal <= wordsFracFrom))) {
            if (fracSize < fracSizeFrom || (fracSize == fracSizeFrom && trailingDigits < trailingDigitsFrom) || (fracSize == fracSizeFrom && wordsFracLocal < wordsFracFrom)) {
                throw std::runtime_error("Decimal type: truncated.");
            }
            wordsFracFrom = wordsFracLocal;
            trailingDigitsFrom = trailingDigits;
        } else if (fracSize > fracSizeFrom && trailingDigitsFrom > 0) {
            if (wordsFracLocal == wordsFracFrom) {
                trailingDigitsFrom = trailingDigits;
                fracSize = fracSizeFrom;
            } else {
                wordsFracFrom++;
                trailingDigitsFrom = 0;
            }
        }
        // xIntFrom part
        if (leadingDigitsFrom > 0) {
            int i = dig2bytes[leadingDigitsFrom];
            int x = (wordBuf[wordIdxFrom] % powers10[leadingDigitsFrom]) ^ mask;
            wordIdxFrom++;
            writeWord(buf, binIdx, x, i);
            binIdx += i;
        }

        // wordsInt + wordsFrac part.
        for (int stop = wordIdxFrom + wordsIntFrom + wordsFracFrom; wordIdxFrom < stop; binIdx += wordSize) {
            int x = wordBuf[wordIdxFrom] ^ mask;
            wordIdxFrom++;
            writeWord(buf, binIdx, x, 4);
        }

        // xFracFrom part
        if (trailingDigitsFrom > 0) {
            int x = 0;
            int i = dig2bytes[trailingDigitsFrom];
            int lim = trailingDigits;
            if (wordsFracFrom < wordsFracLocal) {
                lim = digitsPerWord;
            }

            while (trailingDigitsFrom < lim && dig2bytes[trailingDigitsFrom] == i) {
                trailingDigitsFrom++;
            }
            x = (wordBuf[wordIdxFrom] / powers10[digitsPerWord-trailingDigitsFrom]) ^ mask;
            writeWord(buf,binIdx, x, i);
            binIdx += i;
        }
        if (fracSize > fracSizeFrom) {
            int binIdxEnd = originIntSize + originFracSize;
            while (fracSize > fracSizeFrom && binIdx < binIdxEnd) {
                fracSize--;
                buf[binIdx] = (char)(mask);
                binIdx++;
            }
        }
        buf[0] ^= 0x80;
        return buf;
    }

std::string MyDecimal::toBin() {
  std::string result = toBin(precision, scale);
  return std::move(result);
}

std::string MyDecimal::toBin(int precision, int frac) {
  return writeBin(precision, frac);
}

void MyDecimal::FromBin(std::string& bin, int precision, int frac) {
        int binSize = 0;
        if (bin.empty()) {
            throw std::runtime_error("Decimal type: bin length is zero");
        }
        int digitsIntLocal = precision - frac;
        int wordsInt = digitsIntLocal / digitsPerWord;
        int leadingDigits = digitsIntLocal - wordsInt*digitsPerWord;
        int wordsFrac = frac / digitsPerWord;
        int trailingDigits = frac - wordsFrac*digitsPerWord;
        int wordsIntTo = wordsInt;
        if (leadingDigits > 0) {
            wordsIntTo++;
        }
        int wordsFracTo = wordsFrac;
        if (trailingDigits > 0) {
            wordsFracTo++;
        }

        int binIdx = 0;
        int mask = (int)(-1);
        if ((bin[binIdx]&0x80) > 0) {
            mask = 0;
        }

        DecimalContext decimalContext = DecimalBinSize(precision, frac);
        binSize = decimalContext.decimalBinSize;
        DecimalError err = decimalContext.err;

        if(err != None) {
            throw std::runtime_error("Decimal type: Bad number.");
        }
        if (binSize < 0 || binSize > 40) {
            throw std::runtime_error("Decimal type: Bad number.");
        }

        bin[0] ^= 0x80;

        int oldWordsIntTo = wordsIntTo;
        DecimalContext decimalInProcess = fixWordCntError(wordsIntTo, wordsFracTo);
        wordsIntTo = decimalInProcess.newWordInt;
        wordsFracTo = decimalInProcess.newWordFrac;
        DecimalError err2 = decimalContext.err;

        if(err != None) {
            if(wordsIntTo < oldWordsIntTo) {
                binIdx += dig2bytes[leadingDigits] + (wordsInt-wordsIntTo)*wordSize;
            } else {
                trailingDigits = 0;
                wordsFrac = wordsFracTo;
            }
        }

        negative = (mask != 0);
        digitsInt = wordsInt*digitsPerWord + leadingDigits;
        digitsFrac = wordsFrac*digitsPerWord + trailingDigits;

        int wordIdx = 0;
        if (leadingDigits > 0) {
            int i = dig2bytes[leadingDigits];
            int x = readWord(bin,binIdx, i);
            binIdx += i;
            wordBuf[wordIdx] = x ^ mask;
            if ((long)(wordBuf[wordIdx]) >= (long)(powers10[leadingDigits+1])) {
                throw std::runtime_error("Decimal type: Bad number.");
            }
            if (wordIdx > 0 || wordBuf[wordIdx] != 0) {
                wordIdx++;
            } else {
                digitsInt -= (char)(leadingDigits);
            }
        }
        for (int stop = binIdx + wordsInt*wordSize; binIdx < stop; binIdx += wordSize) {
            wordBuf[wordIdx] = readWord(bin,binIdx, 4) ^ mask;
            if ((int)(wordBuf[wordIdx]) > wordMax) {
                throw std::runtime_error("Decimal type: bad number.");
            }
            if (wordIdx > 0 || wordBuf[wordIdx] != 0) {
                wordIdx++;
            } else {
                digitsInt -= digitsPerWord;
            }
        }

        for (int stop = binIdx + wordsFrac*wordSize; binIdx < stop; binIdx += wordSize) {
            wordBuf[wordIdx] = readWord(bin,binIdx, 4) ^ mask;
            if ((int)(wordBuf[wordIdx]) > wordMax) {
                throw std::runtime_error("Decimal type: bad number.");
            }
            wordIdx++;
        }

        if (trailingDigits > 0) {
            int i = dig2bytes[trailingDigits];
            int x = readWord(bin,binIdx, i);
            wordBuf[wordIdx] = (x ^ mask) * powers10[digitsPerWord-trailingDigits];
            if ((int)(wordBuf[wordIdx]) > wordMax) {
                throw std::runtime_error("Decimal type: Bad number.");
            }
        }

        if (digitsInt == 0 && digitsFrac == 0) {
            throw std::runtime_error("Decimal type: bad number.");
        }
        resultFrac = frac;
    }

MyDecimal::MyDecimal(std::string& buf, int prec, int scal, bool isBin) {
  precision = prec;
  scale = scal;
  FromBin(buf, precision, scale);
}

int MyDecimal::stringSize() {
  return (int)(digitsInt + digitsFrac + 3);
}

std::string MyDecimal::decimalToString() {
        int digitsFracLocal = digitsFrac;
        DecimalContext decimalInProcess = removeLeadingZeros();
        int wordStartIdx = decimalInProcess.wordIdx;
        int digitsIntLocal = decimalInProcess.digitsIntLocal;


        if(digitsIntLocal+digitsFracLocal == 0) {
            digitsIntLocal = 1;
            wordStartIdx = 0;
        }

        int digitsIntLen = digitsIntLocal;
        if(digitsIntLen == 0) {
            digitsIntLen = 1;
        }
        int digitsFracLen = digitsFracLocal;
        int length = digitsIntLen + digitsFracLen;
        if (negative) {
            length++;
        }
        if (digitsFracLocal > 0) {
            length++;
        }

        std::string str;
        str.resize(length); //= new char[length];
        int strIdx = 0;

        if (negative) {
            str[strIdx] = '-';
            strIdx++;
        }

        int fill = 0;
        if (digitsFracLocal > 0) {
            int fracIdx = strIdx + digitsIntLen;
            fill = digitsFracLen - digitsFracLocal;
            int wordIdx = wordStartIdx + digitsToWords(digitsIntLocal);
            str[fracIdx] = '.';
            fracIdx++;
            for (; digitsFracLocal > 0; digitsFracLocal -= digitsPerWord) {
                int x = wordBuf[wordIdx];
                wordIdx++;
              int min = (digitsFracLocal < digitsPerWord) ? digitsFracLocal : digitsPerWord;
                for (int i = min; i > 0; i--) {
                    int y = x / digMask;
                    str[fracIdx] = (char)((char)y + '0');
                    fracIdx++;
                    x -= y * digMask;
                    x *= 10;
                }
            }
            for (; fill > 0; fill--) {
                str[fracIdx] = '0';
                fracIdx++;
            }
        }
        fill = digitsIntLen - digitsIntLocal;
        if (digitsIntLocal == 0) {
            fill--; /* symbol 0 before digital point */
        }
        for (; fill > 0; fill--) {
            str[strIdx] = '0';
            strIdx++;
        }

        if (digitsIntLocal > 0) {
            strIdx += digitsIntLocal;
            int wordIdx = wordStartIdx + digitsToWords(digitsIntLocal);
            for (; digitsIntLocal > 0; digitsIntLocal -= digitsPerWord) {
                wordIdx--;
                int x = wordBuf[wordIdx];
                int min = (digitsIntLocal < digitsPerWord) ? digitsIntLocal : digitsPerWord;
                for (int i = min; i > 0; i--) {
                    int y = x / 10;
                    strIdx--;
                    str[strIdx] = (char)('0' + (char)(x-y*10));
                    x = y;
                }
            }
        } else {
            str[strIdx] = '0';
        }
        return std::string(str);
    }





