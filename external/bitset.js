var BitSet = (function() {
    var HAMMING_TABLE;

    HAMMING_TABLE = [0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4];

    function BitSet() {
      this.bitsPerWord = 32;
      this.addressBitsPerWord = 5;
      this.store = [];
    }

    BitSet.prototype.wordIndex = function(pos) {
      return pos >> this.addressBitsPerWord;
    };

    BitSet.prototype.set = function(pos) {
      return this.store[this.wordIndex(pos)] |= 1 << pos;
    };

    BitSet.prototype.clear = function(pos) {
      return this.store[this.wordIndex(pos)] &= 0xFF ^ (1 << pos);
    };

    BitSet.prototype.get = function(pos) {
      return (this.store[this.wordIndex(pos)] & (1 << pos)) !== 0;
    };

    BitSet.prototype.length = function() {
      if (this.wordLength() === 0) {
        return 0;
      } else {
        return this.bitsPerWord * (this.wordLength() - 1) + (this.store[this.wordLength() - 1].toString(2).length);
      }
    };

    BitSet.prototype.wordLength = function() {
      var length, pos, _i, _ref;
      length = this.store.length;
      for (pos = _i = _ref = this.store.length - 1; _ref <= 0 ? _i <= 0 : _i >= 0; pos = _ref <= 0 ? ++_i : --_i) {
        if (this.store[pos] !== 0) {
          break;
        }
        length--;
      }
      return length;
    };

    BitSet.prototype.cardinality = function() {
      var sum, word, _i, _len, _ref;
      sum = 0;
      _ref = this.store;
      for (_i = 0, _len = _ref.length; _i < _len; _i++) {
        word = _ref[_i];
        sum += HAMMING_TABLE[(word >> 0x00) & 0xF];
        sum += HAMMING_TABLE[(word >> 0x04) & 0xF];
        sum += HAMMING_TABLE[(word >> 0x08) & 0xF];
        sum += HAMMING_TABLE[(word >> 0x0C) & 0xF];
        sum += HAMMING_TABLE[(word >> 0x10) & 0xF];
        sum += HAMMING_TABLE[(word >> 0x14) & 0xF];
        sum += HAMMING_TABLE[(word >> 0x18) & 0xF];
        sum += HAMMING_TABLE[(word >> 0x1C) & 0xF];
      }
      return sum;
    };

    BitSet.prototype.toString = function() {
      var pos, result, _i, _ref;
      result = [];
      for (pos = _i = 0, _ref = this.length(); 0 <= _ref ? _i <= _ref : _i >= _ref; pos = 0 <= _ref ? ++_i : --_i) {
        if (this.get(pos)) {
          result.push(pos);
        }
      }
      return "{" + (result.join(",")) + "}";
    };

    BitSet.prototype.toBinaryString = function() {
      var lpad,
        _this = this;
      lpad = function(str, padString, length) {
        while (str.length < length) {
          str = padString + str;
        }
        return str;
      };
      if (this.wordLength() > 0) {
        return this.store.map(function(word) {
          return lpad(word.toString(2), '0', _this.bitsPerWord);
        }).join('');
      } else {
        return lpad('', 0, this.bitsPerWord);
      }
    };

    BitSet.prototype.or = function(set) {
      var pos, wordsInCommon, _i, _ref;
      if (this === set) {
        return;
      }
      wordsInCommon = Math.min(this.wordLength(), set.wordLength());
      for (pos = _i = 0, _ref = wordsInCommon - 1; 0 <= _ref ? _i <= _ref : _i >= _ref; pos = 0 <= _ref ? ++_i : --_i) {
        this.store[pos] |= set.store[pos];
      }
      if (wordsInCommon < set.wordLength()) {
        this.store = this.store.concat(set.store.slice(wordsInCommon, set.wordLength()));
      }
      return null;
    };

    BitSet.prototype.and = function(set) {
      var pos, _i, _j, _ref, _ref1, _ref2;
      if (this === set) {
        return;
      }
      for (pos = _i = _ref = this.wordLength, _ref1 = set.wordLength(); _ref <= _ref1 ? _i <= _ref1 : _i >= _ref1; pos = _ref <= _ref1 ? ++_i : --_i) {
        this.store[pos] = 0;
      }
      for (pos = _j = 0, _ref2 = this.wordLength(); 0 <= _ref2 ? _j <= _ref2 : _j >= _ref2; pos = 0 <= _ref2 ? ++_j : --_j) {
        this.store[pos] &= set.store[pos];
      }
      return null;
    };

    BitSet.prototype.andNot = function(set) {
      var pos, _i, _ref;
      for (pos = _i = 0, _ref = Math.min(this.wordLength(), set.wordLength) - 1; 0 <= _ref ? _i <= _ref : _i >= _ref; pos = 0 <= _ref ? ++_i : --_i) {
        this.store[pos] &= ~set.store[pos];
      }
      return null;
    };

    BitSet.prototype.xor = function(set) {
      var pos, _i, _ref;
      if (this === set) {
        return;
      }
      for (pos = _i = 0, _ref = this.wordLength(); 0 <= _ref ? _i <= _ref : _i >= _ref; pos = 0 <= _ref ? ++_i : --_i) {
        this.store[pos] ^= set.store[pos];
      }
      return null;
    };

    return BitSet;

  })();
