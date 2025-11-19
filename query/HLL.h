#ifndef LOGGREP_HLL_H
#define LOGGREP_HLL_H

#include <vector>
#include <stdint.h>
#include <string>
#include <cmath>

class HyperLogLog {
    uint8_t p;
    uint32_t m;
    std::vector<uint8_t> reg;
    static inline uint64_t fnv1a64(const char* data, size_t len){ uint64_t h=1469598103934665603ULL; for(size_t i=0;i<len;i++){ h ^= (uint8_t)data[i]; h *= 1099511628211ULL; } return h; }
    static inline int clz64(uint64_t x){ return x? __builtin_clzll(x) : 64; }
public:
    explicit HyperLogLog(uint8_t precision=12): p(precision), m(1u<<precision), reg(m,0){}
    void add(const char* data, size_t len){ uint64_t h=fnv1a64(data,len); uint32_t idx=(uint32_t)(h >> (64 - p)); uint64_t w = (h << p) | (1ULL << (p-1)); int r = clz64(w) + 1; if(r>255) r=255; if(reg[idx] < (uint8_t)r) reg[idx] = (uint8_t)r; }
    void merge(const HyperLogLog& other){ if(other.m!=m) return; for(uint32_t i=0;i<m;i++){ if(reg[i] < other.reg[i]) reg[i] = other.reg[i]; } }
    double estimate() const { double invSum = 0.0; int V=0; for(uint32_t i=0;i<m;i++){ invSum += std::ldexp(1.0, -(int)reg[i]); if(reg[i]==0) V++; } double alpha; if(m==16) alpha=0.673; else if(m==32) alpha=0.697; else if(m==64) alpha=0.709; else alpha = 0.7213/(1.0 + 1.079/m); double E = alpha * m * m / invSum; if(V>0){ double EC = m * std::log((double)m / V); if(EC <= 2.5*m) return EC; } return E; }
};

#endif