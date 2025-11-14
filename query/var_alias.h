#ifndef VAR_ALIAS_H
#define VAR_ALIAS_H

#include <map>
#include <string>
#include <fstream>
#include <iostream>
#include <sstream>
#include <filesystem>
#include <vector>
#include <algorithm>
#include "LogStructure.h"

// 变量ID位置定义
#define POS_TEMPLATE 16
#define POS_VAR 8

/**
 * 变量别名系统 - 为<V,n>格式的变量提供有意义的名称
 * 例如将E1_V2.1这样的变量ID映射为"src.ip"等更有意义的名称
 */
class VarAliasManager {
private:
    // 变量ID到别名的映射
    std::map<int, std::string> varIdToAlias;
    // 别名到变量ID的映射
    std::map<std::string, int> aliasToVarId;
    std::map<std::string, std::vector<int> > aliasToVarIds;
    // 全局配置文件路径 (used as default if not overridden)
    std::string configPath;
    // Default config path (e.g., relative to executable)
    std::string defaultConfigPath;
    // 当前使用的压缩文件路径
    std::string currentZipPath;
    // 单例实例
    static VarAliasManager* instance;

    // 私有构造函数（单例模式）
    VarAliasManager() {}

public:
    // 获取单例实例
    static VarAliasManager* getInstance() {
        if (instance == nullptr) {
            instance = new VarAliasManager();
        }
        return instance;
    }

    // Set the default config path (e.g., relative to executable)
    void setDefaultConfigPath(const std::string& path) {
        defaultConfigPath = path;
    }

    // 初始化别名系统，从配置文件加载
    bool initialize(const std::string& configFile) {
        configPath = configFile;
        // If specific config doesn't exist, fallback to the default config path
        return loadAliasConfig(defaultConfigPath.empty() ? configPath : defaultConfigPath);
    }
    
    // 根据压缩文件路径初始化别名系统
    bool initializeForZip(const std::string& zipPath) {
        currentZipPath = zipPath;
        
        // 尝试加载特定于该压缩文件的别名配置
        std::string zipSpecificConfig = getZipSpecificConfigPath(zipPath);
        
        // 如果特定配置文件存在，则加载它
        if (std::ifstream(zipSpecificConfig).good()) {
            configPath = zipSpecificConfig;  // 修复Bug：更新configPath为文件级配置路径
            return loadAliasConfig(zipSpecificConfig);
        }
        
        // 如果特定配置不存在，回退到全局配置
        // If specific config doesn't exist, fallback to the default config path
        std::string fallbackPath = defaultConfigPath.empty() ? configPath : defaultConfigPath;
        configPath = fallbackPath;  // 修复Bug：确保configPath被正确设置
        return loadAliasConfig(fallbackPath);
    }

    // 获取压缩文件特定的配置文件路径
    std::string getZipSpecificConfigPath(const std::string& zipPath) {
        // 从压缩文件路径中提取目录和文件名
        size_t lastSlash = zipPath.find_last_of("/\\");
        std::string zipDir = (lastSlash != std::string::npos) ? zipPath.substr(0, lastSlash + 1) : "";
        std::string zipName = (lastSlash != std::string::npos) ? zipPath.substr(lastSlash + 1) : zipPath;
        
        // 构建特定配置文件路径 (zipPath/zipName.var_alias)
        return zipDir + zipName + ".var_alias";
    }
    
    // 从配置文件加载别名配置
    // 统一格式：只支持"别名: E模板_V变量, ..."格式，兼容旧的"变量ID=别名"格式（向后兼容）
    bool loadAliasConfig(const std::string& configFilePath) {
        std::ifstream file(configFilePath.c_str());
        if (!file.is_open()) {
            std::cerr << "无法打开变量别名配置文件: " << configFilePath << std::endl;
            return false;
        }

        varIdToAlias.clear();
        aliasToVarId.clear();
        aliasToVarIds.clear();

        std::string line;
        int lineNum = 0;
        while (std::getline(file, line)) {
            lineNum++;
            // 跳过注释和空行
            if (line.empty() || line[0] == '#') {
                continue;
            }

            // 优先尝试新格式：别名: E模板_V变量, ...
            size_t colonPos = line.find(':');
            if (colonPos != std::string::npos && colonPos > 0) {
                std::string alias = line.substr(0, colonPos);
                std::string ids = line.substr(colonPos + 1);
                
                // 去除首尾空白
                alias.erase(0, alias.find_first_not_of(" \t"));
                alias.erase(alias.find_last_not_of(" \t") + 1);
                ids.erase(0, ids.find_first_not_of(" \t"));
                ids.erase(ids.find_last_not_of(" \t") + 1);
                
                if (!alias.empty() && !ids.empty()) {
                    std::vector<int> vars;
                    std::istringstream idss(ids);
                    std::string token;
                    while (std::getline(idss, token, ',')) {
                        token.erase(0, token.find_first_not_of(" \t"));
                        token.erase(token.find_last_not_of(" \t") + 1);
                        
                        int e = 0, v = 0, t = 0;
                        // 支持 E模板_V变量 格式
                        if (sscanf(token.c_str(), "E%d_V%d", &e, &v) == 2) {
                            int varId = (e << POS_TEMPLATE) | (v << POS_VAR);
                            vars.push_back(varId);
                            // 同时更新varIdToAlias（用于向后兼容）
                            varIdToAlias[varId] = alias;
                            // 如果别名还没有单一映射，设置第一个变量ID
                            if (aliasToVarId.find(alias) == aliasToVarId.end()) {
                                aliasToVarId[alias] = varId;
                            }
                        }
                        // 支持 模板_变量.类型 格式（向后兼容）
                        else if (sscanf(token.c_str(), "%d_%d.%d", &e, &v, &t) == 3) {
                            int varId = (e << POS_TEMPLATE) | (v << POS_VAR);
                            vars.push_back(varId);
                            varIdToAlias[varId] = alias;
                            if (aliasToVarId.find(alias) == aliasToVarId.end()) {
                                aliasToVarId[alias] = varId;
                            }
                        }
                    }
                    if (!vars.empty()) {
                        aliasToVarIds[alias] = vars;
                    }
                    continue;
                }
            }
            
            // 向后兼容：支持旧的"变量ID=别名"格式
            std::istringstream iss(line);
            std::string left, right;
            if (std::getline(iss, left, '=') && std::getline(iss, right)) {
                int varId = 0;
                if (left.find('_') != std::string::npos) {
                    int e = 0, v = 0, t = 0;
                    if (sscanf(left.c_str(), "%d_%d.%d", &e, &v, &t) == 3) {
                        varId = (e << POS_TEMPLATE) | (v << POS_VAR);
                    }
                } else {
                    varId = atoi(left.c_str());
                }
                right.erase(0, right.find_first_not_of(" \t"));
                right.erase(right.find_last_not_of(" \t") + 1);
                if (varId > 0 && !right.empty()) {
                    varIdToAlias[varId] = right;
                    aliasToVarId[right] = varId;
                    aliasToVarIds[right].push_back(varId);
                }
            }
        }

        file.close();
        return true;
    }

    // 获取变量的别名，如果没有别名则返回原始格式化名称
    std::string getAlias(int varId) {
        if (varIdToAlias.find(varId) != varIdToAlias.end()) {
            return varIdToAlias[varId];
        }
        return "";
    }

    // 根据别名获取变量ID
    int getVarId(const std::string& alias) {
        if (aliasToVarId.find(alias) != aliasToVarId.end()) {
            return aliasToVarId[alias];
        }
        return -1;
    }

    std::vector<int> getVarIds(const std::string& alias) {
        if (aliasToVarIds.find(alias) != aliasToVarIds.end()) {
            return aliasToVarIds[alias];
        }
        std::vector<int> ret;
        int v = getVarId(alias);
        if (v > 0) ret.push_back(v);
        return ret;
    }

    // 添加新的别名（单个变量）
    bool addAlias(int varId, const std::string& alias) {
        if (varId <= 0 || alias.empty()) {
            return false;
        }

        varIdToAlias[varId] = alias;
        aliasToVarId[alias] = varId;
        aliasToVarIds[alias].push_back(varId);
        return saveAliasConfig();
    }
    
    // 添加别名到多个变量（一对多映射）
    // 优化：自动合并已存在的别名，避免重复定义
    bool addAliasToVars(const std::string& alias, const std::vector<int>& varIds) {
        if (alias.empty() || varIds.empty()) {
            return false;
        }
        
        // 检查是否有变量ID已经被其他别名使用
        std::vector<int> newVarIds;
        std::vector<std::string> conflictingAliases;
        
        for (int varId : varIds) {
            if (varId <= 0) continue;
            
            // 检查变量ID是否已有别名
            if (varIdToAlias.find(varId) != varIdToAlias.end()) {
                std::string existingAlias = varIdToAlias[varId];
                if (existingAlias != alias) {
                    // 发现冲突：变量ID已有其他别名
                    conflictingAliases.push_back(existingAlias);
                    // 从旧别名中移除该变量ID
                    auto& oldVarIds = aliasToVarIds[existingAlias];
                    oldVarIds.erase(
                        std::remove(oldVarIds.begin(), oldVarIds.end(), varId),
                        oldVarIds.end()
                    );
                    // 如果旧别名没有变量了，清理它
                    if (oldVarIds.empty()) {
                        aliasToVarIds.erase(existingAlias);
                        aliasToVarId.erase(existingAlias);
                    }
                }
            }
            
            // 添加到新别名
            varIdToAlias[varId] = alias;
            newVarIds.push_back(varId);
        }
        
        // 合并到aliasToVarIds（去重）
        if (aliasToVarIds.find(alias) != aliasToVarIds.end()) {
            // 合并现有变量ID列表
            auto& existingVarIds = aliasToVarIds[alias];
            for (int varId : newVarIds) {
                // 检查是否已存在（去重）
                if (std::find(existingVarIds.begin(), existingVarIds.end(), varId) == existingVarIds.end()) {
                    existingVarIds.push_back(varId);
                }
            }
        } else {
            // 新别名，直接添加
            aliasToVarIds[alias] = newVarIds;
        }
        
        // 设置第一个变量ID作为单一映射（向后兼容）
        if (!newVarIds.empty() && newVarIds[0] > 0) {
            aliasToVarId[alias] = newVarIds[0];
        }
        
        // 如果有冲突，输出警告
        if (!conflictingAliases.empty()) {
            std::cerr << "警告: 以下变量ID的别名已从旧别名移动到新别名:" << std::endl;
            for (const auto& oldAlias : conflictingAliases) {
                std::cerr << "  " << oldAlias << " -> " << alias << std::endl;
            }
        }
        
        return saveAliasConfig();
    }

    // 保存别名配置到文件
    // 统一格式：使用"别名: E模板_V变量, ..."格式，支持一对多映射
    bool saveAliasConfig() {
        if (configPath.empty()) {
            std::cerr << "错误: 配置文件路径未设置" << std::endl;
            return false;
        }
        
        std::ofstream file(configPath.c_str());
        if (!file.is_open()) {
            std::cerr << "无法写入变量别名配置文件: " << configPath << std::endl;
            return false;
        }

        file << "# 变量别名配置文件\n";
        file << "# 格式: 别名: E模板_V变量, E模板_V变量, ...\n";
        file << "# 示例: Host: E7_V4, E6_V4, E30_V3\n";
        file << "#        Port: E7_V3, E6_V3\n\n";

        // 按别名分组，保存为一对多格式
        std::map<std::string, std::vector<int> > aliasGroups;
        
        // 首先从aliasToVarIds获取一对多映射
        for (const auto& pair : aliasToVarIds) {
            aliasGroups[pair.first] = pair.second;
        }
        
        // 然后处理varIdToAlias中的单一映射（可能不在aliasToVarIds中）
        for (const auto& pair : varIdToAlias) {
            std::string alias = pair.second;
            int varId = pair.first;
            
            // 如果别名已经在aliasGroups中，检查varId是否已包含
            if (aliasGroups.find(alias) != aliasGroups.end()) {
                bool found = false;
                for (int vid : aliasGroups[alias]) {
                    if (vid == varId) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    aliasGroups[alias].push_back(varId);
                }
            } else {
                // 新别名，创建映射
                aliasGroups[alias] = {varId};
            }
        }

        // 按别名排序后写入
        for (const auto& pair : aliasGroups) {
            std::string alias = pair.first;
            std::vector<int> varIds = pair.second;
            
            if (varIds.empty()) continue;
            
            file << alias << ": ";
            for (size_t i = 0; i < varIds.size(); i++) {
                int varId = varIds[i];
                int e = varId >> POS_TEMPLATE;
                int v = (varId >> POS_VAR) & 0xFF;
                file << "E" << e << "_V" << v;
                if (i < varIds.size() - 1) {
                    file << ", ";
                }
            }
            file << "\n";
        }

        file.close();
        return true;
    }
    
    // 获取所有变量别名
    std::map<int, std::string> getAllAliases() {
        return varIdToAlias;
    }
    
    // 获取当前配置文件路径
    std::string getConfigPath() {
        return configPath;
    }
    
    // 格式化变量ID为可读格式
    std::string formatVarId(int varId) {
        std::string result;
        char buffer[32];
        
        if (varId <= 15) {
            sprintf(buffer, "%d", varId);
            result = buffer;
        } else {
            int e = varId >> POS_TEMPLATE;
            int v = (varId >> POS_VAR) & 0xFF;
            int t = varId & 0x0F;
            
            if (t == VAR_TYPE_SUB) {
                int s = (varId >> 4) & 0x0F;
                sprintf(buffer, "%d_%d~%d.%d", e, v, s, t);
            } else {
                sprintf(buffer, "%d_%d.%d", e, v, t);
            }
            result = buffer;
        }
        
        return result;
    }
};

 

#endif // VAR_ALIAS_H
