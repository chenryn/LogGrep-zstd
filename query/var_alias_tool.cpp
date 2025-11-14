#include <iostream>
#include <string>
#include <cstring>
#include <cstdlib>
#include <vector>
#include <sstream>
#include <filesystem> // Required for path manipulation
#include "var_alias.h"

void printUsage() {
    std::cout << "变量别名管理工具使用说明：" << std::endl;
    std::cout << "用法: var_alias_tool <命令> [参数...]" << std::endl;
    std::cout << "命令:" << std::endl;
    std::cout << "  list [压缩文件路径]       - 列出所有变量别名" << std::endl;
    std::cout << "  add <变量ID> <别名> [压缩文件路径] - 添加新的变量别名（单个变量）" << std::endl;
    std::cout << "  add-multi <别名> <变量ID列表> [压缩文件路径] - 添加别名到多个变量" << std::endl;
    std::cout << "  示例: add-multi Host E7_V4,E6_V4,E30_V3" << std::endl;
    std::cout << "  get <变量ID> [压缩文件路径] - 获取指定变量ID的别名" << std::endl;
    std::cout << "  find <别名> [压缩文件路径] - 查找指定别名对应的变量ID" << std::endl;
    std::cout << "  reload [压缩文件路径]     - 重新加载别名配置文件" << std::endl;
    std::cout << "  help                    - 显示此帮助信息" << std::endl;
    std::cout << "示例:" << std::endl;
    std::cout << "  var_alias_tool add 1_1.1 src.ip  - 将变量1_1.1设置别名为src.ip" << std::endl;
    std::cout << "  var_alias_tool list            - 列出所有已定义的变量别名" << std::endl;
    std::cout << "  var_alias_tool list /path/to/Ssh - 列出Ssh压缩文件的变量别名" << std::endl;
    std::cout << "  var_alias_tool add 1_1.1 src.ip /path/to/Ssh - 为Ssh压缩文件添加变量别名" << std::endl;
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        printUsage();
        return 1;
    }

    // 初始化变量别名管理器
    VarAliasManager* aliasManager = VarAliasManager::getInstance();
    
    // 检查是否指定了压缩文件路径
    std::string zipPath = "";
    bool hasZipPath = false;
    
    // 根据命令类型确定压缩文件路径参数的位置
    std::string command = argv[1];
    if (command == "list" && argc >= 3) {
        zipPath = argv[2];
        hasZipPath = true;
    } else if (command == "add" && argc >= 5) {
        zipPath = argv[4];
        hasZipPath = true;
    } else if (command == "add-multi" && argc >= 4) {
        // add-multi <别名> <变量ID列表> [压缩文件路径]
        if (argc >= 5) {
            zipPath = argv[4];
            hasZipPath = true;
        }
    } else if ((command == "get" || command == "find") && argc >= 4) {
        zipPath = argv[3];
        hasZipPath = true;
    } else if (command == "reload" && argc >= 3) {
        zipPath = argv[2];
        hasZipPath = true;
    }

    
    // Determine the directory of the executable
    std::filesystem::path executablePath(argv[0]);
    std::filesystem::path executableDir = executablePath.parent_path();
    std::string defaultConfigPath = (executableDir / "var_alias.conf").string();

    // Set the default config path based on the executable's location
    aliasManager->setDefaultConfigPath(defaultConfigPath);

    // Initialize based on whether a zip path is provided
    if (hasZipPath) {
        // Try initializing with the zip-specific path first.
        // initializeForZip will internally use the default path set above if the zip-specific one fails or doesn't exist.
        if (!aliasManager->initializeForZip(zipPath)) {
            // If initializeForZip fails (meaning both zip-specific and the default fallback failed)
            std::cerr << "无法初始化变量别名系统，检查 zip 特定配置或默认配置: " << defaultConfigPath << std::endl;
            return 1;
        }
    } else {
        // No zip path provided, use the executable-relative default path directly
        if (!aliasManager->initialize(defaultConfigPath)) {
            std::cerr << "无法初始化变量别名系统，请检查配置文件是否存在于: " << defaultConfigPath << std::endl;
            return 1;
        }
    }

    if (command == "list") {
        // 列出所有别名
        std::cout << "变量别名列表:" << std::endl;
        std::cout << "变量ID\t\t别名" << std::endl;
        std::cout << "---------------------" << std::endl;
        
        std::map<int, std::string> allAliases = aliasManager->getAllAliases();
        if (allAliases.empty()) {
            std::cout << "没有定义任何变量别名" << std::endl;
        } else {
            for (const auto& pair : allAliases) {
                std::cout << aliasManager->formatVarId(pair.first) << "\t" << pair.second << std::endl;
            }
            std::cout << "共 " << allAliases.size() << " 个变量别名" << std::endl;
        }
    } 
    else if (command == "add") {
        if (argc < 4) {
            std::cerr << "错误: add命令需要变量ID和别名两个参数" << std::endl;
            printUsage();
            return 1;
        }

        std::string varIdStr = argv[2];
        std::string alias = argv[3];

        // 解析变量ID
        int varId = 0;
        if (varIdStr.find('_') != std::string::npos) {
            // 解析E1_V2.1格式
            int e = 0, v = 0, t = 0;
            if (sscanf(varIdStr.c_str(), "%d_%d.%d", &e, &v, &t) == 3) {
                varId = (e << 16) | (v << 8) | t;
            }
        } else {
            // 纯数字格式
            varId = atoi(varIdStr.c_str());
        }

        if (varId <= 0) {
            std::cerr << "错误: 无效的变量ID格式" << std::endl;
            return 1;
        }

        if (aliasManager->addAlias(varId, alias)) {
            std::cout << "成功添加别名: " << varIdStr << " -> " << alias << std::endl;
        } else {
            std::cerr << "添加别名失败" << std::endl;
            return 1;
        }
    }
    else if (command == "add-multi") {
        if (argc < 4) {
            std::cerr << "错误: add-multi命令需要别名和变量ID列表两个参数" << std::endl;
            printUsage();
            return 1;
        }

        std::string alias = argv[2];
        std::string varIdsStr = argv[3];
        
        // 解析变量ID列表（逗号分隔）
        std::vector<int> varIds;
        std::istringstream iss(varIdsStr);
        std::string token;
        while (std::getline(iss, token, ',')) {
            // 去除空白
            token.erase(0, token.find_first_not_of(" \t"));
            token.erase(token.find_last_not_of(" \t") + 1);
            
            if (token.empty()) continue;
            
            int varId = 0;
            // 解析 E模板_V变量 格式
            if (token.find('E') == 0 && token.find('_') != std::string::npos) {
                int e = 0, v = 0;
                if (sscanf(token.c_str(), "E%d_V%d", &e, &v) == 2) {
                    varId = (e << 16) | (v << 8);
                }
            }
            // 解析 模板_变量.类型 格式
            else if (token.find('_') != std::string::npos) {
                int e = 0, v = 0, t = 0;
                if (sscanf(token.c_str(), "%d_%d.%d", &e, &v, &t) == 3) {
                    varId = (e << 16) | (v << 8);
                }
            }
            
            if (varId > 0) {
                varIds.push_back(varId);
            } else {
                std::cerr << "警告: 无法解析变量ID: " << token << std::endl;
            }
        }
        
        if (varIds.empty()) {
            std::cerr << "错误: 没有有效的变量ID" << std::endl;
            return 1;
        }
        
        if (aliasManager->addAliasToVars(alias, varIds)) {
            std::cout << "成功添加别名: " << alias << " -> ";
            for (size_t i = 0; i < varIds.size(); i++) {
                int varId = varIds[i];
                int e = varId >> 16;
                int v = (varId >> 8) & 0xFF;
                std::cout << "E" << e << "_V" << v;
                if (i < varIds.size() - 1) std::cout << ", ";
            }
            std::cout << std::endl;
        } else {
            std::cerr << "添加别名失败" << std::endl;
            return 1;
        }
    } 
    else if (command == "get") {
        if (argc < 3) {
            std::cerr << "错误: get命令需要变量ID参数" << std::endl;
            printUsage();
            return 1;
        }

        std::string varIdStr = argv[2];
        
        // 解析变量ID
        int varId = 0;
        if (varIdStr.find('_') != std::string::npos) {
            // 解析E1_V2.1格式
            int e = 0, v = 0, t = 0;
            if (sscanf(varIdStr.c_str(), "%d_%d.%d", &e, &v, &t) == 3) {
                varId = (e << 16) | (v << 8) | t;
            }
        } else {
            // 纯数字格式
            varId = atoi(varIdStr.c_str());
        }

        if (varId <= 0) {
            std::cerr << "错误: 无效的变量ID格式" << std::endl;
            return 1;
        }

        std::string alias = aliasManager->getAlias(varId);
        if (!alias.empty()) {
            std::cout << "变量 " << varIdStr << " 的别名是: " << alias << std::endl;
        } else {
            std::cout << "变量 " << varIdStr << " 没有定义别名" << std::endl;
        }
    } 
    else if (command == "find") {
        if (argc < 3) {
            std::cerr << "错误: find命令需要别名参数" << std::endl;
            printUsage();
            return 1;
        }

        std::string alias = argv[2];
        int varId = aliasManager->getVarId(alias);

        if (varId > 0) {
            // 格式化变量ID为E1_V2.1格式
            int e = varId >> 16;
            int v = (varId >> 8) & 0xFF;
            int t = varId & 0x0F;
            std::cout << "别名 " << alias << " 对应的变量ID是: " << e << "_" << v << "." << t << " (" << varId << ")" << std::endl;
        } else {
            std::cout << "没有找到别名 " << alias << " 对应的变量" << std::endl;
        }
    } 
    else if (command == "reload") {
        if (aliasManager->loadAliasConfig(aliasManager->getConfigPath())) {
            std::cout << "成功重新加载变量别名配置" << std::endl;
        } else {
            std::cerr << "重新加载变量别名配置失败" << std::endl;
            return 1;
        }
    } 
    else if (command == "help") {
        printUsage();
    } 
    else {
        std::cerr << "错误: 未知命令 '" << command << "'" << std::endl;
        printUsage();
        return 1;
    }

    return 0;
}