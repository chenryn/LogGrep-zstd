#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
变量别名发现工具
自动分析压缩文件的模板，推断变量语义，生成别名配置建议
"""

import os
import sys
import re
import argparse
from collections import defaultdict
from typing import List, Dict, Tuple, Set

class VariableDiscoverer:
    """变量发现器，分析模板文件推断变量语义"""
    
    IP_PATTERN = re.compile(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$')
    PORT_PATTERN = re.compile(r'^\d{1,5}$')
    TIME_PATTERNS = [
        re.compile(r'^\d{1,2}:\d{2}:\d{2}$'),
        re.compile(r'^\d{1,2}:\d{2}$'),
        re.compile(r'^\d{4}-\d{2}-\d{2}$'),
        re.compile(r'^\d{2}/\d{2}/\d{4}$'),
    ]
    USERNAME_PATTERN = re.compile(r'^[a-zA-Z0-9_]{3,20}$')
    HOSTNAME_PATTERN = re.compile(r'^[a-zA-Z0-9][a-zA-Z0-9\-\.]*[a-zA-Z0-9]$|^[a-zA-Z0-9]$')
    IPV6_PATTERN = re.compile(r'^([0-9a-fA-F]{0,4}:){2,7}[0-9a-fA-F]{0,4}$')
    MAC_PATTERN = re.compile(r'^([0-9A-Fa-f]{2}[:\-]){5}[0-9A-Fa-f]{2}$')
    EMAIL_PATTERN = re.compile(r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
    URL_PATTERN = re.compile(r'^(https?|ftp)://[^\s]+$')
    UUID_PATTERN = re.compile(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$')
    HEX_LONG_PATTERN = re.compile(r'^[0-9a-fA-F]{16,}$')
    FILEPATH_PATTERN = re.compile(r'^(/|[A-Za-z]:\\).+')
    HTTP_METHODS = {"GET","POST","PUT","DELETE","PATCH","HEAD","OPTIONS"}
    HTTP_STATUS_PATTERN = re.compile(r'^\d{3}$')
    
    def __init__(self, templates_file: str = None, variables_file: str = None, sample_limit: int = 100):
        self.templates_file = templates_file
        self.variables_file = variables_file
        self.sample_limit = sample_limit  # 每个变量采样的最大样本数
        self.templates = {}  # {template_id: template_string}
        self.variables = defaultdict(list)  # {var_id: [sample_values]}
        self.var_positions = defaultdict(list)  # {(template_id, var_index): var_id}
        
    def load_templates(self):
        """加载单个模板文件（向后兼容）"""
        if not self.templates_file or not os.path.exists(self.templates_file):
            print(f"错误: 模板文件不存在: {self.templates_file}")
            return False
        return self.load_templates_file(self.templates_file)

    def load_templates_file(self, path: str):
        """加载指定模板文件并聚合到当前集合"""
        if not os.path.exists(path):
            print(f"错误: 模板文件不存在: {path}")
            return False
        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                # 解析格式: E模板ID 出现次数 模板内容
                parts = line.split(None, 2)
                if len(parts) >= 3:
                    template_id_str = parts[0]
                    if template_id_str.startswith('E'):
                        try:
                            template_id = int(template_id_str[1:])
                            template_content = parts[2]
                            self.templates[template_id] = template_content
                            
                            # 提取变量位置
                            var_matches = re.finditer(r'<V(\d+)>', template_content)
                            for match in var_matches:
                                var_index = int(match.group(1))
                                var_id = (template_id << 16) | (var_index << 8)
                                self.var_positions[(template_id, var_index)].append(var_id)
                        except ValueError:
                            continue
        return True
    
    def load_variable_samples(self):
        """从variables文件加载变量样本值（用于从实际日志值推断）
        
        variables文件格式说明：
        - 每行格式: var_id D type pattern count [values...]
        - 例如: 66560 D 1 <V,1>.<V,1>.<V,1>.<V,1> 15 198
        - var_id是变量ID（编码后的整数）
        - D表示字典类型
        - type是类型
        - pattern是模式（如IP地址模式）
        - count是值的数量
        - 后面跟着实际值
        """
        if not self.variables_file or not os.path.exists(self.variables_file):
            return False
        return self.load_variable_samples_file(self.variables_file)

    def load_variable_samples_file(self, path: str):
        """加载指定variables文件并聚合到当前集合"""
        if not os.path.exists(path):
            return False
        try:
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                current_var_id = None
                sample_count = 0
                in_value_section = False
                
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    
                    parts = line.split()
                    if len(parts) < 2:
                        continue
                    
                    # 检查是否是变量ID行（纯数字开头）
                    try:
                        var_id = int(parts[0])
                        # 检查是否是变量定义行（包含D）
                        if len(parts) >= 3 and parts[1] == 'D':
                            current_var_id = var_id
                            sample_count = 0
                            in_value_section = True
                            
                            # 从第5个部分开始可能是实际值（跳过var_id, D, type, pattern, count）
                            # 但实际值可能在后续行中
                            if len(parts) >= 6:
                                # 尝试从当前行提取值
                                for i in range(5, len(parts)):
                                    value = parts[i]
                                    # 检查各种类型的值
                                    if (self.IP_PATTERN.match(value) or
                                        self.PORT_PATTERN.match(value) or
                                        any(p.match(value) for p in self.TIME_PATTERNS) or
                                        self.USERNAME_PATTERN.match(value) or
                                        (self.HOSTNAME_PATTERN.match(value) and '.' in value)):
                                        if sample_count < self.sample_limit:
                                            self.variables[current_var_id].append(value)
                                            sample_count += 1
                        elif parts[1] == 'V':
                            # V行表示变量值列表的开始
                            # 格式: var_id V count
                            # 后续行包含实际值
                            current_var_id = var_id
                            sample_count = 0
                            in_value_section = True
                        else:
                            # 其他类型的行，重置
                            in_value_section = False
                            current_var_id = None
                    except ValueError:
                        # 不是变量ID行，可能是值行
                        if current_var_id and in_value_section and sample_count < self.sample_limit:
                            # 尝试提取值
                            for part in parts:
                                if sample_count >= self.sample_limit:
                                    break
                                
                                # 检查是否是IP地址
                                if self.IP_PATTERN.match(part):
                                    self.variables[current_var_id].append(part)
                                    sample_count += 1
                                # 检查是否是端口
                                elif self.PORT_PATTERN.match(part):
                                    try:
                                        port = int(part)
                                        if 1 <= port <= 65535:
                                            self.variables[current_var_id].append(part)
                                            sample_count += 1
                                    except ValueError:
                                        pass
                                # 检查是否是时间
                                elif any(pattern.match(part) for pattern in self.TIME_PATTERNS):
                                    self.variables[current_var_id].append(part)
                                    sample_count += 1
                                # 检查是否是用户名（字母数字下划线，非纯数字）
                                elif (self.USERNAME_PATTERN.match(part) and 
                                      not part.isdigit() and 
                                      len(part) >= 3):
                                    self.variables[current_var_id].append(part)
                                    sample_count += 1
                                # 检查是否是主机名（包含点，但不是IP）
                                elif (self.HOSTNAME_PATTERN.match(part) and 
                                      '.' in part and 
                                      not self.IP_PATTERN.match(part)):
                                    self.variables[current_var_id].append(part)
                                    sample_count += 1
        except Exception as e:
            print(f"警告: 加载变量样本时出错: {e}")
            import traceback
            traceback.print_exc()
            return False
        
        return True
    
    def infer_from_values(self, var_id: int) -> Dict[str,int]:
        if var_id not in self.variables or not self.variables[var_id]:
            return {}
        samples = self.variables[var_id]
        type_scores = defaultdict(int)
        for sample in samples[:min(100, len(samples))]:
            s = sample.strip()
            if not s:
                continue
            if self.IP_PATTERN.match(s):
                type_scores['ip'] += 3
            elif self.IPV6_PATTERN.match(s):
                type_scores['ipv6'] += 3
            elif self.MAC_PATTERN.match(s):
                type_scores['mac'] += 3
            elif self.EMAIL_PATTERN.match(s):
                type_scores['email'] += 3
            elif self.URL_PATTERN.match(s):
                type_scores['url'] += 3
            elif self.UUID_PATTERN.match(s):
                type_scores['uuid'] += 3
            elif self.HEX_LONG_PATTERN.match(s):
                type_scores['hash'] += 2
            elif s in self.HTTP_METHODS:
                type_scores['http_method'] += 2
            elif self.HTTP_STATUS_PATTERN.match(s):
                try:
                    code = int(s)
                    if 100 <= code <= 599:
                        type_scores['http_status'] += 2
                except ValueError:
                    pass
            elif self.FILEPATH_PATTERN.match(s) or ('/' in s and not self.URL_PATTERN.match(s)):
                type_scores['path'] += 1
            elif self.PORT_PATTERN.match(s):
                try:
                    port = int(s)
                    if 1 <= port <= 65535:
                        type_scores['port'] += 2
                except ValueError:
                    pass
            elif any(p.match(s) for p in self.TIME_PATTERNS):
                type_scores['time'] += 2
            elif self.USERNAME_PATTERN.match(s) and not s.isdigit():
                type_scores['username'] += 1
            elif self.HOSTNAME_PATTERN.match(s) and '.' in s and not self.IP_PATTERN.match(s):
                type_scores['hostname'] += 1
        return dict(type_scores)
    
    def infer_variable_semantic(self, var_id: int, template_id: int, var_index: int, 
                                template_content: str) -> List[str]:
        """推断变量的语义类型（结合模板上下文和实际值）"""
        semantic_scores = defaultdict(int)
        value_scores = self.infer_from_values(var_id)
        for k,v in value_scores.items():
            semantic_scores[k] += v
        
        pattern = r'<V' + str(var_index) + r'>'
        matches = list(re.finditer(pattern, template_content))
        
        for match in matches:
            # 获取变量前后的文本（各20个字符）
            start = max(0, match.start() - 20)
            end = min(len(template_content), match.end() + 20)
            context = template_content[start:end]
            
            # 基于上下文关键词推断
            context_lower = context.lower()
            
            if any(k in context_lower for k in ['ip','address','rhost','from','remoteaddr']):
                semantic_scores['ip'] += 2
            if any(k in context_lower for k in ['port',':']):
                if re.search(r'\d+\.\d+\.\d+\.\d+', context):
                    semantic_scores['port'] += 2
            if any(k in context_lower for k in ['user','username','logname','uid','account']):
                semantic_scores['username'] += 2
            if any(k in context_lower for k in ['host','hostname','server','node']):
                semantic_scores['hostname'] += 2
            if any(k in context_lower for k in ['time','date','timestamp','ts']):
                semantic_scores['timestamp'] += 2
            if any(k in context_lower for k in ['month','day','year']):
                semantic_scores['date'] += 1
            if any(k in context_lower for k in ['hour','minute','second','ms']):
                semantic_scores['time'] += 1
            if any(k in context_lower for k in ['pid','process']):
                semantic_scores['pid'] += 2
            if any(k in context_lower for k in ['method']):
                semantic_scores['http_method'] += 2
            if any(k in context_lower for k in ['status','code']):
                semantic_scores['http_status'] += 2
            if any(k in context_lower for k in ['uri','url','path']):
                semantic_scores['path'] += 2
        
        if not semantic_scores:
            # 检查模板中变量的顺序模式
            all_vars = re.findall(r'<V(\d+)>', template_content)
            if str(var_index) in all_vars:
                var_pos = all_vars.index(str(var_index))
                
                # 常见模式：时间、主机、用户、IP、端口
                if var_pos == 0:
                    semantic_scores['time'] += 1
                elif var_pos == 1:
                    semantic_scores['time'] += 1
                elif 'LabSZ' in template_content and var_pos < 3:
                    semantic_scores['time'] += 1
        
        # 归一与同义词
        normalize = {
            'timestamp':'time',
            'ipv6':'ip',
        }
        final_scores = defaultdict(int)
        for k,v in semantic_scores.items():
            nk = normalize.get(k,k)
            final_scores[nk] += v
        # 选择分数最高的前2类
        if not final_scores:
            return []
        sorted_types = sorted(final_scores.items(), key=lambda x: (-x[1], x[0]))
        top = [t for t,_ in sorted_types[:2] if _ >= 2]
        if not top:
            top = [sorted_types[0][0]]
        return top
    
    def generate_suggestions(self) -> Dict[str, List[Tuple[int, int]]]:
        """生成别名建议"""
        suggestions = defaultdict(list)  # {alias: [(template_id, var_index), ...]}
        
        # 按模板分组变量
        for (template_id, var_index), var_ids in self.var_positions.items():
            template_content = self.templates.get(template_id, "")
            var_id = var_ids[0] if var_ids else 0
            
            # 推断语义
            semantic_types = self.infer_variable_semantic(var_id, template_id, var_index, template_content)
            if semantic_types:
                for alias in semantic_types:
                    suggestions[alias].append((template_id, var_index))
            else:
                alias = f"var_{template_id}_{var_index}"
                suggestions[alias].append((template_id, var_index))
        
        # 合并相同语义的变量
        merged_suggestions = {}
        for alias, positions in suggestions.items():
            # 如果多个模板中有相同语义的变量，合并它们
            if len(positions) > 1:
                merged_suggestions[alias] = positions
            else:
                merged_suggestions[alias] = positions
        
        return merged_suggestions
    
    def generate_config(self, suggestions: Dict[str, List[Tuple[int, int]]], 
                       output_file: str = None) -> str:
        """生成配置文件内容"""
        lines = [
            "# 变量别名配置文件（自动生成）",
            "# 格式: 别名: E模板_V变量, E模板_V变量, ...",
            "# 本文件由 var_discover.py 自动生成，请根据实际情况调整",
            ""
        ]
        
        # 按别名排序
        for alias in sorted(suggestions.keys()):
            positions = suggestions[alias]
            var_list = []
            for template_id, var_index in positions:
                var_list.append(f"E{template_id}_V{var_index}")
            
            if var_list:
                lines.append(f"{alias}: {', '.join(var_list)}")
        
        config_content = "\n".join(lines)
        
        if output_file:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(config_content)
            print(f"配置已保存到: {output_file}")
        else:
            print(config_content)
        
        return config_content


def main():
    parser = argparse.ArgumentParser(
        description='变量别名发现工具 - 自动分析模板文件，推断变量语义，生成别名配置建议'
    )
    parser.add_argument('compressed_dir', help='压缩文件目录路径')
    parser.add_argument('--output', '-o', help='输出配置文件路径（可选）')
    parser.add_argument('--zip-name', help='压缩文件名（不含.zip），默认使用目录名')
    parser.add_argument('--all', action='store_true', help='聚合目录中所有压缩文件的模板与变量进行跨文件别名发现')
    
    args = parser.parse_args()
    
    # 确定文件路径
    # 首先尝试查找目录中的.templates文件
    templates_file = None
    variables_file = None
    zip_name = None
    
    # 支持聚合多个压缩文件
    template_files = []
    variable_files = []
    for file in os.listdir(args.compressed_dir):
        if file.endswith('.zip.templates'):
            template_files.append(os.path.join(args.compressed_dir, file))
            var_file = os.path.join(args.compressed_dir, file.replace('.zip.templates', '.zip.variables'))
            if os.path.exists(var_file):
                variable_files.append(var_file)
    
    # 如果指定了zip_name，使用指定名称
    if args.zip_name:
        zip_name = args.zip_name
        templates_file = os.path.join(args.compressed_dir, f"{zip_name}.zip.templates")
        variables_file = os.path.join(args.compressed_dir, f"{zip_name}.zip.variables")
    
    if (not template_files) and (not templates_file or not os.path.exists(templates_file)):
        print(f"错误: 找不到模板文件")
        print("提示: 请确保压缩文件目录包含 .zip.templates 文件")
        return 1
    
    if not zip_name:
        zip_name = os.path.basename(args.compressed_dir.rstrip('/'))
    
    # 创建发现器
    discoverer = VariableDiscoverer(templates_file, variables_file)
    
    # 加载模板
    if args.all:
        print(f"正在聚合分析目录内 {len(template_files)} 个模板文件")
        for tf in template_files:
            print(f"  读取: {tf}")
            discoverer.load_templates_file(tf)
        for vf in variable_files:
            print(f"  读取变量样本: {vf}")
            discoverer.load_variable_samples_file(vf)
    else:
        print(f"正在分析模板文件: {templates_file}")
        if not discoverer.load_templates():
            return 1
    
    print(f"发现 {len(discoverer.templates)} 个模板")
    print(f"发现 {len(discoverer.var_positions)} 个变量位置")
    
    # 加载变量样本值（如果可用）
    if not args.all:
        if variables_file and os.path.exists(variables_file):
            print(f"正在加载变量样本值: {variables_file}")
            if discoverer.load_variable_samples():
                total_samples = sum(len(vals) for vals in discoverer.variables.values())
                print(f"加载了 {len(discoverer.variables)} 个变量的 {total_samples} 个样本值")
            else:
                print("警告: 无法加载变量样本值，将仅基于模板上下文推断")
        else:
            print("提示: 未提供variables文件，将仅基于模板上下文推断")
    
    # 生成建议
    print("\n正在推断变量语义...")
    suggestions = discoverer.generate_suggestions()
    
    print(f"\n生成了 {len(suggestions)} 个别名建议:")
    for alias, positions in sorted(suggestions.items()):
        print(f"  {alias}: {len(positions)} 个变量")
        for template_id, var_index in positions[:3]:  # 只显示前3个
            print(f"    - E{template_id}_V{var_index}")
        if len(positions) > 3:
            print(f"    ... 还有 {len(positions) - 3} 个")
    
    # 生成配置
    if args.output:
        output_file = args.output
    else:
        if args.all:
            output_file = os.path.join(args.compressed_dir, "var_alias.conf")
        else:
            output_file = os.path.join(args.compressed_dir, f"{zip_name}.zip.var_alias")
    
    print(f"\n正在生成配置文件...")
    discoverer.generate_config(suggestions, output_file)
    
    print("\n完成！")
    print(f"提示: 请检查生成的配置文件，根据实际情况调整别名名称")
    print(f"提示: 可以使用 var_alias_tool 工具进一步编辑配置")
    
    return 0


if __name__ == '__main__':
    sys.exit(main())


