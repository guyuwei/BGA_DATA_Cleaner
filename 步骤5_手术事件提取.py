#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
步骤5：手术事件提取
从非药品类医嘱中提取手术事件信息

处理规则：
1. 排除医嘱状态为"已撤销"的记录
2. 筛选医嘱项目类型="手术"
3. 从医嘱项目内容中提取手术日期（如"拟2023/6/19 8:18:34..." → 2023-06-19）
4. 按admission_key聚合手术信息
5. 输出：是否手术(Surgery, 0/1) + 手术日期列表(Surgery_dates)
"""

import pandas as pd
import re
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# ============================================================================
# 配置
# ============================================================================

BASE_DIR = Path(__file__).resolve().parent  # 项目根目录
HEALTH_DIR = BASE_DIR / "Health"
HYPO_DIR = BASE_DIR / "HYPO"

SEP = "=" * 80
SUB = "-" * 60

def log(msg):
    print(msg)

# ============================================================================
# 手术事件提取函数
# ============================================================================

def extract_surgery_events(non_drug_file, group_name):
    """
    从非药品类医嘱中提取手术事件
    
    返回: DataFrame with columns [admission_key, Surgery, Surgery_dates]
    """
    
    log(f"\n{'='*80}")
    log(f"处理 {group_name} 组")
    log(f"{'='*80}")
    
    # 读取文件
    log(f"\n读取: 非药品医嘱.csv")
    try:
        df = pd.read_csv(non_drug_file, low_memory=False)
    except Exception as e:
        log(f"❌ 读取失败: {e}")
        return None
    
    log(f"  总记录数: {len(df):,}")
    log(f"  总列数: {len(df.columns)}")
    
    # 检查必需列
    order_status_col = 'order_status'      # 医嘱状态列
    order_type_col = 'order_type'          # 医嘱项目类型列
    order_content_col = 'order_item_name'  # 医嘱项目内容列
    
    # 验证列存在
    missing_cols = []
    if order_type_col not in df.columns:
        missing_cols.append(order_type_col)
    if order_content_col not in df.columns:
        missing_cols.append(order_content_col)
    
    if missing_cols:
        log(f"❌ 缺少必需列: {', '.join(missing_cols)}")
        return None
    
    log(f"\n✓ 找到列映射:")
    log(f"  医嘱状态列: {order_status_col}")
    log(f"  医嘱项目类型列: {order_type_col}")
    log(f"  医嘱项目内容列: {order_content_col}")
    
    # 1. 排除已撤销的医嘱
    if order_status_col:
        cancelled_count = df[order_status_col].astype(str).str.contains('已撤销', case=False, na=False).sum()
        if cancelled_count > 0:
            log(f"\n排除已撤销医嘱:")
            log(f"  已撤销记录数: {cancelled_count:,}")
            df = df[~df[order_status_col].astype(str).str.contains('已撤销', case=False, na=False)]
            log(f"  排除后记录数: {len(df):,}")
    else:
        log(f"\n⚠️  未找到医嘱状态列，跳过撤销检查")
    
    # 2. 筛选医嘱项目类型="手术"
    log(f"\n筛选手术记录:")
    surgery_mask = df[order_type_col].astype(str).str.contains('手术', case=False, na=False)
    df_surgery = df[surgery_mask].copy()
    log(f"  手术记录数: {len(df_surgery):,}")
    
    if len(df_surgery) == 0:
        log(f"  ℹ️  未找到手术记录")
        # 返回空的手术特征（所有患者Surgery=0）
        all_patients = df['admission_key'].unique()
        surgery_df = pd.DataFrame({
            'admission_key': all_patients,
            'Surgery': 0,
            'Surgery_dates': ''
        })
        return surgery_df
    
    # 3. 提取手术日期
    log(f"\n提取手术日期:")
    
    def extract_date_from_content(content):
        """从医嘱项目内容中提取日期"""
        if pd.isna(content):
            return None
        
        content = str(content)
        
        # 正则匹配日期格式：YYYY/M/D 或 YYYY-M-D
        # 例如："拟2023/6/19 8:18:34..." → 2023-06-19
        patterns = [
            r'(\d{4})[/-](\d{1,2})[/-](\d{1,2})',  # YYYY/M/D 或 YYYY-M-D
        ]
        
        for pattern in patterns:
            match = re.search(pattern, content)
            if match:
                year, month, day = match.groups()
                try:
                    # 标准化为 YYYY-MM-DD 格式
                    date_obj = datetime(int(year), int(month), int(day))
                    return date_obj.strftime('%Y-%m-%d')
                except ValueError:
                    continue
        
        return None
    
    # 应用日期提取
    df_surgery['extracted_date'] = df_surgery[order_content_col].apply(extract_date_from_content)
    
    # 统计日期提取情况
    date_extracted_count = df_surgery['extracted_date'].notna().sum()
    log(f"  成功提取日期: {date_extracted_count:,} / {len(df_surgery):,} ({date_extracted_count/len(df_surgery)*100:.1f}%)")
    
    if date_extracted_count > 0:
        # 显示前5个提取示例
        log(f"\n  日期提取示例（前5个）:")
        sample_df = df_surgery[df_surgery['extracted_date'].notna()].head(5)
        for idx, row in sample_df.iterrows():
            content_preview = str(row[order_content_col])[:50] + "..."
            log(f"    内容: {content_preview}")
            log(f"    → 日期: {row['extracted_date']}")
    
    # 4. 按admission_key聚合手术信息
    log(f"\n按患者聚合手术信息:")
    
    surgery_by_patient = defaultdict(lambda: {'dates': set()})
    
    for _, row in df_surgery.iterrows():
        patient_key = row['admission_key']
        date = row['extracted_date']
        if pd.notna(date):
            surgery_by_patient[patient_key]['dates'].add(date)
    
    # 获取所有患者（包括没有手术的）
    all_patients = df['admission_key'].unique()
    
    # 构建结果DataFrame
    results = []
    for patient_key in all_patients:
        if patient_key in surgery_by_patient and surgery_by_patient[patient_key]['dates']:
            # 有手术
            dates = sorted(list(surgery_by_patient[patient_key]['dates']))
            results.append({
                'admission_key': patient_key,
                'Surgery': 1,
                'Surgery_dates': ', '.join(dates)  # 多个日期用逗号分隔
            })
        else:
            # 无手术
            results.append({
                'admission_key': patient_key,
                'Surgery': 0,
                'Surgery_dates': ''
            })
    
    surgery_df = pd.DataFrame(results)
    
    # 统计
    surgery_patients = (surgery_df['Surgery'] == 1).sum()
    log(f"  唯一患者数: {len(surgery_df):,}")
    log(f"  有手术患者: {surgery_patients:,} ({surgery_patients/len(surgery_df)*100:.2f}%)")
    log(f"  无手术患者: {len(surgery_df)-surgery_patients:,} ({(len(surgery_df)-surgery_patients)/len(surgery_df)*100:.2f}%)")
    
    # 显示有手术的患者示例（前3个）
    if surgery_patients > 0:
        log(f"\n  有手术患者示例（前3个）:")
        sample = surgery_df[surgery_df['Surgery'] == 1].head(3)
        for idx, row in sample.iterrows():
            log(f"    {row['admission_key']}: 手术日期 = {row['Surgery_dates']}")
    
    return surgery_df

def add_surgery_to_original(non_drug_file, surgery_df, group_name):
    """将手术信息添加到原非药品医嘱文件"""
    log(f"\n添加手术信息到原文件...")
    
    # 读取原文件
    df_original = pd.read_csv(non_drug_file, low_memory=False)
    log(f"  原文件行数: {len(df_original):,}")
    log(f"  原文件列数: {len(df_original.columns)}")
    
    # 检查是否已存在手术列，如果存在则删除（防止重复运行报错）
    surgery_cols = ['Surgery', 'Surgery_dates']
    existing_cols = [col for col in surgery_cols if col in df_original.columns]
    if existing_cols:
        log(f"  ⚠️  检测到已存在的手术列: {len(existing_cols)}个")
        log(f"     将先删除这些列: {', '.join(existing_cols)}")
        df_original = df_original.drop(columns=existing_cols)
        log(f"  ✓ 已删除旧列，当前列数: {len(df_original.columns)}")
    
    # 合并手术信息（基于admission_key）
    df_merged = df_original.merge(surgery_df, on='admission_key', how='left')
    
    # 填充缺失值
    df_merged['Surgery'] = df_merged['Surgery'].fillna(0).astype(int)
    df_merged['Surgery_dates'] = df_merged['Surgery_dates'].fillna('')
    
    log(f"  合并后行数: {len(df_merged):,}")
    log(f"  合并后列数: {len(df_merged.columns)}")
    log(f"  新增列数: 2 (Surgery, Surgery_dates)")
    
    # 保存回原文件
    df_merged.to_csv(non_drug_file, index=False, encoding='utf-8-sig')
    
    log(f"  ✓ 已保存到: {non_drug_file}")

# ============================================================================
# 生成说明文档
# ============================================================================

def save_documentation():
    """保存手术事件变量说明"""
    doc_file = BASE_DIR / "手术事件变量说明.txt"
    
    with open(doc_file, 'w', encoding='utf-8') as f:
        f.write("="*80 + "\n")
        f.write("手术事件变量说明\n")
        f.write("="*80 + "\n\n")
        
        f.write("数据来源：非药品类医嘱文件\n\n")
        
        f.write("处理规则：\n")
        f.write("1. 排除医嘱状态为'已撤销'的记录\n")
        f.write("2. 筛选医嘱项目类型='手术'\n")
        f.write("3. 从医嘱项目内容中提取手术日期\n")
        f.write("   例如：'拟2023/6/19 8:18:34局部麻醉下行其他永久起搏器置换术'\n")
        f.write("   提取为：2023-06-19\n")
        f.write("4. 认为手术发生在该日期的时间窗（当日08:00 - 次日08:00）\n\n")
        
        f.write("="*80 + "\n")
        f.write("2个手术变量（根据variables.docx命名规范）\n")
        f.write("="*80 + "\n\n")
        
        f.write("1. Surgery - 是否手术\n")
        f.write("   英文全称: Surgery\n")
        f.write("   数据类型: 二元变量 (0=无手术, 1=有手术)\n")
        f.write("   说明: 按admission_key判定，该次住院期间是否有手术记录\n")
        f.write("\n")
        
        f.write("2. Surgery_dates - 手术日期\n")
        f.write("   英文全称: Surgery dates\n")
        f.write("   数据类型: 文本（日期列表，YYYY-MM-DD格式，多个日期用逗号分隔）\n")
        f.write("   说明: 该次住院期间的所有手术日期\n")
        f.write("   示例: '2023-06-19' 或 '2023-06-19, 2023-06-25'（多次手术）\n")
        f.write("\n")
        
        f.write("="*80 + "\n")
        f.write("输出位置\n")
        f.write("="*80 + "\n\n")
        f.write("手术列已添加到原文件末尾:\n")
        f.write("  - Health/非药品医嘱.csv (新增2列)\n")
        f.write("  - HYPO/非药品医嘱.csv (新增2列)\n\n")
    
    log(f"\n{'='*80}")
    log(f"保存变量说明")
    log(f"{'='*80}")
    log(f"✓ 变量说明: {doc_file}")

# ============================================================================
# 执行
# ============================================================================

# 处理 Health 组
df_health_surgery = extract_surgery_events(HEALTH_DIR / "非药品医嘱.csv", "Health")
if df_health_surgery is not None:
    add_surgery_to_original(HEALTH_DIR / "非药品医嘱.csv", df_health_surgery, "Health")

# 处理 HYPO 组
df_hypo_surgery = extract_surgery_events(HYPO_DIR / "非药品医嘱.csv", "HYPO")
if df_hypo_surgery is not None:
    add_surgery_to_original(HYPO_DIR / "非药品医嘱.csv", df_hypo_surgery, "HYPO")

# 生成说明文档
save_documentation()

# ============================================================================
# 完成
# ============================================================================

log("\n" + SEP)
log("步骤5 完成：手术事件提取")
log(SEP)
log("  修改: Health/非药品医嘱.csv, HYPO/非药品医嘱.csv (新增2列: Surgery, Surgery_dates)")
log("  说明: 手术事件变量说明.txt")
log("  下一步: python3 步骤6_禁食营养提取.py")
