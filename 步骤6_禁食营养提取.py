#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
步骤6：禁食和营养记录提取
从非药品类医嘱中提取禁食和肠内/肠外营养信息

处理规则（根据用户图片说明）：
1. 属于"禁食"的记录：
   - 提取开立时间（prescribed_time，原C列）的日期
   - 记为该日8:00至次日8:00的时间窗
   - 例如：2023-07-23 09:21:56 → 时间窗为 2023-07-23 8:00 - 2023-07-24 8:00
   - 只记录 G-F≤24h 的记录（stop_time - start_time ≤ 24小时）

2. 属于"肠内/肠外营养"的记录（不属于禁食）：
   - 时间直接记为 F列至G列（start_time 至 stop_time）
   - 不需要判断G-F时长

输出变量：
- Fasting: 是否有禁食记录 (0/1)
- Fasting_periods: 禁食时间段列表
- Nutrition: 是否有营养记录 (0/1)
- Nutrition_periods: 营养时间段列表
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict
import warnings
warnings.filterwarnings('ignore')

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
# 禁食和营养提取函数
# ============================================================================

def calculate_time_diff_hours(start_time, stop_time):
    """
    计算两个日期之间的小时差（G-F）
    
    参数：
        start_time: 开始时间（F列，已转换为YYYY-MM-DD）
        stop_time: 结束时间（G列，已转换为YYYY-MM-DD）
    
    返回：
        时间差（小时数），如果无法计算则返回None
    """
    try:
        if pd.isna(start_time) or pd.isna(stop_time):
            return None
        
        start = pd.to_datetime(start_time)
        stop = pd.to_datetime(stop_time)
        
        # 计算时间差（天数 * 24）
        diff_days = (stop - start).days
        return diff_days * 24
        
    except Exception as e:
        return None

def extract_fasting_nutrition(non_drug_file, group_name):
    """
    从非药品类医嘱中提取禁食和营养信息
    
    返回: DataFrame with columns [admission_key, Fasting, Fasting_periods, Nutrition, Nutrition_periods]
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
    
    # 检查必需列
    required_cols = ['admission_key', 'order_item_name', 'prescribed_time', 'start_time', 'stop_time']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        log(f"❌ 缺少必需列: {', '.join(missing_cols)}")
        return None
    
    # 1. 提取禁食记录
    log(f"\n{'='*60}")
    log(f"提取禁食记录")
    log(f"{'='*60}")
    
    fasting_mask = df['order_item_name'].astype(str).str.contains('禁食', case=False, na=False)
    df_fasting = df[fasting_mask].copy()
    log(f"  禁食记录数: {len(df_fasting):,}")
    
    # 计算G-F时间差（小时）
    if len(df_fasting) > 0:
        df_fasting['time_diff_hours'] = df_fasting.apply(
            lambda row: calculate_time_diff_hours(row['start_time'], row['stop_time']),
            axis=1
        )
        
        # 只保留G-F≤24h的记录
        valid_fasting = df_fasting[df_fasting['time_diff_hours'] <= 24]
        log(f"  其中 G-F≤24h 的记录: {len(valid_fasting):,}")
        
        if len(valid_fasting) > 0:
            log(f"\n  禁食记录示例（前3个）:")
            for idx, row in valid_fasting.head(3).iterrows():
                log(f"    患者: {row['admission_key']}")
                log(f"      开立时间: {row['prescribed_time']}")
                log(f"      开始时间(F): {row['start_time']}")
                log(f"      结束时间(G): {row['stop_time']}")
                log(f"      时间差(G-F): {row['time_diff_hours']}小时")
                # 计算时间窗（开立时间当日8:00-次日8:00）
                prescribed_date = pd.to_datetime(row['prescribed_time'])
                window_start = prescribed_date.replace(hour=8, minute=0, second=0)
                window_end = window_start + timedelta(days=1)
                log(f"      时间窗: {window_start.strftime('%Y-%m-%d %H:%M')} - {window_end.strftime('%Y-%m-%d %H:%M')}")
    else:
        valid_fasting = pd.DataFrame()
    
    # 2. 提取肠内/肠外营养记录（不包含禁食）
    log(f"\n{'='*60}")
    log(f"提取肠内/肠外营养记录")
    log(f"{'='*60}")
    
    nutrition_keywords = ['肠内', '肠外', '营养']
    nutrition_mask = df['order_item_name'].astype(str).str.contains('|'.join(nutrition_keywords), case=False, na=False)
    # 排除已经是禁食的记录
    nutrition_mask = nutrition_mask & ~fasting_mask
    df_nutrition = df[nutrition_mask].copy()
    log(f"  营养记录数（不含禁食）: {len(df_nutrition):,}")
    
    if len(df_nutrition) > 0:
        log(f"\n  营养记录示例（前3个）:")
        for idx, row in df_nutrition.head(3).iterrows():
            log(f"    患者: {row['admission_key']}")
            log(f"      医嘱内容: {row['order_item_name']}")
            log(f"      时间段: {row['start_time']} 至 {row['stop_time']}")
    
    # 3. 按admission_key聚合
    log(f"\n{'='*60}")
    log(f"按患者聚合禁食和营养信息")
    log(f"{'='*60}")
    
    all_patients = df['admission_key'].unique()
    log(f"  唯一患者数: {len(all_patients):,}")
    
    # 构建结果
    results = []
    
    fasting_by_patient = defaultdict(list)
    if len(valid_fasting) > 0:
        for _, row in valid_fasting.iterrows():
            patient_key = row['admission_key']
            prescribed_date = pd.to_datetime(row['prescribed_time'])
            window_start = prescribed_date.replace(hour=8, minute=0, second=0)
            window_end = window_start + timedelta(days=1)
            period = f"{window_start.strftime('%Y-%m-%d %H:%M')} - {window_end.strftime('%Y-%m-%d %H:%M')}"
            fasting_by_patient[patient_key].append(period)
    
    nutrition_by_patient = defaultdict(list)
    if len(df_nutrition) > 0:
        for _, row in df_nutrition.iterrows():
            patient_key = row['admission_key']
            period = f"{row['start_time']} - {row['stop_time']}"
            nutrition_by_patient[patient_key].append(period)
    
    for patient_key in all_patients:
        fasting_periods = fasting_by_patient.get(patient_key, [])
        nutrition_periods = nutrition_by_patient.get(patient_key, [])
        
        results.append({
            'admission_key': patient_key,
            'Fasting': 1 if fasting_periods else 0,
            'Fasting_periods': '; '.join(fasting_periods) if fasting_periods else '',
            'Nutrition': 1 if nutrition_periods else 0,
            'Nutrition_periods': '; '.join(nutrition_periods) if nutrition_periods else ''
        })
    
    result_df = pd.DataFrame(results)
    
    # 统计
    fasting_count = (result_df['Fasting'] == 1).sum()
    nutrition_count = (result_df['Nutrition'] == 1).sum()
    
    log(f"\n统计:")
    log(f"  有禁食记录的患者: {fasting_count:,} ({fasting_count/len(result_df)*100:.2f}%)")
    log(f"  有营养记录的患者: {nutrition_count:,} ({nutrition_count/len(result_df)*100:.2f}%)")
    
    return result_df

def add_fasting_nutrition_to_original(non_drug_file, fasting_nutrition_df, group_name):
    """将禁食和营养信息添加到原非药品医嘱文件"""
    log(f"\n添加禁食和营养信息到原文件...")
    
    # 读取原文件
    df_original = pd.read_csv(non_drug_file, low_memory=False)
    log(f"  原文件行数: {len(df_original):,}")
    log(f"  原文件列数: {len(df_original.columns)}")
    
    # 检查是否已存在列，如果存在则删除（防止重复运行报错）
    new_cols = ['Fasting', 'Fasting_periods', 'Nutrition', 'Nutrition_periods']
    existing_cols = [col for col in new_cols if col in df_original.columns]
    if existing_cols:
        log(f"  ⚠️  检测到已存在的列: {len(existing_cols)}个")
        log(f"     将先删除这些列: {', '.join(existing_cols)}")
        df_original = df_original.drop(columns=existing_cols)
        log(f"  ✓ 已删除旧列，当前列数: {len(df_original.columns)}")
    
    # 合并信息（基于admission_key）
    df_merged = df_original.merge(fasting_nutrition_df, on='admission_key', how='left')
    
    # 填充缺失值
    df_merged['Fasting'] = df_merged['Fasting'].fillna(0).astype(int)
    df_merged['Nutrition'] = df_merged['Nutrition'].fillna(0).astype(int)
    df_merged['Fasting_periods'] = df_merged['Fasting_periods'].fillna('')
    df_merged['Nutrition_periods'] = df_merged['Nutrition_periods'].fillna('')
    
    log(f"  合并后行数: {len(df_merged):,}")
    log(f"  合并后列数: {len(df_merged.columns)}")
    log(f"  新增列数: 4 (Fasting, Fasting_periods, Nutrition, Nutrition_periods)")
    
    # 保存回原文件
    df_merged.to_csv(non_drug_file, index=False, encoding='utf-8-sig')
    
    log(f"  ✓ 已保存到: {non_drug_file}")

# ============================================================================
# 生成说明文档
# ============================================================================

def save_documentation():
    """保存禁食营养变量说明"""
    doc_file = BASE_DIR / "禁食营养变量说明.txt"
    
    with open(doc_file, 'w', encoding='utf-8') as f:
        f.write("="*80 + "\n")
        f.write("禁食和营养变量说明\n")
        f.write("="*80 + "\n\n")
        
        f.write("数据来源：非药品类医嘱文件\n\n")
        
        f.write("处理规则：\n")
        f.write("1. 禁食记录：\n")
        f.write("   - 筛选条件：order_item_name 包含'禁食'\n")
        f.write("   - 时间差判断：G-F（stop_time - start_time）≤ 24小时\n")
        f.write("   - 时间窗定义：提取开立时间（prescribed_time）的日期\n")
        f.write("                 记为该日8:00至次日8:00\n")
        f.write("   - 示例：开立时间 2023-07-23 09:21:56\n")
        f.write("           → 时间窗 2023-07-23 8:00 - 2023-07-24 8:00\n\n")
        
        f.write("2. 肠内/肠外营养记录：\n")
        f.write("   - 筛选条件：order_item_name 包含'肠内'或'肠外'或'营养'（不含禁食）\n")
        f.write("   - 时间记录：直接记为 F列至G列（start_time 至 stop_time）\n")
        f.write("   - 无时间差限制\n\n")
        
        f.write("="*80 + "\n")
        f.write("4个新增变量\n")
        f.write("="*80 + "\n\n")
        
        f.write("1. Fasting - 是否有禁食\n")
        f.write("   数据类型: 二元变量 (0=无禁食, 1=有禁食)\n")
        f.write("   说明: 按admission_key判定，该次住院期间是否有符合条件的禁食记录\n")
        f.write("\n")
        
        f.write("2. Fasting_periods - 禁食时间段\n")
        f.write("   数据类型: 文本（时间窗列表，多个时间窗用分号分隔）\n")
        f.write("   格式: 'YYYY-MM-DD HH:MM - YYYY-MM-DD HH:MM'\n")
        f.write("   示例: '2023-07-23 08:00 - 2023-07-24 08:00; 2023-07-25 08:00 - 2023-07-26 08:00'\n")
        f.write("\n")
        
        f.write("3. Nutrition - 是否有营养记录\n")
        f.write("   数据类型: 二元变量 (0=无营养, 1=有营养)\n")
        f.write("   说明: 按admission_key判定，该次住院期间是否有肠内/肠外营养记录\n")
        f.write("\n")
        
        f.write("4. Nutrition_periods - 营养时间段\n")
        f.write("   数据类型: 文本（时间段列表，多个时间段用分号分隔）\n")
        f.write("   格式: 'YYYY-MM-DD - YYYY-MM-DD'\n")
        f.write("   示例: '2023-07-20 - 2023-07-25; 2023-07-28 - 2023-07-30'\n")
        f.write("\n")
        
        f.write("="*80 + "\n")
        f.write("输出位置\n")
        f.write("="*80 + "\n\n")
        f.write("禁食营养列已添加到原文件末尾:\n")
        f.write("  - Health/非药品医嘱.csv (新增4列)\n")
        f.write("  - HYPO/非药品医嘱.csv (新增4列)\n\n")
    
    log(f"\n{'='*80}")
    log(f"保存变量说明")
    log(f"{'='*80}")
    log(f"✓ 变量说明: {doc_file}")

# ============================================================================
# 执行
# ============================================================================

# 处理 Health 组
df_health = extract_fasting_nutrition(HEALTH_DIR / "非药品医嘱.csv", "Health")
if df_health is not None:
    add_fasting_nutrition_to_original(HEALTH_DIR / "非药品医嘱.csv", df_health, "Health")

# 处理 HYPO 组
df_hypo = extract_fasting_nutrition(HYPO_DIR / "非药品医嘱.csv", "HYPO")
if df_hypo is not None:
    add_fasting_nutrition_to_original(HYPO_DIR / "非药品医嘱.csv", df_hypo, "HYPO")

# 生成说明文档
save_documentation()

# ============================================================================
# 完成
# ============================================================================

log("\n" + SEP)
log("步骤6 完成：禁食营养提取")
log(SEP)
log("  修改: Health/非药品医嘱.csv, HYPO/非药品医嘱.csv (新增4列: Fasting, Fasting_periods, Nutrition, Nutrition_periods)")
log("  说明: 禁食营养变量说明.txt")
log("  下一步: python3 步骤7_拼接时序大表.py")
