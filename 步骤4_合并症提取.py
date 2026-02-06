#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
步骤4：合并症提取

从诊断文件中提取14种合并症，按admission_key聚合
"""

import pandas as pd
from pathlib import Path
import sys

SEP = "=" * 80
SUB = "-" * 60

def log(msg):
    print(msg)
    sys.stdout.flush()

log(SEP)
log("步骤4：合并症提取")
log(SEP)

BASE_DIR = Path(__file__).resolve().parent  # 项目根目录
HEALTH_DIR = BASE_DIR / "Health"
HYPO_DIR = BASE_DIR / "HYPO"

# ============================================================================
# 13种合并症定义（根据variables.docx命名规范）
# ============================================================================

COMORBIDITIES = {
    "DPVD": {
        "name": "糖尿病周围血管病变",
        "full_name": "Diabetic peripheral vascular disease",
        "logic": "AND",
        "keywords": ["糖尿病", "血管病"]
    },
    "DPN": {
        "name": "糖尿病周围神经病变",
        "full_name": "Diabetic peripheral neuropathy",
        "logic": "AND",
        "keywords": ["糖尿病", "神经病"]
    },
    "DF": {
        "name": "糖尿病足",
        "full_name": "Diabetic foot",
        "logic": "AND",
        "keywords": ["糖尿病", "足"]
    },
    "DN": {
        "name": "糖尿病肾病",
        "full_name": "Diabetic nephropathy",
        "logic": "AND",
        "keywords": ["糖尿病", "肾病"]
    },
    "DR": {
        "name": "糖尿病视网膜病变",
        "full_name": "Diabetic retinopathy",
        "logic": "AND",
        "keywords": ["糖尿病", "视网膜"]
    },
    "HTN": {
        "name": "高血压",
        "full_name": "Hypertension",
        "logic": "OR",
        "keywords": ["高血压"]
    },
    "HL": {
        "name": "高脂血症",
        "full_name": "Hyperlipidemia",
        "logic": "OR",
        "keywords": ["高脂血症", "高胆固醇", "甘油三酯", "血脂异常"]
    },
    "CAD": {
        "name": "冠状动脉粥样硬化性心脏病",
        "full_name": "Coronary artery disease",
        "logic": "OR",
        "keywords": ["冠状动脉", "冠心病", "心肌梗死", "心绞痛"]
    },
    "Malignant_tumor": {
        "name": "恶性肿瘤",
        "full_name": "Malignant tumor",
        "logic": "OR",
        "keywords": ["恶性肿瘤", "癌", "肉瘤", "白血病", "淋巴瘤"]
    },
    "CRF": {
        "name": "慢性肾衰竭",
        "full_name": "Chronic Renal Failure",
        "logic": "OR",
        "keywords": ["慢性肾衰", "尿毒症", "慢性肾脏病4期", "慢性肾脏病5期", "慢性肾脏病 4期", "慢性肾脏病 5期"]
    },
    "RRT": {
        "name": "肾脏替代治疗",
        "full_name": "Renal replacement therapy",
        "logic": "OR",
        "keywords": ["透析", "肾移植"]
    },
    "T1DM": {
        "name": "1型糖尿病",
        "full_name": "Type 1 Diabetes Mellitus",
        "logic": "OR",
        "keywords": ["1型糖尿病", "I型糖尿病"]
    },
    "CVA": {
        "name": "脑血管意外",
        "full_name": "Cerebrovascular accident",
        "logic": "OR",
        "keywords": ["脑卒中", "脑血管意外", "脑梗", "脑出血", "蛛网膜下腔出血", "脑缺血"]
    }
}

# ============================================================================
# 处理函数
# ============================================================================

def extract_comorbidities(diagnosis_file, group_name):
    """从诊断文件中提取合并症"""
    log(f"\n{'='*80}")
    log(f"处理 {group_name} 组")
    log(f"{'='*80}")
    
    if not diagnosis_file.exists():
        log(f"  ⚠️ 未找到: {diagnosis_file}")
        return None
    
    log(f"\n读取: {diagnosis_file.name}")
    df = pd.read_csv(diagnosis_file, low_memory=False)
    
    log(f"  诊断记录数: {len(df):,}")
    log(f"  唯一患者数: {df['admission_key'].nunique():,}")
    
    # 检查诊断名称列
    if 'disease_name' not in df.columns:
        log(f"  ⚠️ 未找到诊断名称列 (disease_name)")
        return None
    
    # 清理诊断名称中的制表符和空格
    df['disease_name'] = df['disease_name'].astype(str).str.strip()
    
    # 获取所有唯一患者
    all_patients = df['admission_key'].unique()
    log(f"\n开始提取14种合并症...")
    log(SEP)
    
    # 初始化结果字典
    comorbidity_results = {patient: {} for patient in all_patients}
    
    # 逐个提取合并症
    for var_code, info in COMORBIDITIES.items():
        patients_with_condition = set()
        matched_diagnoses = set()
        
        if info['logic'] == 'AND':
            # 所有关键词都必须出现
            mask = pd.Series([True] * len(df), index=df.index)
            for keyword in info['keywords']:
                mask = mask & df['disease_name'].str.contains(keyword, case=False, na=False)
            
            matched = df[mask]
            patients_with_condition = set(matched['admission_key'].unique())
            if len(matched) > 0:
                matched_diagnoses = set(matched['disease_name'].unique())
        
        elif info['logic'] == 'OR':
            # 任意关键词出现即可
            mask = pd.Series([False] * len(df), index=df.index)
            for keyword in info['keywords']:
                mask = mask | df['disease_name'].str.contains(keyword, case=False, na=False)
            
            matched = df[mask]
            patients_with_condition = set(matched['admission_key'].unique())
            if len(matched) > 0:
                matched_diagnoses = set(matched['disease_name'].unique())
        
        # 记录结果
        for patient in all_patients:
            comorbidity_results[patient][var_code] = 1 if patient in patients_with_condition else 0
        
        # 输出统计
        count = len(patients_with_condition)
        percentage = count / len(all_patients) * 100 if len(all_patients) > 0 else 0
        log(f"  {var_code:20s} - {info['name']:20s}: {count:6,} 个患者 ({percentage:5.2f}%)")
        
        # 显示匹配到的诊断样例（最多5个）
        if matched_diagnoses and len(matched_diagnoses) <= 5:
            log(f"    匹配诊断: {', '.join(list(matched_diagnoses)[:5])}")
        elif len(matched_diagnoses) > 5:
            log(f"    匹配诊断样例: {', '.join(list(matched_diagnoses)[:3])}... (共{len(matched_diagnoses)}种)")
    
    # 转换为DataFrame
    comorbidity_df = pd.DataFrame.from_dict(comorbidity_results, orient='index')
    comorbidity_df.index.name = 'admission_key'
    comorbidity_df = comorbidity_df.reset_index()
    
    # 列顺序：admission_key + 13个合并症
    column_order = ['admission_key'] + list(COMORBIDITIES.keys())
    comorbidity_df = comorbidity_df[column_order]
    
    log(f"\n生成的合并症特征:")
    log(f"  行数（患者数）: {len(comorbidity_df):,}")
    log(f"  列数: {len(comorbidity_df.columns)} (admission_key + 13个合并症)")
    
    # 统计有任意合并症的患者数
    comorbidity_cols = list(COMORBIDITIES.keys())
    any_comorbidity = (comorbidity_df[comorbidity_cols].sum(axis=1) > 0).sum()
    log(f"\n【汇总】")
    log(f"  有任意合并症的患者: {any_comorbidity:,} ({any_comorbidity/len(comorbidity_df)*100:.2f}%)")
    log(f"  无任何合并症的患者: {len(comorbidity_df)-any_comorbidity:,} ({(len(comorbidity_df)-any_comorbidity)/len(comorbidity_df)*100:.2f}%)")
    
    return comorbidity_df

def add_comorbidities_to_original(diagnosis_file, comorbidity_df, group_name):
    """将合并症列添加到原诊断文件后面"""
    log(f"\n添加合并症列到原文件...")
    
    # 读取原文件
    df_original = pd.read_csv(diagnosis_file, low_memory=False)
    log(f"  原文件行数: {len(df_original):,}")
    log(f"  原文件列数: {len(df_original.columns)}")
    
    # 检查是否已存在合并症列，如果存在则删除（防止重复运行报错）
    comorbidity_cols = [col for col in comorbidity_df.columns if col != 'admission_key']
    existing_cols = [col for col in comorbidity_cols if col in df_original.columns]
    if existing_cols:
        log(f"  ⚠️  检测到已存在的合并症列: {len(existing_cols)}个")
        log(f"     将先删除这些列: {', '.join(existing_cols[:5])}" + ("..." if len(existing_cols) > 5 else ""))
        df_original = df_original.drop(columns=existing_cols)
        log(f"  ✓ 已删除旧列，当前列数: {len(df_original.columns)}")
    
    # 合并合并症列（基于admission_key）
    df_merged = df_original.merge(comorbidity_df, on='admission_key', how='left')
    
    # 填充缺失值为0
    df_merged[comorbidity_cols] = df_merged[comorbidity_cols].fillna(0).astype(int)
    
    log(f"  合并后行数: {len(df_merged):,}")
    log(f"  合并后列数: {len(df_merged.columns)}")
    log(f"  新增列数: {len(comorbidity_cols)} (13个合并症)")
    
    # 保存回原文件
    df_merged.to_csv(diagnosis_file, index=False, encoding='utf-8-sig')
    
    log(f"  ✓ 已保存到: {diagnosis_file}")

# ============================================================================
# 执行
# ============================================================================

# 处理 Health 组
df_health = extract_comorbidities(HEALTH_DIR / "诊断.csv", "Health")
if df_health is not None:
    add_comorbidities_to_original(HEALTH_DIR / "诊断.csv", df_health, "Health")

# 处理 HYPO 组
df_hypo = extract_comorbidities(HYPO_DIR / "诊断.csv", "HYPO")
if df_hypo is not None:
    add_comorbidities_to_original(HYPO_DIR / "诊断.csv", df_hypo, "HYPO")

# ============================================================================
# 保存变量说明
# ============================================================================

log("\n" + SUB)
log("保存变量说明")
log(SUB)

variables_doc = BASE_DIR / "合并症变量说明.txt"
with open(variables_doc, 'w', encoding='utf-8') as f:
    f.write("="*80 + "\n")
    f.write("合并症变量说明\n")
    f.write("="*80 + "\n\n")
    
    f.write("数据来源: 全部诊断.csv (E列：诊断名称)\n\n")
    
    f.write("提取规则:\n")
    f.write("  - AND逻辑: 所有关键词都必须出现在诊断名称中\n")
    f.write("  - OR逻辑: 任意一个关键词出现即可\n\n")
    
    f.write("="*80 + "\n")
    f.write("13种合并症变量（二元变量，0=无，1=有）\n")
    f.write("="*80 + "\n\n")
    
    for idx, (var_code, info) in enumerate(COMORBIDITIES.items(), 1):
        f.write(f"{idx}. {var_code} - {info['name']}\n")
        f.write(f"   英文全称: {info['full_name']}\n")
        f.write(f"   匹配逻辑: {info['logic']}\n")
        f.write(f"   关键词: {', '.join(info['keywords'])}\n")
        f.write("\n")
    
    f.write("="*80 + "\n")
    f.write("输出位置\n")
    f.write("="*80 + "\n\n")
    f.write("合并症列已添加到原文件末尾:\n")
    f.write("  - Health/诊断.csv (新增13列)\n")
    f.write("  - HYPO/诊断.csv (新增13列)\n\n")

log(f"✓ 变量说明: {variables_doc}")

log("\n" + SEP)
log("步骤4 完成：合并症提取")
log(SEP)
log("  修改: Health/诊断.csv, HYPO/诊断.csv (新增13列)")
log("  说明: 合并症变量说明.txt")
log("  下一步: python3 步骤5_手术事件提取.py")
