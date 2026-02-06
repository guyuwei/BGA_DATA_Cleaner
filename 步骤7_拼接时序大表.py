#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
步骤7：拼接时序预测大表（每天一条记录）

数据结构：
- 每个admission_key的每一天作为一条记录
- 特征：当天的所有指标（Demographics, Comorbidities, Lab, Medications, Vitals, Glucose）
- 结局：次日的血糖分类
  * 低血糖：次日任一血糖 < 3.9 mmol/L
  * 高血糖：次日任一血糖 > 13.9 mmol/L
  * 正常/中风险：其余情况

输出：
- 合并Health和HYPO的完整时序大表
- 每行代表某患者某天的数据和次日结局
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import re
import io
import warnings
warnings.filterwarnings('ignore')

SEP = "=" * 80
SUB = "-" * 60

# ============================================================================
# 配置
# ============================================================================

BASE_DIR = Path(__file__).resolve().parent  # 项目根目录
HEALTH_DIR = BASE_DIR / "Health"
HYPO_DIR = BASE_DIR / "HYPO"

def log(msg):
    """打印日志"""
    print(msg)

# ============================================================================
# 数据读取函数
# ============================================================================

def load_file_safely(filepath, file_desc):
    """安全读取文件"""
    try:
        if filepath.exists():
            df = pd.read_csv(filepath, low_memory=False)
            log(f"  ✓ {file_desc}: {len(df):,}行")
            return df
        else:
            log(f"  ⚠️  {file_desc}: 文件不存在")
            return None
    except Exception as e:
        log(f"  ❌ {file_desc}: 读取失败 - {e}")
        return None

# ============================================================================
# 血糖数据处理：构建每日血糖记录
# ============================================================================

def process_glucose_data(df_glucose):
    """
    处理血糖数据，构建每日血糖统计
    返回：每个admission_key每天的血糖统计 + 次日结局
    """
    log(f"\n处理血糖数据...")
    
    if df_glucose is None or df_glucose.empty:
        return pd.DataFrame()
    
    # 确保有必需列
    if 'admission_key' not in df_glucose.columns or 'blood_sugar' not in df_glucose.columns:
        log(f"  ❌ 缺少必需列")
        return pd.DataFrame()
    
    # 重命名血糖列
    df = df_glucose.copy()
    df = df.rename(columns={'blood_sugar': 'glucose_value'})
    
    # 转换日期列
    if 'exam_time' in df.columns:
        df['date'] = pd.to_datetime(df['exam_time'], errors='coerce').dt.date
    else:
        log(f"  ❌ 缺少exam_time列")
        return pd.DataFrame()
    
    # 删除无效记录
    df = df.dropna(subset=['date', 'glucose_value'])
    
    # 转换血糖值为数值
    df['glucose_value'] = pd.to_numeric(df['glucose_value'], errors='coerce')
    df = df.dropna(subset=['glucose_value'])
    
    log(f"  有效血糖记录: {len(df):,}条")
    
    # 按患者和日期分组，计算每日血糖统计
    daily_glucose = df.groupby(['admission_key', 'date']).agg({
        'glucose_value': [
            ('min_glucose', 'min'),
            ('max_glucose', 'max'),
            ('mean_glucose', 'mean'),
            ('std_glucose', 'std'),
            ('count_glucose', 'count'),
        ]
    }).reset_index()
    
    # 展平列名
    daily_glucose.columns = ['admission_key', 'date', 'min_glucose', 'max_glucose', 
                             'mean_glucose', 'std_glucose', 'count_glucose']
    
    # 计算CV
    daily_glucose['cv_glucose'] = (daily_glucose['std_glucose'] / daily_glucose['mean_glucose'] * 100).round(2)
    
    log(f"  每日血糖统计: {len(daily_glucose):,}条")
    
    # 计算次日结局
    log(f"\n计算次日血糖结局...")
    
    # 按患者排序
    daily_glucose = daily_glucose.sort_values(['admission_key', 'date']).reset_index(drop=True)
    
    # 创建次日日期
    daily_glucose['next_date'] = daily_glucose['date'].apply(
        lambda x: (pd.to_datetime(x) + timedelta(days=1)).date()
    )
    
    # 合并次日的最低和最高血糖
    next_day_glucose = daily_glucose[['admission_key', 'date', 'min_glucose', 'max_glucose']].copy()
    next_day_glucose.columns = ['admission_key', 'next_date', 'next_min_glucose', 'next_max_glucose']
    
    daily_glucose = daily_glucose.merge(
        next_day_glucose,
        left_on=['admission_key', 'next_date'],
        right_on=['admission_key', 'next_date'],
        how='left'
    )
    
    # 定义次日结局（三分类）
    def classify_outcome(row):
        if pd.isna(row['next_min_glucose']):
            return np.nan  # 无次日数据
        elif row['next_min_glucose'] < 3.9:
            return 0  # 低血糖
        elif row['next_max_glucose'] > 13.9:
            return 2  # 高血糖
        else:
            return 1  # 正常/中风险
    
    daily_glucose['outcome'] = daily_glucose.apply(classify_outcome, axis=1)
    
    # 统计结局分布
    outcome_counts = daily_glucose['outcome'].value_counts().sort_index()
    log(f"\n  次日结局分布:")
    log(f"    低血糖（<3.9）  : {outcome_counts.get(0, 0):,}天")
    log(f"    正常/中风险      : {outcome_counts.get(1, 0):,}天")
    log(f"    高血糖（>13.9） : {outcome_counts.get(2, 0):,}天")
    log(f"    缺失次日数据     : {daily_glucose['outcome'].isna().sum():,}天")
    
    # 删除无次日数据的记录
    daily_glucose = daily_glucose[daily_glucose['outcome'].notna()].copy()
    
    # 删除辅助列
    daily_glucose = daily_glucose.drop(columns=['next_date', 'next_min_glucose', 'next_max_glucose'])
    
    log(f"\n  最终记录数: {len(daily_glucose):,}条（有明确次日结局）")
    
    return daily_glucose

# ============================================================================
# 提取静态特征（Demographics, Comorbidities）
# ============================================================================

def extract_static_features(group_dir, group_name):
    """提取静态特征（每个患者一条记录）"""
    log(f"\n提取 {group_name} 组静态特征...")
    
    result = pd.DataFrame()
    
    # 1. 从入院记录提取Demographics
    df_admission = load_file_safely(group_dir / '入院记录.csv', '入院记录')
    if df_admission is not None:
        # 提取基本信息
        demographics = df_admission[['admission_key']].copy()
        
        # Sex (1=男, 0=女)
        # Smoking, Drinking (需要从is_smoking, is_drinking转换)
        if 'is_smoking' in df_admission.columns:
            demographics['Smoking'] = df_admission['is_smoking'].apply(
                lambda x: 1 if str(x).strip() in ['是', '有', 'Y', 'y', '1'] else 0
            )
        
        if 'is_drinking' in df_admission.columns:
            demographics['Drinking'] = df_admission['is_drinking'].apply(
                lambda x: 1 if str(x).strip() in ['是', '有', 'Y', 'y', '1'] else 0
            )
        
        result = demographics
    
    # 3. 从生命体征提取身高体重BMI（取第一次记录）
    df_vital = load_file_safely(group_dir / '生命体征.csv', '生命体征')
    if df_vital is not None and not result.empty:
        # 取每个患者第一次记录
        vital_first = df_vital.groupby('admission_key').first().reset_index()
        
        vital_features = vital_first[['admission_key']].copy()
        
        if 'height' in vital_first.columns:
            vital_features['Height'] = vital_first['height']
        if 'weight' in vital_first.columns:
            vital_features['Weight'] = vital_first['weight']
        if 'body_mass_index' in vital_first.columns:
            vital_features['BMI'] = vital_first['body_mass_index']
        
        result = result.merge(vital_features, on='admission_key', how='left')
    
    # 3. 从诊断提取合并症和T1DM
    df_diagnosis = load_file_safely(group_dir / '诊断.csv', '诊断')
    if df_diagnosis is not None and not result.empty:
        # 取每个患者的最大值（有诊断即为1）
        comorbidity_cols = ['T1DM', 'HTN', 'HL', 'CAD', 'Malignant_tumor', 'CRF', 'RRT', 
                           'DPVD', 'DPN', 'DF', 'DN', 'DR']
        
        comorbidities = df_diagnosis[['admission_key']].drop_duplicates()
        
        for col in comorbidity_cols:
            if col in df_diagnosis.columns:
                col_max = df_diagnosis.groupby('admission_key')[col].max().reset_index()
                comorbidities = comorbidities.merge(col_max, on='admission_key', how='left')
                comorbidities[col] = comorbidities[col].fillna(0).astype(int)
        
        result = result.merge(comorbidities, on='admission_key', how='left')
        
        # 重命名HL为HLD（与论文一致）
        if 'HL' in result.columns:
            result = result.rename(columns={'HL': 'HLD'})
    
    # 4. 院区（Campus）：步骤1 已在住院表添加 Campus 列，此处直接按 admission_key 取首行合并
    df_hospital = load_file_safely(group_dir / '住院.csv', '住院')
    if df_hospital is not None and not result.empty:
        if 'Campus' in df_hospital.columns:
            campus_df = df_hospital[['admission_key', 'Campus']].groupby('admission_key')['Campus'].first().reset_index()
        else:
            def _extract_campus(dept_str):
                if pd.isna(dept_str):
                    return None
                s = str(dept_str).strip().replace('\t', '')
                m = re.search(r'[（(]([^)）]+)[)）]', s)
                return m.group(1).strip() if m else None
            campus_df = df_hospital[['admission_key', 'visit_department']].copy()
            campus_df['Campus'] = campus_df['visit_department'].apply(_extract_campus)
            campus_df = campus_df.groupby('admission_key')['Campus'].first().reset_index()
        result = result.merge(campus_df, on='admission_key', how='left')
        campus_counts = result['Campus'].value_counts()
        log(f"  ✓ 院区分布:")
        for campus, count in campus_counts.head(10).items():
            log(f"    {str(campus):15s}: {count:6,} ({count/len(result)*100:5.2f}%)")
    
    log(f"  ✓ 静态特征: {len(result):,}行 × {len(result.columns)-1}个变量")
    return result

# ============================================================================
# 提取每日动态特征
# ============================================================================

def extract_daily_lab_features(group_dir, group_name):
    """提取实验室检查特征（宽表格式）"""
    log(f"\n提取 {group_name} 组实验室检查...")
    
    result_list = []
    
    # 生化检查（宽表格式，每个指标一列）
    df_biochem = load_file_safely(group_dir / '生化.csv', '生化')
    if df_biochem is not None:
        # 转换为宽表
        if 'pure_item_name' in df_biochem.columns and 'test_result' in df_biochem.columns:
            df_biochem['test_result'] = pd.to_numeric(df_biochem['test_result'], errors='coerce')
            
            # 提取日期
            if 'test_time' in df_biochem.columns:
                df_biochem['date'] = pd.to_datetime(df_biochem['test_time'], errors='coerce').dt.date
                
                # 取每个患者每天每个指标的第一次测量
                biochem_pivot = df_biochem.pivot_table(
                    index=['admission_key', 'date'],
                    columns='pure_item_name',
                    values='test_result',
                    aggfunc='first'
                ).reset_index()
                
                result_list.append(biochem_pivot)
                log(f"  ✓ 生化检查: {len(biochem_pivot):,}条记录")
    
    # 血常规（宽表格式）
    df_blood = load_file_safely(group_dir / '血常规.csv', '血常规')
    if df_blood is not None:
        if 'pure_item_name' in df_blood.columns and 'test_result' in df_blood.columns:
            df_blood['test_result'] = pd.to_numeric(df_blood['test_result'], errors='coerce')
            
            if 'test_time' in df_blood.columns:
                df_blood['date'] = pd.to_datetime(df_blood['test_time'], errors='coerce').dt.date
                
                blood_pivot = df_blood.pivot_table(
                    index=['admission_key', 'date'],
                    columns='pure_item_name',
                    values='test_result',
                    aggfunc='first'
                ).reset_index()
                
                result_list.append(blood_pivot)
                log(f"  ✓ 血常规: {len(blood_pivot):,}条记录")
    
    # HbA1c
    df_hba1c = load_file_safely(group_dir / '糖代谢.csv', '糖代谢')
    if df_hba1c is not None:
        if 'HbA1c_test_result' in df_hba1c.columns:
            df_hba1c['HbA1c'] = pd.to_numeric(df_hba1c['HbA1c_test_result'], errors='coerce')
            
            if 'HbA1c_test_time' in df_hba1c.columns:
                df_hba1c['date'] = pd.to_datetime(df_hba1c['HbA1c_test_time'], errors='coerce').dt.date
                
                hba1c_daily = df_hba1c.groupby(['admission_key', 'date'])['HbA1c'].first().reset_index()
                
                result_list.append(hba1c_daily)
                log(f"  ✓ HbA1c: {len(hba1c_daily):,}条记录")
    
    # CRP
    df_crp = load_file_safely(group_dir / 'CRP.csv', 'CRP')
    if df_crp is not None:
        # 查找hs-CRP列
        crp_cols = [col for col in df_crp.columns if 'hs-CRP_test_result' in col]
        if crp_cols:
            df_crp['hs_CRP'] = pd.to_numeric(df_crp[crp_cols[0]], errors='coerce')
            
            time_cols = [col for col in df_crp.columns if 'hs-CRP_test_time' in col]
            if time_cols:
                df_crp['date'] = pd.to_datetime(df_crp[time_cols[0]], errors='coerce').dt.date
                
                crp_daily = df_crp.groupby(['admission_key', 'date'])['hs_CRP'].first().reset_index()
                
                result_list.append(crp_daily)
                log(f"  ✓ CRP: {len(crp_daily):,}条记录")
    
    return result_list

def extract_daily_vital_features(group_dir, group_name):
    """提取每日生命体征（取当天第一次记录）"""
    log(f"\n提取 {group_name} 组每日生命体征...")
    
    df_vital = load_file_safely(group_dir / '生命体征.csv', '生命体征')
    if df_vital is None:
        return pd.DataFrame()
    
    # 这里需要找到时间列
    # 根据之前的检查，生命体征文件没有单独的时间列
    # 我们使用test_birth_date_time_quantum1作为日期
    
    # 暂时返回空DataFrame，需要进一步确认时间列
    log(f"  ⚠️  生命体征文件缺少明确的测量时间列，跳过每日统计")
    return pd.DataFrame()

def extract_daily_medication_features(group_dir, group_name):
    """提取每日用药特征"""
    log(f"\n提取 {group_name} 组每日用药...")
    
    df_med = load_file_safely(group_dir / '药品医嘱.csv', '药品医嘱')
    if df_med is None:
        return pd.DataFrame()
    
    # 药物分类列
    med_cols = ['Metformin', 'Sulfonylureas', 'Glinides', 'TZDs', 'AGIs', 
                'DPP4i', 'SGLT2i', 'Rapid_insulin', 'Basal_insulin', 
                'Dual_insulin', 'Premixed_insulin']
    
    # 检查是否有这些列
    available_cols = [col for col in med_cols if col in df_med.columns]
    
    if not available_cols:
        log(f"  ⚠️  未找到药物分类列")
        return pd.DataFrame()
    
    # 需要时间列来按日期分组
    # 暂时取每个患者的整体用药（不按日期）
    med_features = df_med.groupby('admission_key')[available_cols].max().reset_index()
    
    log(f"  ✓ 用药特征: {len(med_features):,}行")
    
    return med_features

# ============================================================================
# 主拼接函数
# ============================================================================

def build_timeseries_dataset(group_dir, group_name):
    """构建时序数据集"""
    log("\n" + SEP)
    log(f"构建 {group_name} 组时序数据集")
    log(SEP)
    
    # 1. 处理血糖数据，获取每日血糖统计和次日结局
    df_glucose = load_file_safely(group_dir / '血糖.csv', '血糖')
    daily_data = process_glucose_data(df_glucose)
    
    if daily_data.empty:
        log(f"  ❌ 无有效血糖数据")
        return pd.DataFrame()
    
    log(f"\n基础时序表: {len(daily_data):,}条（每天一条）")
    
    # 2. 合并静态特征（Demographics, Comorbidities）
    static_features = extract_static_features(group_dir, group_name)
    if not static_features.empty:
        daily_data = daily_data.merge(static_features, on='admission_key', how='left')
        log(f"  ✓ 合并静态特征: {len(static_features.columns)-1}个变量")
    
    # 3. 合并每日实验室检查
    lab_features = extract_daily_lab_features(group_dir, group_name)
    for lab_df in lab_features:
        if not lab_df.empty:
            daily_data = daily_data.merge(lab_df, on=['admission_key', 'date'], how='left')
    
    # 4. 合并用药特征（目前是患者级别，不是每日）
    med_features = extract_daily_medication_features(group_dir, group_name)
    if not med_features.empty:
        daily_data = daily_data.merge(med_features, on='admission_key', how='left')
        log(f"  ✓ 合并用药特征")
    
    # 5. 添加住院天数（从入院开始的天数）
    log(f"\n计算住院天数...")
    daily_data = daily_data.sort_values(['admission_key', 'date']).reset_index(drop=True)
    daily_data['HospDay'] = daily_data.groupby('admission_key').cumcount() + 1
    log(f"  ✓ 住院天数已计算")
    
    # 6. 添加是否既往低血糖
    daily_data['PREHYPO'] = 0
    for idx, row in daily_data.iterrows():
        if row['min_glucose'] < 3.9:
            # 当天有低血糖，标记该患者后续所有日期
            mask = (daily_data['admission_key'] == row['admission_key']) & \
                   (daily_data['date'] > row['date'])
            daily_data.loc[mask, 'PREHYPO'] = 1
    
    # 7. 去掉每个admission_key的第一天和最后一天
    log(f"\n去掉每个患者的第一天和最后一天...")
    log(f"  处理前: {len(daily_data):,}行")
    
    # 为每个患者的记录标记序号
    daily_data = daily_data.sort_values(['admission_key', 'date']).reset_index(drop=True)
    daily_data['day_seq'] = daily_data.groupby('admission_key').cumcount() + 1
    daily_data['total_days'] = daily_data.groupby('admission_key')['admission_key'].transform('count')
    
    # 保留：day_seq > 1 且 day_seq < total_days
    daily_data_filtered = daily_data[
        (daily_data['day_seq'] > 1) & 
        (daily_data['day_seq'] < daily_data['total_days'])
    ].copy()
    
    # 删除辅助列
    daily_data_filtered = daily_data_filtered.drop(columns=['day_seq', 'total_days'])
    
    log(f"  处理后: {len(daily_data_filtered):,}行")
    log(f"  删除记录: {len(daily_data) - len(daily_data_filtered):,}行")
    
    log(f"\n最终时序大表: {len(daily_data_filtered):,}行 × {len(daily_data_filtered.columns)}列")
    
    return daily_data_filtered

# ============================================================================
# 主执行流程
# ============================================================================

def main():
    log(SEP)
    log("步骤7：拼接时序预测大表（每天一条记录）")
    log(SEP)
    log("\n数据结构说明：")
    log("  - 每个admission_key的每一天作为一条记录")
    log("  - 特征：当天的所有测量指标")
    log("  - 结局：次日血糖分类")
    log("    * 0 = 低血糖（次日任一血糖 < 3.9 mmol/L）")
    log("    * 1 = 正常/中风险")
    log("    * 2 = 高血糖（次日任一血糖 > 13.9 mmol/L）")
    
    # 构建Health组
    df_health = build_timeseries_dataset(HEALTH_DIR, "Health")
    if not df_health.empty:
        df_health['group'] = 'Health'
    
    # 构建HYPO组
    df_hypo = build_timeseries_dataset(HYPO_DIR, "HYPO")
    if not df_hypo.empty:
        df_hypo['group'] = 'HYPO'
    
    # 合并两组
    log("\n" + SEP)
    log(f"合并Health和HYPO组")
    log(SEP)
    
    if df_health.empty and df_hypo.empty:
        log(f"  ❌ 两组数据均为空")
        return
    
    df_combined = pd.concat([df_health, df_hypo], ignore_index=True)
    
    log(f"\n合并后数据:")
    log(f"  总记录数: {len(df_combined):,}条")
    log(f"  Health组: {len(df_health):,}条")
    log(f"  HYPO组: {len(df_hypo):,}条")
    log(f"  总列数: {len(df_combined.columns)}")
    
    # 统计次日结局分布
    log(f"\n次日结局分布（合并后）:")
    outcome_counts = df_combined['outcome'].value_counts().sort_index()
    total = len(df_combined)
    log(f"  0 - 低血糖（<3.9）  : {outcome_counts.get(0, 0):8,} ({outcome_counts.get(0, 0)/total*100:5.2f}%)")
    log(f"  1 - 正常/中风险      : {outcome_counts.get(1, 0):8,} ({outcome_counts.get(1, 0)/total*100:5.2f}%)")
    log(f"  2 - 高血糖（>13.9） : {outcome_counts.get(2, 0):8,} ({outcome_counts.get(2, 0)/total*100:5.2f}%)")
    
    # 保存BGA三分类表（原始的三分类）
    output_bga = BASE_DIR / "BGA_时序预测大表_三分类.csv"
    df_combined.to_csv(output_bga, index=False, encoding='utf-8-sig')
    log(f"\n✓ 已保存BGA三分类表: {output_bga}")
    
    # 创建HYPO二分类表（只关注低血糖）
    log("\n" + SEP)
    log(f"生成HYPO二分类表（低血糖预测）")
    log(SEP)
    
    df_hypo = df_combined.copy()
    
    # 重新定义outcome为二分类
    # 0 = 正常（原来的1和2合并）
    # 1 = 低血糖（原来的0）
    df_hypo['outcome_HYPO'] = df_hypo['outcome'].apply(lambda x: 1 if x == 0 else 0)
    
    # 删除原始的outcome列，保留outcome_HYPO
    df_hypo = df_hypo.drop(columns=['outcome'])
    df_hypo = df_hypo.rename(columns={'outcome_HYPO': 'outcome'})
    
    # 统计HYPO二分类分布
    hypo_counts = df_hypo['outcome'].value_counts().sort_index()
    total = len(df_hypo)
    log(f"\nHYPO二分类结局分布:")
    log(f"  0 - 正常/非低血糖    : {hypo_counts.get(0, 0):8,} ({hypo_counts.get(0, 0)/total*100:5.2f}%)")
    log(f"  1 - 低血糖（<3.9）  : {hypo_counts.get(1, 0):8,} ({hypo_counts.get(1, 0)/total*100:5.2f}%)")
    
    # 保存HYPO二分类表
    output_hypo = BASE_DIR / "HYPO_时序预测大表_二分类.csv"
    df_hypo.to_csv(output_hypo, index=False, encoding='utf-8-sig')
    log(f"\n✓ 已保存HYPO二分类表: {output_hypo}")
    
    # 计算缺失统计
    log("\n" + SEP)
    log(f"缺失情况统计（基于BGA三分类表）")
    log(SEP)
    
    missing_report = []
    for col in df_combined.columns:
        if col not in ['admission_key', 'date', 'group', 'outcome']:
            missing_count = df_combined[col].isna().sum()
            missing_pct = missing_count / len(df_combined) * 100
            missing_report.append({
                'Variable': col,
                'Missing_Count': missing_count,
                'Missing_Percent': f"{missing_pct:.2f}%",
                'Imputation_Strategy': ''  # 留空
            })
            
            if missing_pct > 0:
                log(f"  {col:30s}: {missing_count:8,} ({missing_pct:6.2f}%)")
    
    # 保存缺失统计
    df_missing = pd.DataFrame(missing_report)
    missing_file = BASE_DIR / "BGA_缺失统计.csv"
    df_missing.to_csv(missing_file, index=False, encoding='utf-8-sig')
    log(f"\n✓ 缺失统计已保存: {missing_file}")
    
    # 最终总结
    log("\n" + SEP)
    log("步骤7 完成：时序预测大表构建")
    log(SEP)
    log(f"\n生成的文件:")
    log(f"  1. BGA_时序预测大表_三分类.csv - 血糖异常三分类预测")
    log(f"     - 类别0：低血糖（<3.9 mmol/L）")
    log(f"     - 类别1：正常/中风险")
    log(f"     - 类别2：高血糖（>13.9 mmol/L）")
    log(f"\n  2. HYPO_时序预测大表_二分类.csv - 低血糖二分类预测")
    log(f"     - 类别0：正常/非低血糖")
    log(f"     - 类别1：低血糖（<3.9 mmol/L）")
    log(f"\n  3. BGA_缺失统计.csv - 缺失情况统计")
    log(f"\n数据特点:")
    log(f"  - 时序结构：每个患者每天一条记录（已去除首末天）")
    log(f"  - 样本总数：{len(df_combined):,}天")
    log(f"  - 患者总数：{df_combined['admission_key'].nunique():,}人")
    log(f"  - 特征维度：{len(df_combined.columns)-4}个（除去key/date/group/outcome）")
    log(f"\n预测任务:")
    log(f"  - BGA表：三分类（低血糖/正常/高血糖）- 全面的血糖异常预测")
    log(f"  - HYPO表：二分类（低血糖/非低血糖）- 专注低血糖风险预测")

    # 打印最终大表 info
    log("\n" + SEP)
    log("BGA 三分类表 / HYPO 二分类表 info")
    log(SEP)
    buf_bga = io.StringIO()
    df_combined.info(buf=buf_bga)
    log("\n[BGA_时序预测大表_三分类]")
    log(SUB)
    for line in buf_bga.getvalue().strip().splitlines():
        log("  " + line)
    buf_hypo = io.StringIO()
    df_hypo.info(buf=buf_hypo)
    log("\n[HYPO_时序预测大表_二分类]")
    log(SUB)
    for line in buf_hypo.getvalue().strip().splitlines():
        log("  " + line)

if __name__ == '__main__':
    main()
