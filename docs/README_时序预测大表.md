# 时序预测大表构建总结

## 📊 概述

本项目成功构建了两个用于住院患者血糖异常预测的时序数据集：
1. **BGA表**（三分类）- 全面的血糖异常预测
2. **HYPO表**（二分类）- 专注低血糖风险预测

---

## 📁 生成的文件

### 1. BGA_时序预测大表_三分类.csv (17.46 MB)

**预测任务**: 预测次日血糖是否异常及异常类型

**结局定义**:
- **类别0** - 低血糖: 次日任一血糖 < 3.9 mmol/L
- **类别1** - 正常/中风险: 3.9 ≤ 血糖 ≤ 13.9 mmol/L
- **类别2** - 高血糖: 次日任一血糖 > 13.9 mmol/L

**样本分布**:
| 类别 | 样本数 | 百分比 |
|------|--------|--------|
| 低血糖 | 1,599天 | 2.03% |
| 正常/中风险 | 46,280天 | 58.84% |
| 高血糖 | 30,779天 | 39.13% |

**应用场景**:
- ✓ 全面的血糖管理决策支持
- ✓ 识别所有类型的血糖异常风险
- ✓ 为不同血糖状态制定个性化干预策略

---

### 2. HYPO_时序预测大表_二分类.csv (17.31 MB)

**预测任务**: 专注预测次日是否发生低血糖事件

**结局定义**:
- **类别0** - 正常/非低血糖: 次日所有血糖 ≥ 3.9 mmol/L
- **类别1** - 低血糖: 次日任一血糖 < 3.9 mmol/L

**样本分布**:
| 类别 | 样本数 | 百分比 |
|------|--------|--------|
| 正常/非低血糖 | 77,059天 | 97.97% |
| 低血糖 | 1,599天 | 2.03% |

**应用场景**:
- ✓ 低血糖风险预警系统
- ✓ 专注于高危事件预防
- ✓ 优化临床干预时机

---

### 3. BGA_缺失统计.csv

包含所有变量的缺失情况统计，包括：
- 变量名
- 缺失数量
- 缺失百分比
- 填补策略（留空待定）

---

## 📈 数据规模

**共同特征**:
- **总记录数**: 78,658天
- **患者总数**: 13,080人
- **特征维度**: 73个变量（包含新增的Campus）
- **数据来源**: Health组(60,053天) + HYPO组(18,605天)

**数据处理**:
- ✅ 已去除每个患者的第一天和最后一天
- ✅ 所有记录都有完整的前一天数据
- ✅ 所有记录都有明确的次日结局

---

## 🔍 特征类别

### 1. 静态特征 (18个)

**人口统计学**:
- Smoking, Drinking
- Height, Weight, BMI

**院区信息** 🆕:
- **Campus**: 就诊院区（月湖/外滩/方桥/海曙）
  - 数据来源：住院文件的就诊科室字段
  - 提取方法：从科室名称括号中提取
  - 缺失率：0.02%（极低）
  - 分布：月湖(55.02%), 外滩(23.61%), 方桥(20.70%), 其他(0.67%)

**合并症**:
- T1DM, HTN, HLD, CAD, Malignancy
- CRF, RRT, DPVD, DPN, DF, DN, DR

### 2. 每日血糖指标 (6个)

- `min_glucose`: 当日最低血糖
- `max_glucose`: 当日最高血糖
- `mean_glucose`: 当日平均血糖
- `std_glucose`: 当日血糖标准差
- `cv_glucose`: 当日血糖变异系数
- `count_glucose`: 当日血糖测量次数

### 3. 实验室检查 (40+个)

**生化检查**:
- 总胆红素、总胆固醇、甘油三酯、LDL、HDL
- 钠、钾、肌酐、白蛋白
- ALT、AST、GGT

**血常规**:
- 红细胞、白细胞、血小板、血红蛋白

**其他**:
- HbA1c、hs-CRP

### 4. 降糖药物 (11个)

**口服降糖药 (7种)**:
- Metformin（二甲双胍）
- Sulfonylureas（磺脲类）
- Glinides（格列奈类）
- TZDs（噻唑烷二酮类）
- AGIs（α-葡萄糖苷酶抑制剂）
- DPP4i（DPP-4抑制剂）
- SGLT2i（SGLT2抑制剂）

**胰岛素 (4种)**:
- Rapid_insulin（餐时胰岛素）
- Basal_insulin（基础胰岛素）
- Dual_insulin（双胰岛素）
- Premixed_insulin（预混胰岛素）

### 5. 住院信息 (2个)

- `HospDay`: 住院天数（从入院开始的天数）
- `PREHYPO`: 是否有既往低血糖（0/1）

### 6. 元信息 (4个)

- `admission_key`: 患者住院唯一标识
- `date`: 记录日期
- `group`: 数据来源（Health/HYPO）
- `outcome`: 次日结局（分类标签）

---

## 🏥 Campus（院区）变量详解

### 变量说明

**Campus** - 患者就诊的院区位置

- **数据类型**: 分类变量（Categorical）
- **取值**: 月湖、外滩、方桥、海曙、其他
- **数据来源**: 住院文件的就诊科室字段
- **提取方法**: 正则表达式提取括号中的院区名称
  - 例如："心内科二（月湖）" → "月湖"

### 院区分布

| 院区 | 记录数 | 百分比 |
|------|--------|--------|
| 月湖 | 43,277 | 55.02% |
| 外滩 | 18,571 | 23.61% |
| 方桥 | 16,286 | 20.70% |
| 其他/异常 | 509 | 0.65% |
| 缺失 | 15 | 0.02% |

### 建模价值

1. **院区差异分析**
   - 不同院区的血糖管理水平
   - 医疗资源配置差异
   - 患者人群特征差异

2. **分层建模**
   - 按院区建立子模型
   - 提高预测针对性

3. **特征工程**
   - One-Hot编码
   - Target编码（基于历史低血糖率）
   - 交互特征（Campus × 其他变量）

### 特征工程示例

```python
import pandas as pd

# 读取数据
df = pd.read_csv('BGA_时序预测大表_三分类.csv')

# 1. 处理异常值
df['Campus'] = df['Campus'].replace({'1': '其他', '2': '其他'})

# 2. One-Hot编码（推荐用于树模型）
campus_dummies = pd.get_dummies(df['Campus'], prefix='Campus', drop_first=True)
df = pd.concat([df, campus_dummies], axis=1)

# 3. Target编码（基于历史低血糖率）
campus_hypo_rate = df.groupby('Campus')['outcome'].apply(
    lambda x: (x == 0).mean()  # 低血糖比例
).to_dict()
df['Campus_risk_score'] = df['Campus'].map(campus_hypo_rate)

# 4. 交互特征（示例：Campus与HospDay的交互）
df['Campus_HospDay'] = df['Campus'].astype(str) + '_' + df['HospDay'].astype(str)
```

---

## ⚠️ 类别不平衡问题

### BGA三分类

- **不平衡比例**: 正常类 vs 低血糖类 = 28.9:1
- **不平衡程度**: 中等
- **处理建议**:
  - 使用类别权重 `class_weight='balanced'`
  - 或使用SMOTE适度过采样
  - 评估指标：weighted F1-score, macro-AUROC

### HYPO二分类

- **不平衡比例**: 正常类 vs 低血糖类 = 48.2:1
- **不平衡程度**: 严重 ⚠️
- **处理建议**:
  - **必须**使用类别权重或过采样技术
  - 关注指标：F1-score, Precision-Recall AUC
  - 优化召回率（Recall）以捕获低血糖事件
  - 考虑代价敏感学习（Cost-sensitive learning）
  - 可能需要调整决策阈值

---

## 🎯 建议的建模流程

### 1. 数据预处理

#### 缺失值处理
- **高缺失 (>75%)**: 考虑删除该变量
- **中等缺失 (25-75%)**: Multiple Imputation（多重插补）
- **低缺失 (<25%)**: Forward fill（时序前向填充）+ MICE

#### 特征工程
- **滞后特征**: lag 1天、3天、7天的血糖指标
- **滚动统计**: 3天/7天窗口的mean、std、min、max
- **入院累积统计**: 计算入院以来的INPMEAN、INPCV
- **派生特征**: 计算eGFR、血糖趋势、变化率等

### 2. 模型开发 - BGA三分类

**基线模型**:
- Multinomial Logistic Regression

**高级模型**:
- XGBoost (multiclass, class_weight)
- LightGBM (multiclass, balanced)
- Random Forest (balanced)

**深度学习**:
- MLP（多层感知机）
- LSTM（考虑时序依赖）
- Attention-based models

**评估指标**:
- Weighted F1-score
- Macro-AUROC
- Per-class Precision/Recall
- Confusion Matrix

### 3. 模型开发 - HYPO二分类

**基线模型**:
- Logistic Regression (class_weight='balanced')

**高级模型**:
- XGBoost (scale_pos_weight=48)
- LightGBM (is_unbalance=True)
- Random Forest (class_weight='balanced')

**集成方法**:
- Ensemble of multiple models
- Stacking
- Threshold optimization

**评估指标**:
- **主要指标**: F1-score, AUPRC（强烈推荐）
- **次要指标**: Recall, Precision, AUROC
- **不推荐**: Accuracy（受不平衡影响）

### 4. 验证策略

**交叉验证**:
- 时序交叉验证（Time Series Cross-Validation）
- 按患者分组（GroupKFold，避免数据泄漏）
- 确保验证集在时间上晚于训练集

**数据分割**:
- 训练集：70%（早期数据）
- 验证集：15%（中期数据）
- 测试集：15%（最新数据）

**避免数据泄漏**:
- ✓ 同一患者的记录不能同时出现在训练集和测试集
- ✓ 特征工程时不能使用未来信息
- ✓ 滞后特征和滚动统计只使用历史数据

---

## 💡 高级优化建议

### 处理类别不平衡

1. **过采样技术**:
   - SMOTE（Synthetic Minority Over-sampling Technique）
   - ADASYN（Adaptive Synthetic Sampling）
   - Borderline-SMOTE

2. **欠采样技术**:
   - Random Undersampling
   - Tomek Links
   - Near Miss

3. **混合方法**:
   - SMOTE + Tomek Links
   - SMOTE + ENN

4. **算法层面**:
   - 使用class_weight
   - 调整decision threshold
   - Cost-sensitive learning

### 特征选择

1. **基于重要性**:
   - Tree-based feature importance
   - Permutation importance
   - SHAP values

2. **基于相关性**:
   - 去除高度相关特征（>0.9）
   - VIF（方差膨胀因子）分析

3. **降维方法**:
   - PCA（主成分分析）
   - t-SNE（可视化）
   - UMAP（可视化）

### 模型解释性

1. **全局解释**:
   - Feature Importance
   - Partial Dependence Plots
   - SHAP Summary Plot

2. **局部解释**:
   - LIME（局部可解释模型）
   - SHAP Force Plot
   - Individual Conditional Expectation

---

## 📊 预期性能指标

### BGA三分类

**可能达到的性能**（基于类似研究）:
- Macro F1-score: 0.65 - 0.75
- Weighted F1-score: 0.70 - 0.80
- 低血糖类F1: 0.40 - 0.60（最具挑战性）
- 正常类F1: 0.75 - 0.85
- 高血糖类F1: 0.70 - 0.80

### HYPO二分类

**可能达到的性能**:
- F1-score: 0.30 - 0.50（受严重不平衡影响）
- AUPRC: 0.15 - 0.30（更合适的指标）
- AUROC: 0.75 - 0.85
- Recall@10% FPR: 0.30 - 0.50

---

## 🚀 快速开始

### Python代码示例

```python
import pandas as pd
from sklearn.model_selection import GroupKFold
from sklearn.preprocessing import StandardScaler
from xgboost import XGBClassifier
from sklearn.metrics import f1_score, roc_auc_score

# 1. 读取数据
df_hypo = pd.read_csv('HYPO_时序预测大表_二分类.csv')

# 2. 分离特征和标签
X = df_hypo.drop(['admission_key', 'date', 'group', 'outcome'], axis=1)
y = df_hypo['outcome']
groups = df_hypo['admission_key']

# 3. 处理缺失值（示例：简单填充）
X = X.fillna(X.median())

# 4. 标准化
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# 5. 训练模型（考虑类别不平衡）
model = XGBClassifier(
    scale_pos_weight=48,  # 正负样本比例
    max_depth=5,
    learning_rate=0.05,
    n_estimators=200,
    random_state=42
)

# 6. 按患者分组的交叉验证
gkf = GroupKFold(n_splits=5)
f1_scores = []

for train_idx, val_idx in gkf.split(X_scaled, y, groups):
    X_train, X_val = X_scaled[train_idx], X_scaled[val_idx]
    y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]
    
    model.fit(X_train, y_train)
    y_pred = model.predict(X_val)
    
    f1 = f1_score(y_val, y_pred)
    f1_scores.append(f1)

print(f"平均F1-score: {np.mean(f1_scores):.3f}")
```

---

## ✅ 数据质量检查清单

在开始建模前，请确认：

- [ ] 已检查所有特征的数据类型
- [ ] 已处理或删除高缺失变量（>75%）
- [ ] 已检查离群值（特别是血糖、实验室指标）
- [ ] 已确认没有数据泄漏（未来信息）
- [ ] 已验证时间序列的连续性
- [ ] 已确认患者ID不重复（同一患者不同天）
- [ ] 已理解outcome定义和临床意义

---

## 📞 联系与支持

如有问题或需要进一步的数据处理，请参考：
- 数据清洗脚本：`步骤1_日期8-8规则转换.py` 至 `步骤7_拼接时序大表.py`
- 变量说明文档：`降糖药物变量说明.txt`, `合并症变量说明.txt`, `手术事件变量说明.txt`, `禁食营养变量说明.txt`

---

## 📝 更新日志

**2026-01-30**:
- ✅ 完成数据清洗（步骤1-6）
- ✅ 生成BGA三分类表
- ✅ 生成HYPO二分类表
- ✅ 去除每个患者首末天记录
- ✅ 生成缺失统计报告
- ✅ **新增Campus（院区）变量** 🆕
  - 从住院文件提取院区信息
  - 包含4个主要院区：月湖、外滩、方桥、海曙
  - 缺失率极低（0.02%）
  - 特征维度: 72 → 73

---

**祝建模顺利！🚀**
