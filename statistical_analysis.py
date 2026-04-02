"""
上部消化管グループ 統合症例登録DB — statistical_analysis.py
2群比較・単変量解析・多変量解析モジュール

臨床研究の典型的ワークフロー:
  1. アウトカム変数を選択（合併症有無, 死亡, 再発 など）
  2. 患者背景・手術因子を2群で比較（Table 1）
  3. 単変量解析（ロジスティック回帰 or Cox回帰）
  4. 有意変数を選んで多変量解析
  5. テーブル出力（CSV / Excel）
"""

import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
from codebook import get_column_label, get_codebook, COLUMN_LABELS

# ---------------------------------------------------------------------------
# 日本語ラベル（表示用）— codebook.py の COLUMN_LABELS を単一ソースとして参照
# ---------------------------------------------------------------------------
VARIABLE_JP = COLUMN_LABELS  # 後方互換エイリアス

# コードブック値ラベルのキャッシュ {field_name: {code_value: label}}
_CODE_LABEL_CACHE: dict = {}

# ---------------------------------------------------------------------------
# DB カラム名 → codebook field_name のフォールバックマッピング
# DB は汎用名 (c_depth) だが codebook は疾患別 (c_depth_gastric / c_depth_eso)
# 胃癌を優先し、食道癌をフォールバックとする
# ---------------------------------------------------------------------------
_FIELD_FALLBACKS: dict = {
    # 術前診断
    "c_depth":              ["c_depth_gastric", "c_depth_eso", "c_depth_uicc8"],
    "c_ln_metastasis":      ["c_ln_gastric", "c_ln_eso"],
    "c_stage":              ["c_stage_gastric", "c_stage_eso"],
    "c_histology1":         ["histology_gastric", "histology_eso"],
    "c_histology2":         ["histology_gastric", "histology_eso"],
    "c_histology3":         ["histology_gastric", "histology_eso"],
    "c_macroscopic_type":   ["macroscopic_type"],
    # 病理
    "p_depth":              ["p_depth_gastric", "p_depth_eso"],
    "p_ln_metastasis":      ["p_ln_gastric", "p_ln_eso"],
    "p_stage":              ["p_stage_gastric", "p_stage_eso"],
    "p_histology1":         ["histology_gastric", "histology_eso"],
    "p_histology2":         ["histology_gastric", "histology_eso"],
    "p_ly":                 ["lymphatic_invasion"],
    "p_v":                  ["venous_invasion"],
    "p_inf":                ["inf_pattern"],
    "p_residual_tumor":     ["residual_tumor"],
    # 手術
    "op_procedure":         ["op_procedure_gastric", "op_procedure_eso"],
    "op_dissection":        ["op_dissection_gastric", "op_dissection_eso"],
    "op_reconstruction":    ["op_reconstruction_gastric", "op_reconstruction_eso"],
    "op_approach":          ["op_approach"],
    "op_anastomosis_method": ["op_anastomosis_method"],
    "op_cd_grade_max":      ["_cd_grade_with_none"],
    # 合併症 (各comp_*はCD分類; 0=なし を含む)
    **{f"comp_{c}": ["_cd_grade_with_none"] for c in [
        "ssi", "anastomotic_leak", "anastomotic_stricture", "anastomotic_bleeding",
        "pancreatic_fistula", "bile_leak", "duodenal_stump_leak",
        "pneumonia", "atelectasis", "ileus", "bleeding", "dvt_pe", "dge",
        "wound_dehiscence", "intra_abd_abscess", "uti", "delirium",
        "cardiac", "perforation", "cholelithiasis", "rln_palsy",
        "chylothorax", "empyema", "pneumothorax", "sepsis", "hepatic_failure",
    ]},
    # 化学療法
    "nac_regimen":          ["nac_regimen_gastric", "nac_regimen_eso"],
    "adj_regimen":          ["adj_regimen_gastric", "adj_regimen_eso"],
    # その他
    "disease_category":     ["disease_class"],
}


def jp(col: str) -> str:
    """カラム名を日本語に変換。"""
    return get_column_label(col)


def _ensure_cd_grade_with_none():
    """CD分類 + 0=なし のキャッシュを作成。"""
    if "_cd_grade_with_none" in _CODE_LABEL_CACHE:
        return
    base = _load_codebook_to_cache("op_cd_grade")
    base[0] = "なし"
    base[0.0] = "なし"
    base["0"] = "なし"
    _CODE_LABEL_CACHE["_cd_grade_with_none"] = base


def _load_codebook_to_cache(field_name: str) -> dict:
    """codebook の field_name からコード→ラベルのマッピングを読み込みキャッシュに格納。
    version_id=NULL で見つからなければ version_id=1,2,3... を順に試す。
    """
    try:
        mapping = get_codebook(field_name)  # version_id=NULL をまず検索
        if not mapping:
            # version_id 付きで再試行 (1=胃癌規約, 2=UICC8, 3=食道癌規約)
            for vid in [1, 2, 3]:
                mapping = get_codebook(field_name, version_id=vid)
                if mapping:
                    break
        normalized = {}
        for k, v in mapping.items():
            normalized[k] = v
            try:
                normalized[int(k)] = v
                normalized[float(k)] = v
            except (ValueError, TypeError):
                pass
        return normalized
    except Exception:
        return {}


def _code_label(field_name: str, code_value) -> str:
    """codebook の code → label を返す。
    DB カラム名で見つからなければ疾患別フォールバック (_FIELD_FALLBACKS) を順に試す。
    """
    # CD分類＋なし の特殊キャッシュを初期化
    _ensure_cd_grade_with_none()

    # 試すべき field_name のリスト: 本体 → フォールバック
    candidates = [field_name] + _FIELD_FALLBACKS.get(field_name, [])

    for fn in candidates:
        if fn not in _CODE_LABEL_CACHE:
            _CODE_LABEL_CACHE[fn] = _load_codebook_to_cache(fn)
        cache = _CODE_LABEL_CACHE[fn]
        if not cache:
            continue
        # 複数の型でルックアップ
        for cv in [code_value]:
            label = cache.get(cv)
            if label:
                return label
            try:
                label = cache.get(int(cv))
                if label:
                    return label
            except (ValueError, TypeError):
                pass
            try:
                label = cache.get(float(cv))
                if label:
                    return label
            except (ValueError, TypeError):
                pass
            try:
                label = cache.get(str(cv))
                if label:
                    return label
            except (ValueError, TypeError):
                pass
    return str(code_value)


def _resolve_dummy_name(name: str, predictors: list, df: pd.DataFrame) -> str:
    """ダミー変数名 (e.g. 'asa_2.0') を日本語表示名に変換する。
    元のカラム名ならjp()、ダミーなら 'jp(var): ラベル' 形式で返す。
    """
    # そのままのカラム名で見つかればそれを返す
    if name in VARIABLE_JP:
        return jp(name)
    # ダミー変数名 → 元のカラム名を特定
    for var in predictors:
        if name.startswith(f"{var}_"):
            code_part = name[len(var) + 1:]
            try:
                code_num = int(float(code_part))
            except (ValueError, TypeError):
                code_num = code_part
            return f"{jp(var)}: {_code_label(var, code_num)}"
    return name


# ---------------------------------------------------------------------------
# 統計検定ユーティリティ
# ---------------------------------------------------------------------------

def _is_binary(s: pd.Series) -> bool:
    vals = s.dropna().unique()
    return set(vals).issubset({0, 1, 0.0, 1.0})


def _is_categorical(s: pd.Series, max_unique: int = 10) -> bool:
    if s.dtype == object or s.dtype.name == "category":
        return True
    vals = s.dropna().unique()
    if len(vals) <= max_unique and all(float(v).is_integer() for v in vals if pd.notna(v)):
        return True
    return False


def _test_continuous(g0: pd.Series, g1: pd.Series):
    """連続変数の2群比較 → Mann-Whitney U検定 (正規性を仮定しない)。"""
    from scipy.stats import mannwhitneyu
    a, b = g0.dropna(), g1.dropna()
    if len(a) < 2 or len(b) < 2:
        return np.nan
    try:
        _, p = mannwhitneyu(a, b, alternative="two-sided")
        return p
    except Exception:
        return np.nan


def _test_categorical(g0: pd.Series, g1: pd.Series):
    """カテゴリ変数の2群比較 → χ²検定 or Fisher 正確検定。"""
    from scipy.stats import chi2_contingency, fisher_exact
    combined = pd.concat([g0, g1], keys=["g0", "g1"])
    ct = pd.crosstab(combined.index.get_level_values(0), combined.values)
    if ct.shape[0] < 2 or ct.shape[1] < 2:
        return np.nan
    # 2×2 かつ期待度数5未満のセルがあれば Fisher
    if ct.shape == (2, 2):
        try:
            expected = chi2_contingency(ct.values)[3]
            if (expected < 5).any():
                _, p = fisher_exact(ct.values)
                return p
        except Exception:
            pass
    try:
        chi2, p, dof, _ = chi2_contingency(ct.values)
        return p
    except Exception:
        return np.nan


def _format_continuous(s: pd.Series) -> str:
    v = s.dropna()
    if len(v) == 0:
        return "-"
    return f"{v.median():.1f} ({v.quantile(0.25):.1f}-{v.quantile(0.75):.1f})"


def _format_categorical_binary(s: pd.Series) -> str:
    v = s.dropna()
    if len(v) == 0:
        return "-"
    pos = (v > 0).sum() if pd.api.types.is_numeric_dtype(v) else v.value_counts().iloc[0]
    return f"{pos} ({pos / len(v) * 100:.1f}%)"


def _format_p(p):
    if pd.isna(p):
        return "-"
    if p < 0.001:
        return "<0.001"
    return f"{p:.3f}"


# ---------------------------------------------------------------------------
# 2群比較テーブル (Table 1)
# ---------------------------------------------------------------------------

def two_group_comparison(df: pd.DataFrame, group_col: str,
                         variables: list) -> pd.DataFrame:
    """
    2群比較テーブルを生成。
    group_col: バイナリ(0/1)のグルーピング変数。
    variables: 比較する変数リスト。

    Returns: DataFrame with columns [変数, 全体, Group 0, Group 1, p値, 検定]
    """
    g0 = df[df[group_col] == 0]
    g1 = df[df[group_col] == 1]

    g0_label = f"{jp(group_col)}=0 (n={len(g0)})"
    g1_label = f"{jp(group_col)}=1 (n={len(g1)})"

    rows = []
    for var in variables:
        if var not in df.columns or var == group_col:
            continue
        s_all = df[var]
        s0 = g0[var]
        s1 = g1[var]

        if _is_binary(s_all) or (_is_categorical(s_all) and s_all.dropna().nunique() == 2):
            # バイナリ / 2カテゴリ
            fmt_all = _format_categorical_binary(s_all)
            fmt_0 = _format_categorical_binary(s0)
            fmt_1 = _format_categorical_binary(s1)
            p = _test_categorical(s0, s1)
            test_name = "Fisher" if s_all.dropna().nunique() == 2 else "χ²"
            rows.append({
                "変数": jp(var), "全体 (n={})".format(len(df)): fmt_all,
                g0_label: fmt_0, g1_label: fmt_1,
                "p値": _format_p(p), "検定": test_name,
            })
        elif _is_categorical(s_all):
            # 多カテゴリ
            p = _test_categorical(s0, s1)
            test_name = "χ²"
            vals = sorted(s_all.dropna().unique())
            # ヘッダ行
            rows.append({
                "変数": jp(var), "全体 (n={})".format(len(df)): "",
                g0_label: "", g1_label: "",
                "p値": _format_p(p), "検定": test_name,
            })
            for v in vals:
                n_all = (s_all == v).sum()
                n0 = (s0 == v).sum()
                n1 = (s1 == v).sum()
                pct_all = n_all / len(s_all.dropna()) * 100 if len(s_all.dropna()) > 0 else 0
                pct_0 = n0 / len(s0.dropna()) * 100 if len(s0.dropna()) > 0 else 0
                pct_1 = n1 / len(s1.dropna()) * 100 if len(s1.dropna()) > 0 else 0
                rows.append({
                    "変数": f"  {_code_label(var, v)}",
                    "全体 (n={})".format(len(df)): f"{n_all} ({pct_all:.1f}%)",
                    g0_label: f"{n0} ({pct_0:.1f}%)",
                    g1_label: f"{n1} ({pct_1:.1f}%)",
                    "p値": "", "検定": "",
                })
        else:
            # 連続変数 → 中央値 (IQR)
            fmt_all = _format_continuous(s_all)
            fmt_0 = _format_continuous(s0)
            fmt_1 = _format_continuous(s1)
            p = _test_continuous(s0, s1)
            test_name = "Mann-Whitney"
            rows.append({
                "変数": jp(var), "全体 (n={})".format(len(df)): fmt_all,
                g0_label: fmt_0, g1_label: fmt_1,
                "p値": _format_p(p), "検定": test_name,
            })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 単変量ロジスティック回帰
# ---------------------------------------------------------------------------

def univariate_logistic(df: pd.DataFrame, outcome_col: str,
                        predictors: list) -> pd.DataFrame:
    """
    各予測変数について、アウトカムに対する単変量ロジスティック回帰。
    Returns: DataFrame [変数, OR, 95%CI, p値]
    """
    import statsmodels.api as sm

    rows = []
    for var in predictors:
        if var not in df.columns or var == outcome_col:
            continue
        sub = df[[outcome_col, var]].dropna()
        if len(sub) < 10 or sub[outcome_col].nunique() < 2:
            rows.append({"変数": jp(var), "OR": "-", "95% CI": "-", "p値": "-"})
            continue

        y = sub[outcome_col].astype(float)
        X = sub[[var]].astype(float)

        # カテゴリ変数はダミー化
        if _is_categorical(df[var]) and not _is_binary(df[var]):
            X = pd.get_dummies(X, columns=[var], drop_first=True, dtype=float)

        X = sm.add_constant(X, has_constant="add")

        try:
            model = sm.Logit(y, X).fit(disp=0, method="bfgs", maxiter=100)
            for i, name in enumerate(model.params.index):
                if name == "const":
                    continue
                or_val = np.exp(model.params[name])
                ci = np.exp(model.conf_int().iloc[i])
                p_val = model.pvalues[name]
                if i == 1 and not (_is_categorical(df[var]) and not _is_binary(df[var])):
                    display_name = jp(var)
                elif i == 1:
                    # カテゴリ変数の最初のダミー：ヘッダ行 + サブ行
                    display_name = jp(var)
                    rows.append({
                        "変数": display_name,
                        "OR": "", "95% CI": "", "p値": "",
                        "_p_raw": np.nan, "_var": var,
                    })
                    # ダミー名 (e.g. "asa_2.0") → code部分を抽出してラベル化
                    code_part = name.replace(f"{var}_", "")
                    try:
                        code_part_num = int(float(code_part))
                    except (ValueError, TypeError):
                        code_part_num = code_part
                    display_name = f"  {_code_label(var, code_part_num)}"
                else:
                    # 2番目以降のダミー
                    code_part = name.replace(f"{var}_", "")
                    try:
                        code_part_num = int(float(code_part))
                    except (ValueError, TypeError):
                        code_part_num = code_part
                    display_name = f"  {_code_label(var, code_part_num)}"
                rows.append({
                    "変数": display_name,
                    "OR": f"{or_val:.2f}",
                    "95% CI": f"{ci[0]:.2f}-{ci[1]:.2f}",
                    "p値": _format_p(p_val),
                    "_p_raw": p_val,
                    "_var": var,
                })
        except Exception:
            rows.append({"変数": jp(var), "OR": "-", "95% CI": "-", "p値": "-",
                         "_p_raw": np.nan, "_var": var})

    result = pd.DataFrame(rows)
    return result


# ---------------------------------------------------------------------------
# 多変量ロジスティック回帰
# ---------------------------------------------------------------------------

def multivariate_logistic(df: pd.DataFrame, outcome_col: str,
                          predictors: list) -> pd.DataFrame:
    """
    多変量ロジスティック回帰。
    Returns: DataFrame [変数, OR, 95%CI, p値]
    """
    import statsmodels.api as sm

    sub = df[[outcome_col] + predictors].dropna()
    if len(sub) < 10 or sub[outcome_col].nunique() < 2:
        return pd.DataFrame(columns=["変数", "OR", "95% CI", "p値"])

    y = sub[outcome_col].astype(float)

    # カテゴリ変数をダミー化
    X_parts = []
    for var in predictors:
        col = sub[[var]].astype(float)
        if _is_categorical(df[var]) and not _is_binary(df[var]):
            col = pd.get_dummies(col, columns=[var], drop_first=True, dtype=float)
        X_parts.append(col)

    X = pd.concat(X_parts, axis=1)
    X = sm.add_constant(X, has_constant="add")

    try:
        model = sm.Logit(y, X).fit(disp=0, method="bfgs", maxiter=200)
        rows = []
        for name in model.params.index:
            if name == "const":
                continue
            or_val = np.exp(model.params[name])
            ci = np.exp(model.conf_int().loc[name])
            p_val = model.pvalues[name]
            # ダミー変数名 (e.g. "asa_2.0") → 元のカラム名+コードラベル
            display_name = _resolve_dummy_name(name, predictors, df)
            rows.append({
                "変数": display_name,
                "OR": f"{or_val:.2f}",
                "95% CI": f"{ci[0]:.2f}-{ci[1]:.2f}",
                "p値": _format_p(p_val),
            })
        return pd.DataFrame(rows)
    except Exception as e:
        st.error(f"多変量解析エラー: {e}")
        return pd.DataFrame(columns=["変数", "OR", "95% CI", "p値"])


# ---------------------------------------------------------------------------
# 単変量 Cox 回帰
# ---------------------------------------------------------------------------

def univariate_cox(df: pd.DataFrame, time_col: str, event_col: str,
                   predictors: list) -> pd.DataFrame:
    """
    各予測変数について単変量 Cox 比例ハザード回帰。
    Returns: DataFrame [変数, HR, 95%CI, p値]
    """
    try:
        from lifelines import CoxPHFitter
    except ImportError:
        # lifelines がない場合は statsmodels で代替
        return _univariate_cox_statsmodels(df, time_col, event_col, predictors)

    rows = []
    for var in predictors:
        if var not in df.columns or var == event_col or var == time_col:
            continue
        cols = [time_col, event_col, var]
        sub = df[cols].dropna()
        sub = sub[sub[time_col] > 0]
        if len(sub) < 10 or sub[event_col].nunique() < 2:
            rows.append({"変数": jp(var), "HR": "-", "95% CI": "-", "p値": "-",
                         "_p_raw": np.nan, "_var": var})
            continue

        try:
            cph = CoxPHFitter()
            cph.fit(sub, duration_col=time_col, event_col=event_col)
            summary = cph.summary
            for idx in summary.index:
                hr = summary.loc[idx, "exp(coef)"]
                ci_lo = summary.loc[idx, "exp(coef) lower 95%"]
                ci_hi = summary.loc[idx, "exp(coef) upper 95%"]
                p_val = summary.loc[idx, "p"]
                rows.append({
                    "変数": jp(var),
                    "HR": f"{hr:.2f}",
                    "95% CI": f"{ci_lo:.2f}-{ci_hi:.2f}",
                    "p値": _format_p(p_val),
                    "_p_raw": p_val,
                    "_var": var,
                })
        except Exception:
            rows.append({"変数": jp(var), "HR": "-", "95% CI": "-", "p値": "-",
                         "_p_raw": np.nan, "_var": var})

    return pd.DataFrame(rows)


def _univariate_cox_statsmodels(df, time_col, event_col, predictors):
    """lifelines がない場合の代替 Cox 回帰 (statsmodels PHReg)。"""
    rows = []
    try:
        from statsmodels.duration.hazard_regression import PHReg
    except ImportError:
        st.error("Cox回帰には `lifelines` または `statsmodels` が必要です。"
                 "`pip install lifelines` を実行してください。")
        return pd.DataFrame(columns=["変数", "HR", "95% CI", "p値"])

    for var in predictors:
        if var not in df.columns or var == event_col or var == time_col:
            continue
        sub = df[[time_col, event_col, var]].dropna()
        sub = sub[sub[time_col] > 0]
        if len(sub) < 10 or sub[event_col].nunique() < 2:
            rows.append({"変数": jp(var), "HR": "-", "95% CI": "-", "p値": "-",
                         "_p_raw": np.nan, "_var": var})
            continue
        try:
            mod = PHReg(sub[time_col], sub[[var]], status=sub[event_col])
            result = mod.fit()
            hr = np.exp(result.params[0])
            ci = np.exp(result.conf_int()[0])
            p_val = result.pvalues[0]
            rows.append({
                "変数": jp(var), "HR": f"{hr:.2f}",
                "95% CI": f"{ci[0]:.2f}-{ci[1]:.2f}",
                "p値": _format_p(p_val),
                "_p_raw": p_val, "_var": var,
            })
        except Exception:
            rows.append({"変数": jp(var), "HR": "-", "95% CI": "-", "p値": "-",
                         "_p_raw": np.nan, "_var": var})

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 多変量 Cox 回帰
# ---------------------------------------------------------------------------

def multivariate_cox(df: pd.DataFrame, time_col: str, event_col: str,
                     predictors: list) -> pd.DataFrame:
    """多変量 Cox 比例ハザード回帰。"""
    try:
        from lifelines import CoxPHFitter
    except ImportError:
        return _multivariate_cox_statsmodels(df, time_col, event_col, predictors)

    cols = [time_col, event_col] + predictors
    sub = df[cols].dropna()
    sub = sub[sub[time_col] > 0]
    if len(sub) < 10:
        return pd.DataFrame(columns=["変数", "HR", "95% CI", "p値"])

    try:
        cph = CoxPHFitter()
        cph.fit(sub, duration_col=time_col, event_col=event_col)
        summary = cph.summary
        rows = []
        for idx in summary.index:
            hr = summary.loc[idx, "exp(coef)"]
            ci_lo = summary.loc[idx, "exp(coef) lower 95%"]
            ci_hi = summary.loc[idx, "exp(coef) upper 95%"]
            p_val = summary.loc[idx, "p"]
            rows.append({
                "変数": jp(idx),
                "HR": f"{hr:.2f}",
                "95% CI": f"{ci_lo:.2f}-{ci_hi:.2f}",
                "p値": _format_p(p_val),
            })
        return pd.DataFrame(rows)
    except Exception as e:
        st.error(f"多変量Cox回帰エラー: {e}")
        return pd.DataFrame(columns=["変数", "HR", "95% CI", "p値"])


def _multivariate_cox_statsmodels(df, time_col, event_col, predictors):
    """lifelines がない場合の代替。"""
    try:
        from statsmodels.duration.hazard_regression import PHReg
    except ImportError:
        st.error("Cox回帰には `lifelines` または `statsmodels` が必要です。")
        return pd.DataFrame(columns=["変数", "HR", "95% CI", "p値"])

    cols = [time_col, event_col] + predictors
    sub = df[cols].dropna()
    sub = sub[sub[time_col] > 0]
    if len(sub) < 10:
        return pd.DataFrame(columns=["変数", "HR", "95% CI", "p値"])

    try:
        mod = PHReg(sub[time_col], sub[predictors], status=sub[event_col])
        result = mod.fit()
        rows = []
        for i, var in enumerate(predictors):
            hr = np.exp(result.params[i])
            ci = np.exp(result.conf_int()[i])
            p_val = result.pvalues[i]
            rows.append({
                "変数": jp(var), "HR": f"{hr:.2f}",
                "95% CI": f"{ci[0]:.2f}-{ci[1]:.2f}",
                "p値": _format_p(p_val),
            })
        return pd.DataFrame(rows)
    except Exception as e:
        st.error(f"多変量Cox回帰エラー: {e}")
        return pd.DataFrame(columns=["変数", "HR", "95% CI", "p値"])


# ---------------------------------------------------------------------------
# Excel エクスポート
# ---------------------------------------------------------------------------

def tables_to_excel(tables: dict) -> bytes:
    """
    複数テーブルを1つのExcelファイル（シート別）に変換。
    tables: {"Table1_2群比較": df, "Table2_単変量": df, ...}
    """
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for sheet_name, tbl in tables.items():
            # 内部カラム (_p_raw 等) を除外
            export_cols = [c for c in tbl.columns if not c.startswith("_")]
            tbl[export_cols].to_excel(writer, sheet_name=sheet_name[:31],
                                      index=False)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Streamlit UI — 統計解析ページ
# ---------------------------------------------------------------------------

def render_statistical_analysis(df: pd.DataFrame):
    """統計解析タブのUI。"""
    st.markdown("### 📊 統計解析（2群比較・単変量・多変量）")

    # ----- 利用可能ライブラリチェック -----
    has_scipy = True
    has_statsmodels = True
    has_lifelines = True
    try:
        import scipy.stats
    except ImportError:
        has_scipy = False
    try:
        import statsmodels.api
    except ImportError:
        has_statsmodels = False
    try:
        import lifelines
    except ImportError:
        has_lifelines = False

    missing = []
    if not has_scipy:
        missing.append("scipy")
    if not has_statsmodels:
        missing.append("statsmodels")
    if not has_lifelines:
        missing.append("lifelines（Cox回帰に推奨）")

    if not has_scipy or not has_statsmodels:
        st.error(
            "統計解析には以下のライブラリが必要です。\n\n"
            f"**未インストール**: {', '.join(missing)}\n\n"
            "```bash\npip install scipy statsmodels lifelines\n```"
        )
        return

    if missing:
        st.info(f"💡 追加推奨: {', '.join(missing)} — `pip install lifelines` でCox回帰が高速化します")

    # ===== STEP 1: 解析タイプ選択 =====
    st.markdown("---")
    analysis_type = st.radio(
        "解析タイプ",
        ["ロジスティック回帰（バイナリアウトカム）",
         "Cox比例ハザード回帰（生存時間解析）"],
        horizontal=True, key="stat_type"
    )

    is_cox = "Cox" in analysis_type

    # ===== STEP 2: アウトカム変数選択 =====
    st.markdown("---")
    st.markdown("#### Step 1: アウトカム変数の選択")

    if is_cox:
        col1, col2 = st.columns(2)
        with col1:
            time_options = ["os_months", "rfs_months"]
            time_labels = [jp(t) for t in time_options]
            time_idx = st.selectbox("生存時間変数", range(len(time_options)),
                                    format_func=lambda i: time_labels[i],
                                    key="cox_time")
            time_col = time_options[time_idx]
        with col2:
            event_options = ["os_event", "rfs_event"]
            event_labels = [jp(e) for e in event_options]
            event_idx = st.selectbox("イベント変数", range(len(event_options)),
                                     format_func=lambda i: event_labels[i],
                                     key="cox_event")
            event_col = event_options[event_idx]
        outcome_col = event_col  # for display
    else:
        # --- バイナリアウトカム候補 ---
        binary_candidates = []
        for col in df.columns:
            if _is_binary(df[col]) and df[col].notna().sum() > 10:
                binary_candidates.append(col)
        # 代表的なものを上に
        priority = ["op_complication_yn", "comp_anastomotic_leak", "comp_pancreatic_fistula",
                     "comp_dge", "comp_pneumonia", "comp_ssi", "mortality_30d",
                     "readmission_30d", "recurrence_yn"]
        sorted_binary = [c for c in priority if c in binary_candidates]
        sorted_binary += [c for c in binary_candidates if c not in sorted_binary]

        # --- 連続変数候補（中央値で2値化） ---
        continuous_candidates = []
        for col in df.columns:
            if col in {"id", "patient_id", "study_id"} or col.endswith("_dt") or col.endswith("_label"):
                continue
            if pd.api.types.is_numeric_dtype(df[col]) and not _is_binary(df[col]) and df[col].notna().sum() > 10:
                continuous_candidates.append(col)
        continuous_candidates.sort()

        # 統合リスト
        all_outcome_candidates = sorted_binary + continuous_candidates
        if not all_outcome_candidates:
            st.warning("アウトカム変数の候補が見つかりません。")
            return

        # バイナリ/連続のラベル付け
        def _outcome_label(i):
            c = all_outcome_candidates[i]
            if c in sorted_binary:
                return f"{jp(c)} ({c}) [バイナリ]"
            else:
                med = df[c].dropna().median()
                return f"{jp(c)} ({c}) [連続→中央値{med:.1f}で2値化]"

        outcome_idx = st.selectbox(
            "アウトカム変数",
            range(len(all_outcome_candidates)),
            format_func=_outcome_label,
            key="logit_outcome"
        )
        outcome_col = all_outcome_candidates[outcome_idx]

        # 連続変数の場合は中央値で2値化
        _binarized_outcome = False
        if outcome_col not in sorted_binary:
            _binarized_outcome = True
            median_val = df[outcome_col].dropna().median()
            binarized_col = f"{outcome_col}_median_bin"
            df[binarized_col] = (df[outcome_col] >= median_val).astype(int)
            st.info(f"**{jp(outcome_col)}**: 中央値 = {median_val:.2f} → ≥中央値を1, <中央値を0 として2値化")
            # アウトカム分布表示
            vc = df[binarized_col].value_counts()
            n0 = vc.get(0, 0)
            n1 = vc.get(1, 0)
            st.write(f"0群 (<{median_val:.2f}): {n0}例, 1群 (≥{median_val:.2f}): {n1}例")
            outcome_col = binarized_col  # 以降の解析ではbinarized列を使用
        else:
            # バイナリの場合
            vc = df[outcome_col].value_counts()
            n0 = vc.get(0, 0)
            n1 = vc.get(1, 0)
            st.info(f"**{jp(outcome_col)}**: 0群 = {n0}例, 1群 = {n1}例 "
                    f"(発生率 {n1 / (n0 + n1) * 100:.1f}%)" if (n0 + n1) > 0 else "")

    # ===== STEP 3: 説明変数選択 =====
    st.markdown("---")
    st.markdown("#### Step 2: 説明変数の選択")

    # 数値 + バイナリ変数を候補に
    exclude = {"id", "patient_id", "study_id", "birthdate", "surgery_date",
               "admission_date", "discharge_date", "data_status",
               "surgery_dt", "death_dt", "last_alive_dt", "os_end_dt",
               "rfs_end_dt", "birthdate_dt", "recurrence_dt",
               "os_days", "rfs_days",
               outcome_col}
    if is_cox:
        exclude.add(time_col)
        exclude.add(event_col)

    # _label, _dt 等の派生カラムを除外
    candidates = []
    for col in df.columns:
        if col in exclude:
            continue
        if col.endswith("_label") or col.endswith("_dt"):
            continue
        if df[col].dtype == object and not _is_categorical(df[col]):
            continue
        if df[col].notna().sum() < 10:
            continue
        candidates.append(col)

    # カテゴリ分け
    cat_patient = [c for c in candidates if c in
                   {"age_at_surgery", "sex", "bmi", "bmi_change_pct", "ps", "asa",
                    "disease_class", "height_cm", "weight_admission", "weight_discharge",
                    "smoking", "alcohol", "adl_status", "preop_weight_loss_10pct",
                    "surgery_year"}]
    cat_comor = [c for c in candidates if c.startswith("comor_") and c != "comor_confirmed"]
    cat_med = [c for c in candidates if c.startswith("med_") and c != "med_confirmed"]
    cat_sym = [c for c in candidates if c.startswith("sym_") and c != "sym_confirmed"]
    cat_preop = [c for c in candidates if c.startswith("preop_") or c in
                 {"c_depth", "c_ln_metastasis", "c_distant_metastasis", "c_stage",
                  "c_tumor_size_major_mm"}]
    cat_op = [c for c in candidates if c.startswith("op_") and c not in exclude]
    cat_path = [c for c in candidates if c.startswith("p_") and c in
                {"p_depth", "p_ln_metastasis", "p_stage", "p_residual_tumor"}]
    cat_bio = [c for c in candidates if c in
               {"msi_status", "her2_status", "pdl1_cps", "claudin18_status", "ebv_status"}]
    cat_comp = [c for c in candidates if c.startswith("comp_")]
    cat_other = [c for c in candidates if c not in
                 set(cat_patient + cat_comor + cat_med + cat_sym + cat_preop +
                     cat_op + cat_path + cat_bio + cat_comp)]

    # グループごとに選択
    groups = [
        ("患者背景", cat_patient),
        ("併存疾患", cat_comor),
        ("内服薬", cat_med),
        ("症状", cat_sym),
        ("術前検査・Stage", cat_preop),
        ("手術因子", cat_op),
        ("病理", cat_path),
        ("バイオマーカー", cat_bio),
        ("合併症", cat_comp),
        ("その他", cat_other),
    ]

    selected_vars = []
    for group_name, group_vars in groups:
        if not group_vars:
            continue
        with st.expander(f"{group_name} ({len(group_vars)}項目)", expanded=(group_name == "患者背景")):
            labels = [f"{jp(v)} ({v})" for v in group_vars]
            chosen = st.multiselect(
                f"{group_name}の変数",
                group_vars,
                format_func=lambda v: f"{jp(v)} ({v})",
                key=f"stat_vars_{group_name}",
                label_visibility="collapsed",
            )
            selected_vars.extend(chosen)

    if not selected_vars:
        st.warning("説明変数を1つ以上選択してください。")
        return

    st.markdown(f"**選択された変数**: {len(selected_vars)}個")

    # ===== STEP 4: 解析実行（session_state で結果を保持） =====
    st.markdown("---")

    # session_state 初期化
    if "_stat_tbl1" not in st.session_state:
        st.session_state["_stat_tbl1"] = None
        st.session_state["_stat_tbl2"] = None
        st.session_state["_stat_tbl3"] = None
        st.session_state["_stat_sig_vars"] = []

    if st.button("🔬 解析を実行", type="primary", key="run_stat"):
        with st.spinner("2群比較・単変量解析を実行中..."):
            # --- Table 1: 2群比較 ---
            if not is_cox:
                tbl1 = two_group_comparison(df, outcome_col, selected_vars)
            else:
                tbl1 = two_group_comparison(df, event_col, selected_vars)

            # --- Table 2: 単変量解析 ---
            if is_cox:
                tbl2 = univariate_cox(df, time_col, event_col, selected_vars)
            else:
                tbl2 = univariate_logistic(df, outcome_col, selected_vars)

            # p < 0.05 の変数を抽出
            sig_vars = []
            if "_p_raw" in tbl2.columns and "_var" in tbl2.columns:
                for _, row in tbl2.iterrows():
                    if pd.notna(row.get("_p_raw")) and row["_p_raw"] < 0.05:
                        v = row["_var"]
                        if v not in sig_vars:
                            sig_vars.append(v)

            # session_state に保存
            st.session_state["_stat_tbl1"] = tbl1
            st.session_state["_stat_tbl2"] = tbl2
            st.session_state["_stat_tbl3"] = None  # 多変量はリセット
            st.session_state["_stat_sig_vars"] = sig_vars

    # --- 結果表示（session_state から読み出し） ---
    tbl1 = st.session_state.get("_stat_tbl1")
    tbl2 = st.session_state.get("_stat_tbl2")
    sig_vars = st.session_state.get("_stat_sig_vars", [])

    if tbl1 is not None:
        st.markdown("#### Table 1: 2群比較")
        st.dataframe(tbl1, hide_index=True, use_container_width=True)

    if tbl2 is not None:
        st.markdown("---")
        st.markdown("#### Table 2: 単変量解析")
        display_cols = [c for c in tbl2.columns if not c.startswith("_")]
        st.dataframe(tbl2[display_cols], hide_index=True, use_container_width=True)

        # --- 多変量候補の選択 ---
        st.markdown("---")
        st.markdown("#### Table 3: 多変量解析")
        st.markdown("単変量解析で p < 0.05 の変数を自動選択しています（手動で変更可）。")

        multi_vars = st.multiselect(
            "多変量解析に投入する変数",
            selected_vars,
            default=sig_vars,
            format_func=lambda v: f"{jp(v)} ({v})",
            key="multi_vars",
        )

        if multi_vars and st.button("🔬 多変量解析を実行", type="primary", key="run_multi"):
            with st.spinner("多変量解析中..."):
                if is_cox:
                    tbl3 = multivariate_cox(df, time_col, event_col, multi_vars)
                else:
                    tbl3 = multivariate_logistic(df, outcome_col, multi_vars)
                st.session_state["_stat_tbl3"] = tbl3

        # 多変量結果の表示
        tbl3 = st.session_state.get("_stat_tbl3")
        if tbl3 is not None and not tbl3.empty:
            st.dataframe(tbl3, hide_index=True, use_container_width=True)

    # --- Excel エクスポート ---
    tables = {}
    if tbl1 is not None:
        tables["Table1_2群比較"] = tbl1
    if tbl2 is not None:
        tables["Table2_単変量"] = tbl2
    tbl3 = st.session_state.get("_stat_tbl3")
    if tbl3 is not None and not tbl3.empty:
        tables["Table3_多変量"] = tbl3

    if tables:
        st.markdown("---")
        st.markdown("#### 📥 結果のエクスポート")
        col1, col2 = st.columns(2)
        with col1:
            excel_data = tables_to_excel(tables)
            st.download_button(
                "📥 Excel (.xlsx) でダウンロード",
                data=excel_data,
                file_name="statistical_analysis.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="dl_xlsx",
            )
        with col2:
            if tbl1 is not None:
                csv = tbl1.to_csv(index=False).encode("utf-8-sig")
                st.download_button(
                    "📥 CSV でダウンロード (Table 1)",
                    data=csv,
                    file_name="table1_comparison.csv",
                    mime="text/csv",
                    key="dl_csv",
                )
