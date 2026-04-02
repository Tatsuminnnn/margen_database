"""
上部消化管グループ 統合症例登録DB — analytics.py
インタラクティブ分析ダッシュボード

GDC CDave 風のデータ探索・可視化モジュール
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime

from database import get_db
from codebook import get_codebook, get_column_label, get_all_column_labels, COLUMN_LABELS
from statistical_analysis import render_statistical_analysis

# ============================================================
# カラーパレット
# ============================================================
COLORS = px.colors.qualitative.Set2
COLORS_SEQUENTIAL = px.colors.sequential.Blues

PLOTLY_LAYOUT = dict(
    template="plotly_white",
    font=dict(family="Helvetica, Arial, sans-serif", size=12),
    margin=dict(l=40, r=20, t=50, b=40),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
)

# ============================================================
# 日本語ラベル — codebook.py の COLUMN_LABELS を唯一のソースとする
# ============================================================
# プレフィックス別フィルタ（患者背景タブの棒グラフ用）
COMOR_LABELS = {k: v for k, v in COLUMN_LABELS.items() if k.startswith("comor_")}
MED_LABELS   = {k: v for k, v in COLUMN_LABELS.items() if k.startswith("med_")}
SYM_LABELS   = {k: v for k, v in COLUMN_LABELS.items() if k.startswith("sym_")}
COL_JP_LABELS = COLUMN_LABELS  # 後方互換

# ============================================================
# データ取得
# ============================================================
@st.cache_data(ttl=60)
def load_analysis_data():
    """全症例の分析用データをJOINして返す。"""
    with get_db() as conn:
        df = pd.read_sql_query("""
            SELECT
                p.id, p.study_id, p.sex, p.birthdate, p.surgery_date,
                p.disease_class, p.ps, p.asa, p.data_status,
                p.height_cm, p.weight_admission, p.weight_discharge,
                p.admission_date, p.discharge_date,
                p.comor_hypertension, p.comor_cardiovascular, p.comor_cerebrovascular,
                p.comor_respiratory, p.comor_renal, p.comor_renal_dialysis,
                p.comor_hepatic, p.comor_diabetes, p.comor_endocrine,
                p.comor_collagen, p.comor_hematologic, p.comor_neurologic,
                p.comor_psychiatric, p.comor_other,
                p.med_antihypertensive, p.med_antithrombotic, p.med_oral_hypoglycemic,
                p.med_insulin, p.med_steroid_immunosup, p.med_antineoplastic,
                p.med_thyroid, p.med_psychotropic, p.med_other,
                p.sym_asymptomatic, p.sym_epigastric_pain, p.sym_dysphagia,
                p.sym_weight_loss, p.sym_anemia, p.sym_melena,
                p.sym_hematemesis, p.sym_nausea_vomiting, p.sym_abdominal_distension,
                p.sym_obstruction, p.sym_other,
                tp.c_depth, tp.c_ln_metastasis, tp.c_distant_metastasis, tp.c_stage,
                tp.c_macroscopic_type, tp.c_histology1, tp.c_histology2, tp.c_histology3,
                tp.c_tumor_size_major_mm,
                tp.preop_alb, tp.preop_hb, tp.preop_crp,
                s.op_approach, s.op_procedure, s.op_dissection,
                s.op_reconstruction, s.op_anastomosis_method,
                s.op_surgeon, s.op_assistant1, s.op_assistant2, s.op_scopist,
                s.op_time_min, s.op_console_time_min, s.op_blood_loss_ml,
                s.op_complication_yn, s.op_cd_grade_max,
                s.op_transfusion_intra, s.op_transfusion_post,
                s.op_icu_days, s.op_reop_yn, s.readmission_30d,
                s.comp_ssi, s.comp_anastomotic_leak, s.comp_pancreatic_fistula,
                s.comp_pneumonia, s.comp_ileus, s.comp_bleeding,
                s.comp_dvt_pe, s.comp_dge, s.comp_wound_dehiscence,
                s.comp_intra_abd_abscess, s.comp_bile_leak,
                s.comp_duodenal_stump_leak, s.comp_rln_palsy,
                s.comp_chylothorax, s.comp_cardiac, s.comp_delirium,
                s.mortality_30d AS s_mortality_30d, s.mortality_inhospital AS s_mortality_inhospital,
                pa.p_depth, pa.p_ln_metastasis, pa.p_stage,
                pa.p_residual_tumor, pa.p_histology1,
                pa.p_ly, pa.p_v, pa.p_inf,
                pa.msi_status, pa.her2_status, pa.pdl1_cps,
                pa.claudin18_status, pa.fgfr2b_status, pa.ebv_status,
                o.vital_status, o.recurrence_yn,
                o.recurrence_date, o.last_alive_date, o.death_date,
                o.mortality_30d, o.mortality_inhospital,
                neo.nac_yn, neo.nac_regimen, neo.recist_overall,
                adj.adj_yn, adj.adj_regimen
            FROM patients p
            LEFT JOIN tumor_preop tp ON p.id = tp.patient_id
            LEFT JOIN surgery s ON p.id = s.patient_id
            LEFT JOIN pathology pa ON p.id = pa.patient_id
            LEFT JOIN outcome o ON p.id = o.patient_id
            LEFT JOIN neoadjuvant neo ON p.id = neo.patient_id
            LEFT JOIN adjuvant_chemo adj ON p.id = adj.patient_id
        """, conn)
    return df


def prepare_survival_data(df):
    """OS/RFS 計算用のカラムを追加。"""
    df = df.copy()

    # 手術日をdatetimeに変換
    df["surgery_dt"] = pd.to_datetime(df["surgery_date"], errors="coerce")

    # OS: 手術日 → 死亡日 or 最終確認日
    df["death_dt"] = pd.to_datetime(df["death_date"], errors="coerce")
    df["last_alive_dt"] = pd.to_datetime(df["last_alive_date"], errors="coerce")

    # OS endpoint
    df["os_end_dt"] = df["death_dt"].fillna(df["last_alive_dt"])
    df["os_days"] = (df["os_end_dt"] - df["surgery_dt"]).dt.days
    df["os_months"] = df["os_days"] / 30.44
    df["os_event"] = df["vital_status"].isin([2, 3, 4, 5]).astype(int)  # 死亡=1

    # RFS: 手術日 → 再発日 or OS endpoint
    df["recurrence_dt"] = pd.to_datetime(df["recurrence_date"], errors="coerce")
    df["rfs_end_dt"] = df["recurrence_dt"].fillna(df["os_end_dt"])
    df["rfs_days"] = (df["rfs_end_dt"] - df["surgery_dt"]).dt.days
    df["rfs_months"] = df["rfs_days"] / 30.44
    df["rfs_event"] = ((df["recurrence_yn"] == 1) | df["os_event"].astype(bool)).astype(int)

    # 年齢
    df["birthdate_dt"] = pd.to_datetime(df["birthdate"], errors="coerce")
    df["age_at_surgery"] = ((df["surgery_dt"] - df["birthdate_dt"]).dt.days / 365.25).round(0)

    # 手術年
    df["surgery_year"] = df["surgery_dt"].dt.year

    # BMI計算 (weight_admission / (height_cm/100)^2)
    if "height_cm" in df.columns and "weight_admission" in df.columns:
        h = df["height_cm"] / 100.0
        df["bmi"] = df["weight_admission"] / (h * h)
        df["bmi"] = df["bmi"].where(df["bmi"].between(10, 60))  # 異常値除外
    # BMI変化率 (入院時→退院時)
    if "weight_admission" in df.columns and "weight_discharge" in df.columns:
        df["bmi_change_pct"] = (
            (df["weight_discharge"] - df["weight_admission"]) / df["weight_admission"] * 100
        ).where(df["weight_admission"] > 0)

    return df


def apply_label_mapping(df):
    """コードブックのラベルマッピングを適用。"""
    mappings = {
        "disease_class": "disease_class",
        "sex": "sex",
        "op_approach": "op_approach",
        "op_procedure": "op_procedure_gastric",
        "op_dissection": "op_dissection_gastric",
        "op_reconstruction": "op_reconstruction_gastric",
        "vital_status": "vital_status",
        "ps": "ps",
        "asa": "asa",
    }
    for col, field in mappings.items():
        cb = get_codebook(field)
        if cb and col in df.columns:
            df[f"{col}_label"] = df[col].map(cb).fillna("不明")

    # Stage labels
    for prefix in ["c_stage", "p_stage"]:
        cb = get_codebook(f"{prefix}_gastric", 1)
        if cb and prefix in df.columns:
            df[f"{prefix}_label"] = df[prefix].map(cb).fillna("不明")

    return df


# ============================================================
# Kaplan-Meier 生存曲線
# ============================================================
def kaplan_meier_estimate(durations, events):
    """
    Kaplan-Meier推定量を計算。
    lifelines が使えない環境にも対応するため自前実装。
    戻り値: (times, survival_prob, ci_lower, ci_upper, at_risk)
    """
    valid = ~(pd.isna(durations) | pd.isna(events))
    T = np.array(durations[valid], dtype=float)
    E = np.array(events[valid], dtype=int)

    if len(T) == 0:
        return np.array([0]), np.array([1.0]), np.array([1.0]), np.array([1.0]), np.array([0])

    # イベント時刻のソート
    unique_times = np.sort(np.unique(T[E == 1]))
    if len(unique_times) == 0:
        return np.array([0, T.max()]), np.array([1.0, 1.0]), np.array([1.0, 1.0]), np.array([1.0, 1.0]), np.array([len(T), 0])

    times = [0]
    survival = [1.0]
    variance_sum = 0.0
    n = len(T)
    at_risk_list = [n]

    for t in unique_times:
        n_i = np.sum(T >= t)          # at risk
        d_i = np.sum((T == t) & (E == 1))  # events
        if n_i == 0:
            continue
        s = 1 - d_i / n_i
        survival.append(survival[-1] * s)
        times.append(t)
        at_risk_list.append(n_i)
        if d_i > 0 and n_i > d_i:
            variance_sum += d_i / (n_i * (n_i - d_i))

    times = np.array(times)
    survival = np.array(survival)
    at_risk_arr = np.array(at_risk_list)

    # Greenwood CIの近似
    se = np.array([s * np.sqrt(variance_sum) if s > 0 else 0 for s in survival])
    ci_lower = np.clip(survival - 1.96 * se, 0, 1)
    ci_upper = np.clip(survival + 1.96 * se, 0, 1)

    return times, survival, ci_lower, ci_upper, at_risk_arr


def log_rank_test(groups_data):
    """
    Simple log-rank test implementation.
    groups_data: list of (durations, events) tuples
    Returns chi2, p_value
    """
    try:
        from scipy import stats as sp_stats

        # Prepare data for log-rank test
        all_times = []
        all_events = []
        all_groups = []

        for group_idx, (durations, events) in enumerate(groups_data):
            valid = ~(pd.isna(durations) | pd.isna(events))
            T = np.array(durations[valid], dtype=float)
            E = np.array(events[valid], dtype=int)
            all_times.extend(T)
            all_events.extend(E)
            all_groups.extend([group_idx] * len(T))

        if len(all_times) == 0:
            return None, None

        # Sort by time
        sorted_idx = np.argsort(all_times)
        times = np.array(all_times)[sorted_idx]
        events = np.array(all_events)[sorted_idx]
        groups = np.array(all_groups)[sorted_idx]

        # Calculate log-rank statistic
        unique_times = np.unique(times[events == 1])
        n_groups = len(groups_data)

        # Observed and expected counts
        O = np.zeros(n_groups)
        E_count = np.zeros(n_groups)

        for t in unique_times:
            mask = times == t
            event_mask = mask & (events == 1)
            d_i = np.sum(event_mask)
            n_i = np.sum(mask)

            if n_i > 0 and d_i > 0:
                for g in range(n_groups):
                    n_ig = np.sum((groups == g) & mask)
                    d_ig = np.sum((groups == g) & event_mask)
                    O[g] += d_ig
                    E_count[g] += d_i * n_ig / n_i

        # Chi-square statistic
        if np.sum(E_count) > 0:
            chi2 = np.sum((O - E_count) ** 2 / np.maximum(E_count, 1e-10))
            p_value = 1 - sp_stats.chi2.cdf(chi2, df=n_groups-1)
            return chi2, p_value
        else:
            return None, None
    except ImportError:
        st.warning("scipy がインストールされていないため、ログランク検定は実行できません。")
        return None, None


def plot_kaplan_meier(df, time_col, event_col, group_col=None, title="生存曲線",
                      xlabel="月", max_months=120):
    """Kaplan-Meier曲線をPlotlyで描画。"""
    fig = go.Figure()

    valid = df[time_col].notna() & df[event_col].notna()
    if group_col:
        valid = valid & df[group_col].notna()
    plot_df = df[valid].copy()

    # 月単位に変換
    if "days" in time_col:
        plot_df["_time"] = plot_df[time_col] / 30.44
    elif "months" in time_col:
        plot_df["_time"] = plot_df[time_col]
    else:
        plot_df["_time"] = plot_df[time_col]

    plot_df = plot_df[plot_df["_time"] >= 0]
    if max_months:
        plot_df["_time"] = plot_df["_time"].clip(upper=max_months)

    if group_col and group_col in plot_df.columns:
        groups = sorted(plot_df[group_col].dropna().unique())
        for i, grp in enumerate(groups):
            mask = plot_df[group_col] == grp
            sub = plot_df[mask]
            if len(sub) < 2:
                continue
            times, surv, ci_lo, ci_hi, _ = kaplan_meier_estimate(sub["_time"], sub[event_col])

            color = COLORS[i % len(COLORS)]
            n_events = int(sub[event_col].sum())
            n_total = len(sub)

            # CI fill
            fig.add_trace(go.Scatter(
                x=np.concatenate([times, times[::-1]]),
                y=np.concatenate([ci_hi, ci_lo[::-1]]),
                fill="toself", fillcolor=color, opacity=0.1,
                line=dict(width=0), showlegend=False, hoverinfo="skip",
            ))
            # KM line
            fig.add_trace(go.Scatter(
                x=times, y=surv, mode="lines",
                name=f"{grp} (n={n_total}, events={n_events})",
                line=dict(color=color, width=2, shape="hv"),
                hovertemplate=f"{grp}<br>%{{x:.0f}}月: %{{y:.1%}}<extra></extra>",
            ))
    else:
        times, surv, ci_lo, ci_hi, _ = kaplan_meier_estimate(plot_df["_time"], plot_df[event_col])
        n_events = int(plot_df[event_col].sum())
        n_total = len(plot_df)

        fig.add_trace(go.Scatter(
            x=np.concatenate([times, times[::-1]]),
            y=np.concatenate([ci_hi, ci_lo[::-1]]),
            fill="toself", fillcolor=COLORS[0], opacity=0.1,
            line=dict(width=0), showlegend=False, hoverinfo="skip",
        ))
        fig.add_trace(go.Scatter(
            x=times, y=surv, mode="lines",
            name=f"全体 (n={n_total}, events={n_events})",
            line=dict(color=COLORS[0], width=2, shape="hv"),
            hovertemplate="全体<br>%{x:.0f}月: %{y:.1%}<extra></extra>",
        ))

    fig.update_layout(
        title=title,
        xaxis_title=xlabel,
        yaxis_title="生存率",
        yaxis=dict(range=[0, 1.05], tickformat=".0%"),
        xaxis=dict(range=[0, max_months]),
        **PLOTLY_LAYOUT,
    )
    return fig


# ============================================================
# 手術成績
# ============================================================
def plot_operative_outcomes(df):
    """手術成績のバイオリンプロットと棒グラフを返す。"""
    figs = []

    # 1. 手術時間・出血量 by 到達法
    if "op_approach_label" in df.columns:
        valid = df["op_time_min"].notna() & df["op_approach_label"].notna()
        sub = df[valid]
        if len(sub) > 0:
            fig = px.violin(sub, x="op_approach_label", y="op_time_min",
                           color="op_approach_label", color_discrete_sequence=COLORS,
                           title="手術時間（到達法別）",
                           labels={"op_approach_label": "到達法", "op_time_min": "手術時間 (分)"},
                           points="all", box=True)
            fig.update_layout(**PLOTLY_LAYOUT, showlegend=False)
            figs.append(("手術時間（到達法別）", fig))

        valid = df["op_blood_loss_ml"].notna() & df["op_approach_label"].notna()
        sub = df[valid]
        if len(sub) > 0:
            fig = px.violin(sub, x="op_approach_label", y="op_blood_loss_ml",
                           color="op_approach_label", color_discrete_sequence=COLORS,
                           title="出血量（到達法別）",
                           labels={"op_approach_label": "到達法", "op_blood_loss_ml": "出血量 (mL)"},
                           points="all", box=True)
            fig.update_layout(**PLOTLY_LAYOUT, showlegend=False)
            figs.append(("出血量（到達法別）", fig))

    # 2. 手術成績の年次推移
    if "surgery_year" in df.columns:
        year_data = []
        for year in sorted(df["surgery_year"].dropna().unique()):
            ydf = df[df["surgery_year"] == year]
            if "op_procedure_label" in ydf.columns:
                for proc in sorted(ydf["op_procedure_label"].dropna().unique()):
                    pdf = ydf[ydf["op_procedure_label"] == proc]
                    ot = pdf["op_time_min"].dropna()
                    if len(ot) > 0:
                        year_data.append({
                            "年": year,
                            "術式": proc,
                            "手術時間 中央値": ot.median(),
                            "症例数": len(ot)
                        })

        if year_data:
            year_df = pd.DataFrame(year_data)
            fig = px.bar(year_df, x="年", y="手術時間 中央値", color="術式",
                        barmode="group", title="年次別 手術時間 (中央値)",
                        labels={"年": "手術年", "手術時間 中央値": "手術時間 (分)"})
            fig.update_layout(**PLOTLY_LAYOUT)
            figs.append(("年次別手術時間", fig))

    # 3. 手術時間by術式
    if "op_procedure_label" in df.columns:
        valid = df["op_time_min"].notna() & df["op_procedure_label"].notna()
        sub = df[valid]
        if len(sub) > 0:
            fig = px.violin(sub, x="op_procedure_label", y="op_time_min",
                           color="op_procedure_label", color_discrete_sequence=COLORS,
                           title="手術時間（術式別）",
                           labels={"op_procedure_label": "術式", "op_time_min": "手術時間 (分)"},
                           points="all", box=True)
            fig.update_layout(**PLOTLY_LAYOUT, showlegend=False)
            figs.append(("手術時間（術式別）", fig))

    return figs


# ============================================================
# 合併症
# ============================================================
def plot_complication_rates(df):
    """合併症関連の図を返す。"""
    figs = []

    # 1. 6つの主要合併症の割合
    comp_cols = {
        "comp_dge": "DGE",
        "comp_anastomotic_leak": "縫合不全",
        "comp_pancreatic_fistula": "膵液瘻",
        "comp_rln_palsy": "反回神経麻痺",
        "comp_pneumonia": "肺炎",
        "comp_ileus": "イレウス"
    }

    # 全体の合併症率
    comp_rates = []
    for col, label in comp_cols.items():
        if col in df.columns:
            valid = df[col].notna()
            if valid.any():
                rate = (df[valid][col] > 0).sum() / len(df[valid]) * 100
                comp_rates.append({"合併症": label, "発生率(%)": rate})

    if comp_rates:
        comp_df = pd.DataFrame(comp_rates)
        fig = px.bar(comp_df, x="発生率(%)", y="合併症", orientation="h",
                    title="主要合併症 発生率",
                    labels={"合併症": "合併症名"})
        fig.update_layout(**PLOTLY_LAYOUT)
        figs.append(("主要合併症発生率", fig))

    # 2. 主要合併症 by 術式と年
    if "op_procedure_label" in df.columns and "surgery_year" in df.columns:
        year_proc_data = []
        for year in sorted(df["surgery_year"].dropna().unique()):
            ydf = df[df["surgery_year"] == year]
            for proc in sorted(ydf["op_procedure_label"].dropna().unique()):
                pdf = ydf[ydf["op_procedure_label"] == proc]
                for col, label in list(comp_cols.items())[:6]:  # top 6
                    if col in pdf.columns:
                        valid = pdf[col].notna()
                        if valid.any():
                            rate = (pdf[valid][col] > 0).sum() / len(pdf[valid]) * 100
                            year_proc_data.append({
                                "年": year,
                                "術式": proc,
                                "合併症": label,
                                "発生率(%)": rate
                            })

        if year_proc_data:
            year_proc_df = pd.DataFrame(year_proc_data)
            fig = px.bar(year_proc_df, x="年", y="発生率(%)", color="合併症",
                        facet_col="術式", facet_col_wrap=2,
                        title="合併症発生率 (年別・術式別)")
            fig.update_layout(**PLOTLY_LAYOUT, showlegend=True)
            figs.append(("合併症発生率_年別術式別", fig))

    return figs


# ============================================================
# 患者背景
# ============================================================
def plot_demographics(df):
    """患者背景を示す複数の図を返す。"""
    figs = []

    # 1. 年齢分布
    if "age_at_surgery" in df.columns and df["age_at_surgery"].notna().any():
        fig = px.histogram(df[df["age_at_surgery"].notna()], x="age_at_surgery",
                          nbins=20, title="年齢分布",
                          labels={"age_at_surgery": "手術時年齢 (歳)"},
                          color_discrete_sequence=[COLORS[0]])
        fig.update_layout(**PLOTLY_LAYOUT)
        figs.append(("年齢分布", fig))

    # 2. 性別分布
    if "sex_label" in df.columns and df["sex_label"].notna().any():
        sex_counts = df["sex_label"].value_counts()
        fig = px.bar(x=sex_counts.index, y=sex_counts.values,
                    title="性別分布", labels={"x": "性別", "y": "症例数"},
                    color_discrete_sequence=COLORS)
        fig.update_layout(**PLOTLY_LAYOUT)
        figs.append(("性別分布", fig))

    # 3. 疾患分類分布
    if "disease_class_label" in df.columns and df["disease_class_label"].notna().any():
        dc_counts = df["disease_class_label"].value_counts()
        fig = px.bar(x=dc_counts.index, y=dc_counts.values,
                    title="疾患分類分布", labels={"x": "疾患分類", "y": "症例数"},
                    color_discrete_sequence=COLORS)
        fig.update_layout(**PLOTLY_LAYOUT)
        figs.append(("疾患分類分布", fig))

    # 4. pStage分布
    if "p_stage_label" in df.columns and df["p_stage_label"].notna().any():
        stage_counts = df["p_stage_label"].value_counts().sort_index()
        fig = px.bar(x=stage_counts.index, y=stage_counts.values,
                    title="pStage分布", labels={"x": "pStage", "y": "症例数"},
                    color_discrete_sequence=COLORS)
        fig.update_layout(**PLOTLY_LAYOUT)
        figs.append(("pStage分布", fig))

    # 5. BMI分布
    if "bmi" in df.columns and df["bmi"].notna().any():
        fig = px.violin(df[df["bmi"].notna()], y="bmi",
                       title="BMI分布", box=True, points="all",
                       labels={"bmi": "BMI"})
        fig.update_layout(**PLOTLY_LAYOUT)
        figs.append(("BMI分布", fig))

    # 5b. 体重変化率（入院時→退院時）
    if "bmi_change_pct" in df.columns and df["bmi_change_pct"].notna().any():
        sub = df[df["bmi_change_pct"].notna()]
        fig = px.histogram(sub, x="bmi_change_pct", nbins=20,
                          title="体重変化率（入院時→退院時）",
                          labels={"bmi_change_pct": "体重変化率 (%)"},
                          color_discrete_sequence=[COLORS[2]])
        fig.add_vline(x=0, line_dash="dash", line_color="red",
                      annotation_text="変化なし")
        median_val = sub["bmi_change_pct"].median()
        fig.add_vline(x=median_val, line_dash="dot", line_color="blue",
                      annotation_text=f"中央値: {median_val:.1f}%")
        fig.update_layout(**PLOTLY_LAYOUT)
        figs.append(("体重変化率", fig))

    # 6. 内服薬の割合
    med_cols = [col for col in MED_LABELS.keys() if col in df.columns]
    if med_cols:
        med_data = []
        for col in med_cols:
            if df[col].notna().any():
                valid = df[col].notna()
                pct = (df[valid][col] > 0).sum() / len(df[valid]) * 100
                med_data.append({
                    "薬剤": MED_LABELS.get(col, col),
                    "使用率(%)": pct
                })

        if med_data:
            med_df = pd.DataFrame(med_data).sort_values("使用率(%)", ascending=True)
            fig = px.bar(med_df, x="使用率(%)", y="薬剤", orientation="h",
                        title="内服薬の割合",
                        color_discrete_sequence=[COLORS[0]])
            fig.update_layout(**PLOTLY_LAYOUT)
            figs.append(("内服薬の割合", fig))

    # 7. 併存疾患の割合
    comor_cols = [col for col in COMOR_LABELS.keys() if col in df.columns]
    if comor_cols:
        comor_data = []
        for col in comor_cols:
            if df[col].notna().any():
                valid = df[col].notna()
                pct = (df[valid][col] > 0).sum() / len(df[valid]) * 100
                comor_data.append({
                    "疾患": COMOR_LABELS.get(col, col),
                    "合併率(%)": pct
                })

        if comor_data:
            comor_df = pd.DataFrame(comor_data).sort_values("合併率(%)", ascending=True)
            fig = px.bar(comor_df, x="合併率(%)", y="疾患", orientation="h",
                        title="併存疾患の割合",
                        color_discrete_sequence=[COLORS[1]])
            fig.update_layout(**PLOTLY_LAYOUT)
            figs.append(("併存疾患の割合", fig))

    # 8. 症状の割合
    sym_cols = [col for col in SYM_LABELS.keys() if col in df.columns]
    if sym_cols:
        sym_data = []
        for col in sym_cols:
            if df[col].notna().any():
                valid = df[col].notna()
                pct = (df[valid][col] > 0).sum() / len(df[valid]) * 100
                sym_data.append({
                    "症状": SYM_LABELS.get(col, col),
                    "出現率(%)": pct
                })

        if sym_data:
            sym_df = pd.DataFrame(sym_data).sort_values("出現率(%)", ascending=True)
            fig = px.bar(sym_df, x="出現率(%)", y="症状", orientation="h",
                        title="症状の割合",
                        color_discrete_sequence=[COLORS[2]])
            fig.update_layout(**PLOTLY_LAYOUT)
            figs.append(("症状の割合", fig))

    return figs


# ============================================================
# バイオマーカー
# ============================================================
def plot_biomarkers(df):
    """バイオマーカーの図を返す。"""
    figs = []

    # 術前アルブミン
    if "preop_alb" in df.columns and df["preop_alb"].notna().any():
        fig = px.violin(df[df["preop_alb"].notna()], y="preop_alb",
                       title="術前アルブミン", box=True, points="all",
                       labels={"preop_alb": "アルブミン (g/dL)"})
        fig.update_layout(**PLOTLY_LAYOUT)
        figs.append(("術前アルブミン", fig))

    # 術前ヘモグロビン
    if "preop_hb" in df.columns and df["preop_hb"].notna().any():
        fig = px.violin(df[df["preop_hb"].notna()], y="preop_hb",
                       title="術前ヘモグロビン", box=True, points="all",
                       labels={"preop_hb": "ヘモグロビン (g/dL)"})
        fig.update_layout(**PLOTLY_LAYOUT)
        figs.append(("術前ヘモグロビン", fig))

    # 術前CRP
    if "preop_crp" in df.columns and df["preop_crp"].notna().any():
        fig = px.violin(df[df["preop_crp"].notna()], y="preop_crp",
                       title="術前CRP", box=True, points="all",
                       labels={"preop_crp": "CRP (mg/dL)"})
        fig.update_layout(**PLOTLY_LAYOUT)
        figs.append(("術前CRP", fig))

    # MSI status
    if "msi_status" in df.columns and df["msi_status"].notna().any():
        msi_counts = df["msi_status"].value_counts()
        fig = px.bar(x=msi_counts.index, y=msi_counts.values,
                    title="MSI ステータス", labels={"x": "MSI", "y": "症例数"},
                    color_discrete_sequence=COLORS)
        fig.update_layout(**PLOTLY_LAYOUT)
        figs.append(("MSI_status", fig))

    # Her2 status
    if "her2_status" in df.columns and df["her2_status"].notna().any():
        her2_counts = df["her2_status"].value_counts()
        fig = px.bar(x=her2_counts.index, y=her2_counts.values,
                    title="HER2 ステータス", labels={"x": "HER2", "y": "症例数"},
                    color_discrete_sequence=COLORS)
        fig.update_layout(**PLOTLY_LAYOUT)
        figs.append(("HER2_status", fig))

    # PD-L1 CPS
    if "pdl1_cps" in df.columns and df["pdl1_cps"].notna().any():
        fig = px.violin(df[df["pdl1_cps"].notna()], y="pdl1_cps",
                       title="PD-L1 CPS スコア", box=True, points="all",
                       labels={"pdl1_cps": "CPS"})
        fig.update_layout(**PLOTLY_LAYOUT)
        figs.append(("PDL1_CPS", fig))

    return figs


# ============================================================
# 統計サマリー
# ============================================================
def compute_summary_stats(df):
    """サマリー統計を計算。"""
    stats = {}

    stats["n"] = len(df)

    # 手術時間
    ot = df["op_time_min"].dropna()
    if len(ot) > 0:
        stats["op_time_median"] = f"{ot.median():.0f}"
        stats["op_time_iqr"] = f"{ot.quantile(0.25):.0f}-{ot.quantile(0.75):.0f}"
    else:
        stats["op_time_median"] = "-"
        stats["op_time_iqr"] = "-"

    # 出血量
    bl = df["op_blood_loss_ml"].dropna()
    if len(bl) > 0:
        stats["blood_loss_median"] = f"{bl.median():.0f}"
        stats["blood_loss_iqr"] = f"{bl.quantile(0.25):.0f}-{bl.quantile(0.75):.0f}"
    else:
        stats["blood_loss_median"] = "-"
        stats["blood_loss_iqr"] = "-"

    # 合併症率
    comp = df["op_complication_yn"].dropna()
    if len(comp) > 0:
        stats["complication_rate"] = f"{(comp > 0).sum() / len(comp) * 100:.1f}%"
    else:
        stats["complication_rate"] = "-"

    # CD Grade III以上
    cd = df["op_cd_grade_max"].dropna()
    if len(cd) > 0:
        stats["cd3_rate"] = f"{(cd >= 3).sum() / len(cd) * 100:.1f}%"
    else:
        stats["cd3_rate"] = "-"

    # 縫合不全率
    leak = df["comp_anastomotic_leak"].dropna()
    if len(leak) > 0:
        stats["leak_rate"] = f"{(leak > 0).sum() / len(leak) * 100:.1f}%"
    else:
        stats["leak_rate"] = "-"

    # 膵液瘻率
    fistula = df["comp_pancreatic_fistula"].dropna()
    if len(fistula) > 0:
        stats["fistula_rate"] = f"{(fistula > 0).sum() / len(fistula) * 100:.1f}%"
    else:
        stats["fistula_rate"] = "-"

    # 30日死亡率
    mort30 = df["s_mortality_30d"].dropna()
    if len(mort30) > 0:
        stats["mortality_30d"] = f"{(mort30 > 0).sum() / len(mort30) * 100:.1f}%"
    else:
        stats["mortality_30d"] = "-"

    # 在院死亡
    mort_hosp = df["s_mortality_inhospital"].dropna()
    if len(mort_hosp) > 0:
        stats["mortality_hosp"] = f"{(mort_hosp > 0).sum() / len(mort_hosp) * 100:.1f}%"
    else:
        stats["mortality_hosp"] = "-"

    return stats


# ============================================================
# メインダッシュボード
# ============================================================
@st.cache_data(ttl=60)
def _load_analysis_df():
    """共有: 分析用DataFrame を読み込み・前処理して返す。"""
    raw_df = load_analysis_data()
    if raw_df.empty:
        return None
    df = prepare_survival_data(raw_df)
    df = apply_label_mapping(df)
    return df


def render_analytics_dashboard():
    """分析ダッシュボードをレンダリング。"""
    st.markdown("## 📊 データ分析ダッシュボード")

    df = _load_analysis_df()
    if df is None:
        st.info("分析するデータがありません。症例を登録してください。")
        return

    # ============================
    # メイン領域フィルター
    # ============================
    with st.expander("🔍 分析フィルター", expanded=False):
        col1, col2, col3 = st.columns(3)

        # フィルター定義
        filter_defs = [
            {
                "name": "disease_class",
                "label": "疾患分類",
                "label_col": "disease_class_label",
            },
            {
                "name": "p_stage",
                "label": "pStage",
                "label_col": "p_stage_label",
            },
            {
                "name": "op_approach",
                "label": "到達法",
                "label_col": "op_approach_label",
            },
            {
                "name": "op_surgeon",
                "label": "執刀医",
                "label_col": "op_surgeon",
            },
        ]

        with col1:
            # 疾患分類
            dc = get_codebook("disease_class")
            if dc:
                dc_options = list(dc.values())
                selected_diseases = st.multiselect("疾患分類", dc_options, default=dc_options)
                if selected_diseases and "disease_class_label" in df.columns:
                    df = df[df["disease_class_label"].isin(selected_diseases)]

        with col2:
            # 年範囲
            if "surgery_year" in df.columns and df["surgery_year"].notna().any():
                year_min = int(df["surgery_year"].min())
                year_max = int(df["surgery_year"].max())
                if year_min < year_max:
                    yr_range = st.slider("手術年", year_min, year_max, (year_min, year_max))
                    df = df[(df["surgery_year"] >= yr_range[0]) & (df["surgery_year"] <= yr_range[1])]

        with col3:
            # pStage
            if "p_stage_label" in df.columns:
                stages = sorted(df["p_stage_label"].dropna().unique())
                if stages:
                    selected_stages = st.multiselect("pStage", stages, default=stages)
                    if selected_stages:
                        df = df[df["p_stage_label"].isin(selected_stages) | df["p_stage_label"].isna()]

        # 到達法フィルター
        col4, col5 = st.columns(2)
        with col4:
            if "op_approach_label" in df.columns:
                approaches = sorted(df["op_approach_label"].dropna().unique())
                if approaches:
                    selected_approaches = st.multiselect("到達法", approaches, default=approaches)
                    if selected_approaches:
                        df = df[df["op_approach_label"].isin(selected_approaches) | df["op_approach_label"].isna()]

        with col5:
            # 執刀医フィルター
            if "op_surgeon" in df.columns:
                surgeons = sorted([s for s in df["op_surgeon"].dropna().unique() if s])
                if surgeons:
                    selected_surgeons = st.multiselect("執刀医", surgeons, default=surgeons)
                    if selected_surgeons:
                        df = df[df["op_surgeon"].isin(selected_surgeons) | df["op_surgeon"].isna()]

    st.markdown(f"**対象症例: {len(df)} 件**")

    if df.empty:
        st.warning("フィルター条件に合致する症例がありません。")
        return

    # ============================
    # サマリー指標
    # ============================
    stats = compute_summary_stats(df)
    st.markdown("### 📈 手術成績サマリー")
    cols = st.columns(6)
    metric_items = [
        ("症例数", str(stats.get("n", 0)), None),
        ("手術時間 (中央値)", stats.get("op_time_median", "-") + " 分",
         f"IQR: {stats.get('op_time_iqr', '-')}"),
        ("出血量 (中央値)", stats.get("blood_loss_median", "-") + " mL",
         f"IQR: {stats.get('blood_loss_iqr', '-')}"),
        ("合併症率", stats.get("complication_rate", "-"), f"CD≥III: {stats.get('cd3_rate', '-')}"),
        ("縫合不全率", stats.get("leak_rate", "-"), f"膵液瘻: {stats.get('fistula_rate', '-')}"),
        ("30日死亡率", stats.get("mortality_30d", "-"), f"在院死亡: {stats.get('mortality_hosp', '-')}"),
    ]
    for i, (label, value, delta) in enumerate(metric_items):
        with cols[i]:
            st.metric(label, value, delta=delta, delta_color="off")

    # ============================
    # タブで切り替え
    # ============================
    tab_labels = ["🏥 患者背景", "📉 生存曲線", "🔪 手術成績",
                  "⚠️ 合併症", "👨‍⚕️ 術者別成績", "🧬 バイオマーカー",
                  "📊 統計解析", "🔍 データ探索", "🧬 クラスタリング"]
    tabs = st.tabs(tab_labels)

    # --- Tab 1: 患者背景 ---
    with tabs[0]:
        demo_figs = plot_demographics(df)
        if demo_figs:
            for i in range(0, len(demo_figs), 3):
                cols = st.columns(3)
                for j in range(3):
                    if i + j < len(demo_figs):
                        with cols[j]:
                            st.plotly_chart(demo_figs[i + j][1], use_container_width=True)
        else:
            st.info("表示できるデータがありません。")

    # --- Tab 2: 生存曲線 ---
    with tabs[1]:
        st.markdown("### Kaplan-Meier 生存曲線")

        col1, col2, col3 = st.columns(3)
        with col1:
            surv_type = st.radio("生存指標", ["OS (全生存)", "RFS (無再発生存)"],
                                 horizontal=True, key="surv_type")
        with col2:
            stratify_options = {
                "なし（全体）": None,
                "疾患分類": "disease_class_label",
                "pStage": "p_stage_label",
                "到達法": "op_approach_label",
                "術式": "op_procedure_label",
                "執刀医": "op_surgeon",
                "cStage": "c_stage_label",
                "性別": "sex_label",
            }
            stratify_label = st.selectbox("層別化", list(stratify_options.keys()), key="km_strat")
            stratify_col = stratify_options[stratify_label]
        with col3:
            max_m = st.slider("最大表示月数", 12, 120, 60, 12, key="km_max")

        if surv_type.startswith("OS"):
            time_col, event_col = "os_months", "os_event"
            title = "全生存曲線 (OS)"
        else:
            time_col, event_col = "rfs_months", "rfs_event"
            title = "無再発生存曲線 (RFS)"

        if stratify_col:
            title += f" — {stratify_label}別"

        km_fig = plot_kaplan_meier(df, time_col, event_col,
                                   group_col=stratify_col, title=title,
                                   max_months=max_m)
        st.plotly_chart(km_fig, use_container_width=True)

        # 生存率テーブル
        st.markdown("#### 生存率推定値と統計")
        milestones = [12, 24, 36, 60]
        valid = df[time_col].notna() & df[event_col].notna() & (df[time_col] >= 0)
        plot_df = df[valid]
        if len(plot_df) > 0:
            # Overall survival table
            times, surv, _, _, at_risk = kaplan_meier_estimate(
                plot_df[time_col], plot_df[event_col])
            rows = []
            for m in milestones:
                idx = np.searchsorted(times, m, side="right") - 1
                if idx >= 0 and idx < len(surv):
                    rows.append({"時点": f"{m}ヶ月", "生存率": f"{surv[idx]:.1%}"})
            if rows:
                st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=False)

            # Stratified statistics
            if stratify_col and stratify_col in plot_df.columns:
                st.markdown("#### 層別グループ別 統計")
                groups = sorted(plot_df[stratify_col].dropna().unique())

                stat_rows = []
                groups_data_for_test = []

                for grp in groups:
                    mask = plot_df[stratify_col] == grp
                    sub = plot_df[mask]
                    if len(sub) < 2:
                        continue

                    n = len(sub)
                    events = int(sub[event_col].sum())

                    times_g, surv_g, _, _, _ = kaplan_meier_estimate(sub[time_col], sub[event_col])

                    # Median survival
                    median_idx = np.searchsorted(surv_g, 0.5, side="right") - 1
                    if median_idx >= 0 and median_idx < len(times_g):
                        median_surv = times_g[median_idx]
                    else:
                        median_surv = None

                    # Survival at specific timepoints
                    surv_1yr = None
                    surv_2yr = None
                    surv_3yr = None
                    surv_5yr = None

                    for i, m in enumerate([12, 24, 36, 60]):
                        idx = np.searchsorted(times_g, m, side="right") - 1
                        if idx >= 0 and idx < len(surv_g):
                            if m == 12:
                                surv_1yr = surv_g[idx]
                            elif m == 24:
                                surv_2yr = surv_g[idx]
                            elif m == 36:
                                surv_3yr = surv_g[idx]
                            elif m == 60:
                                surv_5yr = surv_g[idx]

                    stat_rows.append({
                        "グループ": grp,
                        "N": n,
                        "イベント": events,
                        "中央値生存(月)": f"{median_surv:.1f}" if median_surv else "-",
                        "1年生存率": f"{surv_1yr:.1%}" if surv_1yr else "-",
                        "2年生存率": f"{surv_2yr:.1%}" if surv_2yr else "-",
                        "3年生存率": f"{surv_3yr:.1%}" if surv_3yr else "-",
                        "5年生存率": f"{surv_5yr:.1%}" if surv_5yr else "-",
                    })

                    groups_data_for_test.append((sub[time_col], sub[event_col]))

                if stat_rows:
                    st.dataframe(pd.DataFrame(stat_rows), hide_index=True, use_container_width=True)

                    # Log-rank test
                    if len(groups_data_for_test) > 1:
                        chi2, p_val = log_rank_test(groups_data_for_test)
                        if chi2 is not None and p_val is not None:
                            st.write(f"**Log-rank test**: χ² = {chi2:.3f}, p-value = {p_val:.4f}")

    # --- Tab 3: 手術成績 ---
    with tabs[2]:
        op_figs = plot_operative_outcomes(df)
        if op_figs:
            for i in range(0, len(op_figs), 3):
                cols = st.columns(3)
                for j in range(3):
                    if i + j < len(op_figs):
                        with cols[j]:
                            st.plotly_chart(op_figs[i + j][1], use_container_width=True)
        else:
            st.info("表示できるデータがありません。")

    # --- Tab 4: 合併症 ---
    with tabs[3]:
        comp_figs = plot_complication_rates(df)
        if comp_figs:
            for i in range(0, len(comp_figs), 3):
                cols = st.columns(3)
                for j in range(3):
                    if i + j < len(comp_figs):
                        with cols[j]:
                            st.plotly_chart(comp_figs[i + j][1], use_container_width=True)
        else:
            st.info("表示できるデータがありません。")

    # --- Tab 5: 術者別成績 ---
    with tabs[4]:
        st.markdown("### 👨‍⚕️ 術者（執刀医）別 手術成績")
        if "op_surgeon" in df.columns and df["op_surgeon"].notna().any() and (df["op_surgeon"] != "").any():
            surgeon_df = df[df["op_surgeon"].notna() & (df["op_surgeon"] != "")].copy()
            surgeons = sorted(surgeon_df["op_surgeon"].unique())

            # 執刀医選択
            selected_surgeons = st.multiselect(
                "表示する執刀医", surgeons, default=surgeons, key="an_surgeons")
            if selected_surgeons:
                surgeon_df = surgeon_df[surgeon_df["op_surgeon"].isin(selected_surgeons)]

            if not surgeon_df.empty:
                # 集計テーブル
                agg_rows = []
                for s in sorted(surgeon_df["op_surgeon"].unique()):
                    sdf = surgeon_df[surgeon_df["op_surgeon"] == s]
                    n = len(sdf)
                    ot = sdf["op_time_min"].dropna()
                    bl = sdf["op_blood_loss_ml"].dropna()
                    comp = sdf["op_complication_yn"].dropna()
                    leak = sdf["comp_anastomotic_leak"].dropna()
                    agg_rows.append({
                        "執刀医": s,
                        "症例数": n,
                        "手術時間 中央値(分)": f"{ot.median():.0f}" if len(ot) > 0 else "-",
                        "手術時間 IQR": f"{ot.quantile(0.25):.0f}-{ot.quantile(0.75):.0f}" if len(ot) > 0 else "-",
                        "出血量 中央値(mL)": f"{bl.median():.0f}" if len(bl) > 0 else "-",
                        "出血量 IQR": f"{bl.quantile(0.25):.0f}-{bl.quantile(0.75):.0f}" if len(bl) > 0 else "-",
                        "合併症率": f"{comp.sum()/len(comp)*100:.1f}%" if len(comp) > 0 else "-",
                        "縫合不全率": f"{(leak>0).sum()/len(leak)*100:.1f}%" if len(leak) > 0 else "-",
                    })
                st.dataframe(pd.DataFrame(agg_rows), hide_index=True, use_container_width=True)

                st.markdown("---")

                # Violin plots: 手術時間・出血量
                col1, col2, col3 = st.columns(3)
                with col1:
                    if surgeon_df["op_time_min"].notna().any():
                        fig_time = px.violin(
                            surgeon_df, x="op_surgeon", y="op_time_min",
                            title="執刀医別 手術時間",
                            labels={"op_surgeon": "執刀医", "op_time_min": "手術時間 (分)"},
                            color="op_surgeon", points="all", box=True
                        )
                        fig_time.update_layout(**PLOTLY_LAYOUT, showlegend=False)
                        st.plotly_chart(fig_time, use_container_width=True)

                with col2:
                    if surgeon_df["op_blood_loss_ml"].notna().any():
                        fig_blood = px.violin(
                            surgeon_df, x="op_surgeon", y="op_blood_loss_ml",
                            title="執刀医別 出血量",
                            labels={"op_surgeon": "執刀医", "op_blood_loss_ml": "出血量 (mL)"},
                            color="op_surgeon", points="all", box=True
                        )
                        fig_blood.update_layout(**PLOTLY_LAYOUT, showlegend=False)
                        st.plotly_chart(fig_blood, use_container_width=True)

                with col3:
                    if surgeon_df["op_icu_days"].notna().any():
                        fig_icu = px.violin(
                            surgeon_df, x="op_surgeon", y="op_icu_days",
                            title="執刀医別 ICU日数",
                            labels={"op_surgeon": "執刀医", "op_icu_days": "ICU日数"},
                            color="op_surgeon", points="all", box=True
                        )
                        fig_icu.update_layout(**PLOTLY_LAYOUT, showlegend=False)
                        st.plotly_chart(fig_icu, use_container_width=True)

                # 合併症率比較
                comp_cols = [c for c in surgeon_df.columns if c.startswith("comp_") and
                             surgeon_df[c].dtype in [np.int64, np.float64, int, float] and
                             c not in ["comp_confirmed"]]
                if comp_cols:
                    comp_labels = {
                        "comp_ssi": "SSI", "comp_anastomotic_leak": "縫合不全",
                        "comp_pancreatic_fistula": "膵液瘻", "comp_pneumonia": "肺炎",
                        "comp_ileus": "イレウス", "comp_bleeding": "出血",
                        "comp_dge": "DGE", "comp_dvt_pe": "DVT/PE",
                        "comp_wound_dehiscence": "創離開", "comp_delirium": "せん妄",
                    }
                    top_comps = [c for c in comp_labels if c in comp_cols][:8]
                    if top_comps:
                        comp_data = []
                        for s in sorted(surgeon_df["op_surgeon"].unique()):
                            sdf = surgeon_df[surgeon_df["op_surgeon"] == s]
                            for c in top_comps:
                                vals = sdf[c].dropna()
                                rate = (vals > 0).sum() / len(vals) * 100 if len(vals) > 0 else 0
                                comp_data.append({"執刀医": s, "合併症": comp_labels.get(c, c), "発生率(%)": rate})
                        fig_comp = px.bar(
                            pd.DataFrame(comp_data), x="合併症", y="発生率(%)",
                            color="執刀医", barmode="group",
                            title="執刀医別 主要合併症 発生率",
                            template="plotly_white"
                        )
                        st.plotly_chart(fig_comp, use_container_width=True)
            else:
                st.info("選択された執刀医のデータがありません。")
        else:
            st.info("執刀医データが登録されていません。手術タブの「術者」セクションで執刀医を入力してください。")

    # --- Tab 6: バイオマーカー ---
    with tabs[5]:
        bio_figs = plot_biomarkers(df)
        if bio_figs:
            for i in range(0, len(bio_figs), 3):
                cols = st.columns(3)
                for j in range(3):
                    if i + j < len(bio_figs):
                        with cols[j]:
                            st.plotly_chart(bio_figs[i + j][1], use_container_width=True)
        else:
            st.info("バイオマーカーデータがありません。")

    # --- Tab 7: 統計解析 ---
    with tabs[6]:
        render_statistical_analysis(df)

    # --- Tab 8: データ探索 ---
    with tabs[7]:
        render_data_exploration(df)

    # --- Tab 9: クラスタリング ---
    with tabs[8]:
        st.markdown("### 🧬 クラスタリング分析")
        st.write("患者を主要な特性に基づいてクラスタリングし、潜在的なサブグループを識別します。")

        # 解析用カラム選択
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        numeric_cols = [c for c in numeric_cols if not c.endswith("_id") and not c.endswith("_code")
                       and c not in ["id", "patient_id", "study_id"]]

        if numeric_cols:
            # 日本語ラベル付き選択
            def _jp_num(c):
                lbl = COL_JP_LABELS.get(c, "")
                return f"{lbl} ({c})" if lbl else c

            selected_features = st.multiselect(
                "クラスタリングに使用する特性（最低2つ選択）",
                numeric_cols,
                default=numeric_cols[:min(5, len(numeric_cols))],
                format_func=_jp_num,
                key="cluster_features"
            )

            if len(selected_features) >= 2:
                # 次元削減手法 + クラスター数
                ctrl1, ctrl2 = st.columns(2)
                with ctrl1:
                    dim_method = st.selectbox(
                        "次元削減手法",
                        ["PCA", "t-SNE", "UMAP"],
                        index=0,
                        key="cluster_dim_method"
                    )
                with ctrl2:
                    n_clusters = st.slider("クラスター数 (K)", 2, 10, 3)

                # データ準備
                X = df[selected_features].dropna()
                if len(X) > 0:
                    # 正規化
                    from sklearn.preprocessing import StandardScaler
                    scaler = StandardScaler()
                    X_scaled = scaler.fit_transform(X)

                    # KMeans
                    from sklearn.cluster import KMeans
                    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
                    clusters = kmeans.fit_predict(X_scaled)

                    # 次元削減
                    axis_labels = {}
                    if dim_method == "PCA":
                        from sklearn.decomposition import PCA
                        reducer = PCA(n_components=2)
                        X_2d = reducer.fit_transform(X_scaled)
                        axis_labels = {
                            "x": f"PC1 ({reducer.explained_variance_ratio_[0]:.1%})",
                            "y": f"PC2 ({reducer.explained_variance_ratio_[1]:.1%})",
                        }
                        title_prefix = "PCA"
                    elif dim_method == "t-SNE":
                        from sklearn.manifold import TSNE
                        reducer = TSNE(n_components=2, random_state=42, perplexity=min(30, len(X_scaled) - 1))
                        X_2d = reducer.fit_transform(X_scaled)
                        axis_labels = {"x": "t-SNE 1", "y": "t-SNE 2"}
                        title_prefix = "t-SNE"
                    else:  # UMAP
                        try:
                            import umap
                            reducer = umap.UMAP(n_components=2, random_state=42)
                            X_2d = reducer.fit_transform(X_scaled)
                            axis_labels = {"x": "UMAP 1", "y": "UMAP 2"}
                            title_prefix = "UMAP"
                        except ImportError:
                            st.warning("UMAPを使用するには `pip install umap-learn` が必要です。PCAにフォールバックします。")
                            from sklearn.decomposition import PCA
                            reducer = PCA(n_components=2)
                            X_2d = reducer.fit_transform(X_scaled)
                            axis_labels = {
                                "x": f"PC1 ({reducer.explained_variance_ratio_[0]:.1%})",
                                "y": f"PC2 ({reducer.explained_variance_ratio_[1]:.1%})",
                            }
                            title_prefix = "PCA (UMAP未インストール)"

                    # Plot
                    cluster_df = pd.DataFrame({
                        "Dim1": X_2d[:, 0],
                        "Dim2": X_2d[:, 1],
                        "クラスター": clusters.astype(str)
                    })

                    fig_clusters = px.scatter(
                        cluster_df, x="Dim1", y="Dim2", color="クラスター",
                        title=f"{title_prefix} による {n_clusters} クラスター",
                        labels={"Dim1": axis_labels.get("x", "Dim 1"),
                                "Dim2": axis_labels.get("y", "Dim 2")},
                        color_discrete_sequence=COLORS
                    )
                    fig_clusters.update_layout(**PLOTLY_LAYOUT)
                    st.plotly_chart(fig_clusters, use_container_width=True)

                    # クラスター特性表
                    st.markdown("#### クラスター特性")
                    cluster_stats = []
                    for c in sorted(np.unique(clusters)):
                        mask = clusters == c
                        cluster_stats.append({
                            "クラスター": c,
                            "症例数": mask.sum(),
                        })
                    st.dataframe(pd.DataFrame(cluster_stats), hide_index=True, use_container_width=False)
                else:
                    st.warning("選択した特性に欠損が多すぎます。")
            else:
                st.info("クラスタリングには最低2つ以上の特性を選択してください。")
        else:
            st.warning("数値型カラムが見つかりません。")


# ============================================================
# データ探索（独立関数 — 独立ページ / タブ内 両対応）
# ============================================================
def render_data_exploration(df):
    """インタラクティブ・データ探索。"""
    st.markdown("### 🔍 インタラクティブ・データ探索")
    st.write("見たい項目を自由に組み合わせて、データの分布や関係性を視覚化できます。")

    # グラフに使えるカラムを抽出（内部IDなどを除外）
    exclude_cols = ["id", "patient_id", "study_id", "birthdate", "surgery_date", "data_status",
                   "_time", "_end_dt", "_dt", "_days"]
    available_cols = sorted([c for c in df.columns if c not in exclude_cols and not c.endswith("_dt")])

    def _jp_label(c):
        lbl = COL_JP_LABELS.get(c, "")
        return f"{lbl} ({c})" if lbl else c

    # X/Y軸入替ボタン用のセッションステート初期化
    if "explore_x_val" not in st.session_state:
        st.session_state["explore_x_val"] = available_cols.index("disease_class") if "disease_class" in available_cols else 0
    if "explore_y_val" not in st.session_state:
        st.session_state["explore_y_val"] = 0

    # 4列のレイアウト: X軸 | 入替ボタン | Y軸 | 色分け
    col1, col_swap, col2, col3 = st.columns([3, 0.8, 3, 3])
    with col1:
        x_idx = st.selectbox(
            "X軸の項目 (必須)",
            range(len(available_cols)),
            index=st.session_state["explore_x_val"],
            format_func=lambda i: _jp_label(available_cols[i]),
            key="explore_x"
        )
        x_col = available_cols[x_idx]
        st.session_state["explore_x_val"] = x_idx
    with col_swap:
        st.write("")
        st.write("")
        if st.button("⇄ 入替", key="swap_xy", help="X軸とY軸を入れ替えます"):
            old_x = st.session_state.get("explore_x_val", 0)
            old_y = st.session_state.get("explore_y_val", 0)
            if old_y > 0:
                new_x = old_y - 1
                new_y = old_x + 1
                st.session_state["explore_x_val"] = new_x
                st.session_state["explore_y_val"] = new_y
                st.rerun()
    with col2:
        y_options = ["(選択なし)"] + available_cols
        y_idx = st.selectbox(
            "Y軸の項目 (任意)",
            range(len(y_options)),
            index=st.session_state["explore_y_val"],
            format_func=lambda i: y_options[i] if i == 0 else _jp_label(y_options[i]),
            key="explore_y"
        )
        y_col = y_options[y_idx]
        st.session_state["explore_y_val"] = y_idx
    with col3:
        c_options = ["(選択なし)"] + available_cols
        c_idx = st.selectbox(
            "色分け / グループ化",
            range(len(c_options)),
            index=0,
            format_func=lambda i: c_options[i] if i == 0 else _jp_label(c_options[i]),
            key="explore_color"
        )
        color_col = c_options[c_idx]

    plot_df = df.copy()
    c_color = color_col if color_col != "(選択なし)" else None
    c_y = y_col if y_col != "(選択なし)" else None

    if x_col:
        try:
            if c_y:
                if pd.api.types.is_numeric_dtype(plot_df[c_y]):
                    if pd.api.types.is_numeric_dtype(plot_df[x_col]):
                        fig = px.scatter(plot_df, x=x_col, y=c_y, color=c_color,
                                       title=f"{COL_JP_LABELS.get(x_col, x_col)} vs {COL_JP_LABELS.get(c_y, c_y)}",
                                       labels={x_col: COL_JP_LABELS.get(x_col, x_col),
                                               c_y: COL_JP_LABELS.get(c_y, c_y)})
                    else:
                        fig = px.violin(plot_df, x=x_col, y=c_y, color=c_color,
                                      title=f"{COL_JP_LABELS.get(x_col, x_col)} vs {COL_JP_LABELS.get(c_y, c_y)}",
                                      points="all", box=True,
                                      labels={x_col: COL_JP_LABELS.get(x_col, x_col),
                                              c_y: COL_JP_LABELS.get(c_y, c_y)})
                else:
                    if pd.api.types.is_numeric_dtype(plot_df[x_col]):
                        fig = px.box(plot_df, x=x_col, y=c_y, color=c_color,
                                    title=f"{COL_JP_LABELS.get(x_col, x_col)} vs {COL_JP_LABELS.get(c_y, c_y)}",
                                    labels={x_col: COL_JP_LABELS.get(x_col, x_col),
                                            c_y: COL_JP_LABELS.get(c_y, c_y)})
                    else:
                        cross_tab = pd.crosstab(plot_df[x_col], plot_df[c_y])
                        fig = px.imshow(cross_tab, title=f"{COL_JP_LABELS.get(x_col, x_col)} vs {COL_JP_LABELS.get(c_y, c_y)}",
                                      labels=dict(x=COL_JP_LABELS.get(x_col, x_col),
                                                  y=COL_JP_LABELS.get(c_y, c_y)))
            else:
                if pd.api.types.is_numeric_dtype(plot_df[x_col]):
                    fig = px.histogram(plot_df, x=x_col, color=c_color, nbins=30,
                                     title=f"{COL_JP_LABELS.get(x_col, x_col)} 分布",
                                     labels={x_col: COL_JP_LABELS.get(x_col, x_col)})
                else:
                    counts = plot_df[x_col].value_counts().head(20)
                    fig = px.bar(x=counts.index, y=counts.values,
                                title=f"{COL_JP_LABELS.get(x_col, x_col)} 分布",
                                labels={"x": COL_JP_LABELS.get(x_col, x_col), "y": "症例数"})

            fig.update_layout(**PLOTLY_LAYOUT)
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"図の生成に失敗しました: {e}")


# ============================================================
# エントリーポイント
# ============================================================
if __name__ == "__main__":
    render_analytics_dashboard()
