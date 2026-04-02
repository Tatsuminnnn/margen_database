"""
上部消化管グループ 統合症例登録DB — codebook.py
コードブック（選択肢マスタ）初期データ投入スクリプト

使用方法:
    python codebook.py          # 初期データを投入
    python codebook.py --reset  # 既存データを削除して再投入
"""

import sys
import os

# database.py と同じディレクトリにあることを想定
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from database import get_db, init_db

# ============================================================
# コードブック定義
# ============================================================
# 形式: { field_name: [(code, label, label_en, ncd_mapping, registry_mapping), ...] }
# version_id=None → 規約バージョン非依存
# version_id=1 → 胃癌取扱い規約15版
# version_id=2 → UICC-TNM 8版
# version_id=3 → 食道癌取扱い規約12版

# ------------------------------------------------------------------
# 共通（バージョン非依存）
# ------------------------------------------------------------------
COMMON_ENTRIES = {
    "sex": [
        (1, "男性", "Male", "男性", None),
        (2, "女性", "Female", "女性", None),
    ],
    "smoking": [
        (0, "なし", "Never", "喫煙歴なし", None),
        (1, "過去あり", "Former", "過去に喫煙", None),
        (2, "現在喫煙中", "Current", "現在喫煙", None),
    ],
    "alcohol": [
        (0, "なし", "None", None, None),
        (1, "機会飲酒", "Social", None, None),
        (2, "常習飲酒", "Heavy", None, None),
    ],
    "ps": [
        (0, "PS 0: 無症状", "PS 0: Fully active", "PS0", None),
        (1, "PS 1: 軽度制限", "PS 1: Restricted in strenuous activity", "PS1", None),
        (2, "PS 2: 日中50%以上歩行可", "PS 2: Ambulatory >50%", "PS2", None),
        (3, "PS 3: 日中50%以上臥床", "PS 3: Confined to bed >50%", "PS3", None),
        (4, "PS 4: 完全臥床", "PS 4: Completely disabled", "PS4", None),
    ],
    "asa": [
        (1, "ASA I: 健康", "ASA I", "I", None),
        (2, "ASA II: 軽度全身疾患", "ASA II", "II", None),
        (3, "ASA III: 重度全身疾患", "ASA III", "III", None),
        (4, "ASA IV: 生命を脅かす", "ASA IV", "IV", None),
        (5, "ASA V: 瀕死", "ASA V", "V", None),
        (6, "ASA VI: 脳死ドナー", "ASA VI", "VI", None),
    ],
    "hp_eradication": [
        (0, "なし", "None", None, None),
        (1, "除菌成功", "Eradicated", None, None),
        (2, "除菌不成功", "Failed", None, None),
        (3, "未施行", "Not tested", None, None),
    ],
    "adl_status": [
        (0, "自立", "Independent", "自立", None),
        (1, "一部介助", "Partially dependent", "一部介助", None),
        (2, "全介助", "Totally dependent", "全介助", None),
    ],
    "discharge_destination": [
        (1, "自宅退院", "Home", "自宅退院", None),
        (2, "転院(リハビリ)", "Transfer (Rehab)", "転院(リハビリ)", None),
        (3, "転院(治療継続)", "Transfer (Continued treatment)", "転院(治療継続)", None),
        (4, "施設入所", "Nursing facility", "施設入所", None),
        (5, "死亡退院", "Death", "死亡退院", None),
        (9, "その他", "Other", "その他", None),
    ],

    # ------------------------------------------------------------------
    # 疾患分類
    # ------------------------------------------------------------------
    "disease_class": [
        (1, "初発胃癌", "Primary gastric cancer", "P", "胃癌"),
        (2, "初発胃癌（ESD後）", "Primary gastric cancer (post-ESD)", "E", "胃癌"),
        (3, "残胃の癌", "Remnant stomach cancer", "R", "胃癌"),
        (4, "GIST", "GIST", "G", "GIST"),
        (5, "悪性リンパ腫（B cell）", "Malignant lymphoma (B cell)", "LB", None),
        (6, "悪性リンパ腫（T cell）", "Malignant lymphoma (T cell)", "LT", None),
        (7, "悪性リンパ腫（その他）", "Malignant lymphoma (Other)", "LO", None),
        (8, "その他（神経原性腫瘍）", "Other (Neurogenic tumor)", "ON", None),
        (9, "その他（平滑筋腫瘍）", "Other (Leiomyoma)", "OL", None),
        (99, "不明", "Unknown", "XX", None),
    ],

    # ------------------------------------------------------------------
    # 胃 占居部位（長軸） ― 胃癌取扱い規約15版 準拠
    # 複数領域にまたがる場合は multiselect で選択 → 結合して保存
    # ------------------------------------------------------------------
    "location_long": [
        (1, "U (上部)", "U (Upper)", "U", None),
        (2, "M (中部)", "M (Middle)", "M", None),
        (3, "L (下部)", "L (Lower)", "L", None),
        (4, "T (胃全体)", "T (Whole stomach)", "T", None),
        (5, "E (食道)", "E (Esophagus)", "E", None),
        (6, "D (十二指腸)", "D (Duodenum)", "D", None),
        (99, "不明", "Unknown", "XX", None),
    ],

    # ------------------------------------------------------------------
    # 胃 占居部位（短軸） ― 胃癌取扱い規約15版 準拠
    # ------------------------------------------------------------------
    "location_short": [
        (1, "小弯 (Less)", "Less (Lesser curvature)", "L", None),
        (2, "大弯 (Gre)", "Gre (Greater curvature)", "G", None),
        (3, "前壁 (Ant)", "Ant (Anterior wall)", "A", None),
        (4, "後壁 (Post)", "Post (Posterior wall)", "P", None),
        (5, "全周 (Circ)", "Circ (Circumferential)", "C", None),
        (99, "不明", "Unknown", "XX", None),
    ],

    # ------------------------------------------------------------------
    # 肉眼型 (共通)
    # ------------------------------------------------------------------
    "macroscopic_type": [
        (0, "Type 0 (表在型)", "Type 0 (Superficial)", None, None),
        (1, "Type 1 (隆起型)", "Type 1 (Polypoid)", "B1", None),
        (2, "Type 2 (潰瘍限局型)", "Type 2 (Ulcerated, circumscribed)", "B2", None),
        (3, "Type 3 (潰瘍浸潤型)", "Type 3 (Ulcerated, infiltrative)", "B3", None),
        (4, "Type 4 (びまん浸潤型)", "Type 4 (Diffuse)", "B4", None),
        (5, "Type 5 (分類不能)", "Type 5 (Unclassifiable)", "B5", None),
        (99, "不明", "Unknown", "XX", None),
    ],
    "type0_subclass": [
        (1, "0-I (隆起型)", "0-I (Protruded)", "1E", None),
        (2, "0-IIa (表面隆起型)", "0-IIa (Slightly elevated)", "2A", None),
        (3, "0-IIb (表面平坦型)", "0-IIb (Flat)", "2B", None),
        (4, "0-IIc (表面陥凹型)", "0-IIc (Slightly depressed)", "2C", None),
        (5, "0-III (陥凹型)", "0-III (Excavated)", "3E", None),
    ],

    # ------------------------------------------------------------------
    # 手術 — 到達法 / 術式 / 再建 / 郭清
    # ------------------------------------------------------------------
    "op_approach": [
        (1, "開腹のみ", "Open only", "A", "開腹"),
        (2, "開胸開腹（連続）", "Thoraco-abdominal (continuous)", "C", "開胸開腹連続"),
        (3, "開胸開腹（非連続）", "Thoraco-abdominal (non-continuous)", "N", "開胸開腹非連続"),
        (4, "横隔膜切開非開胸（開腹横隔膜切開）", "Diaphragm incision", "D", "横隔膜切開"),
        (5, "胸骨縦切開", "Median sternotomy", "S", "胸骨縦切開"),
        (6, "腹腔鏡・腹腔鏡補助", "Laparoscopic/LAP-assisted", "L", "腹腔鏡"),
        (7, "ロボット支援", "Robotic", "R", "ロボット支援"),
        (91, "その他", "Other", "O", "その他"),
        (90, "非手術", "Non-surgical", "Z", "非手術"),
        (99, "不明", "Unknown", "XX", "不明"),
    ],
    "op_completion": [
        (1, "予定通り完遂", "Completed as planned", None, None),
        (2, "コンバージョン", "Conversion", None, None),
        (3, "中止", "Aborted", None, None),
    ],
    # CRF追加項目: 使用機器（ロボットシステム）
    "robot_system_type": [
        (1, "da Vinci S", "da Vinci S", None, None),
        (2, "da Vinci Si", "da Vinci Si", None, None),
        (3, "da Vinci Xi", "da Vinci Xi", None, None),
        (4, "da Vinci X", "da Vinci X", None, None),
        (5, "da Vinci SP", "da Vinci SP", None, None),
        (6, "da Vinci 5", "da Vinci 5", None, None),
        (7, "hinotori", "hinotori", None, None),
        (8, "Hugo RAS", "Hugo RAS", None, None),
        (9, "Senhance", "Senhance", None, None),
        (10, "Saroa", "Saroa", None, None),
        (11, "ANSUR", "ANSUR", None, None),
        (99, "その他", "Other", None, None),
    ],
    # CRF追加項目: 腹腔鏡詳細分類
    "lap_detail_type": [
        (1, "完全腹腔鏡下", "Totally laparoscopic", None, None),
        (2, "用手補助(HALS)", "Hand-assisted", None, None),
        (3, "腹腔鏡補助下", "Laparoscopy-assisted", None, None),
        (4, "開腹移行", "Conversion to open", None, None),
    ],
    # CRF追加項目: ロボット手術詳細分類
    "robot_detail_type": [
        (1, "完全ロボット支援下", "Totally robotic", None, None),
        (2, "腹腔鏡併用", "Combined with laparoscopy", None, None),
        (3, "小開腹併用", "Combined with mini-laparotomy", None, None),
        (9, "その他", "Other", None, None),
    ],
    # CRF追加: 食道手術 — 胸腔操作体位
    "thoracic_position": [
        (1, "左側臥位", "Left lateral decubitus", None, None),
        (2, "右側臥位", "Right lateral decubitus", None, None),
        (3, "腹臥位", "Prone", None, None),
        (4, "仰臥位", "Supine", None, None),
    ],
    # CRF追加: 食道手術 — 胸腔鏡詳細
    "thoracoscope_detail_type": [
        (1, "右胸腔鏡(完全)", "Right thoracoscopic (total)", None, None),
        (2, "左胸腔鏡(完全)", "Left thoracoscopic (total)", None, None),
        (3, "右胸腔鏡(小開胸併設)", "Right thoracoscopic (mini-thoracotomy)", None, None),
        (4, "左胸腔鏡(小開胸併設)", "Left thoracoscopic (mini-thoracotomy)", None, None),
        (9, "その他", "Other", None, None),
    ],
    # CRF追加: 接合部癌 — EGJ腫瘍中心位置
    "egj_center_position": [
        (1, "OE", "OE (esophageal predominant)", None, None),
        (2, "OEG", "OEG", None, None),
        (3, "OE=G", "OE=G (equal)", None, None),
        (4, "OGE", "OGE", None, None),
        (5, "OG", "OG (gastric predominant)", None, None),
    ],
    # CRF追加: 術後転帰 — 30日/90日
    "outcome_30_90d": [
        (1, "生存", "Alive", None, None),
        (2, "原病死", "Death from primary disease", None, None),
        (3, "他病死", "Death from other disease", None, None),
        (4, "他癌死", "Death from other cancer", None, None),
    ],
    # CRF追加: 死因詳細
    "death_cause_detail": [
        (1, "原病死", "Primary disease", None, None),
        (2, "他病死", "Other disease", None, None),
        (3, "他癌死", "Other cancer", None, None),
    ],
    "op_anesthesia_type": [
        (1, "全身麻酔", "General", "全身麻酔", None),
        (2, "全身麻酔＋硬膜外", "General + Epidural", "全身麻酔＋硬膜外", None),
        (3, "全身麻酔＋IVPCA", "General + IVPCA", "全身麻酔＋IVPCA", None),
        (4, "脊椎麻酔", "Spinal", "脊椎麻酔", None),
        (5, "局所麻酔", "Local", "局所麻酔", None),
        (6, "腰椎麻酔", "Lumbar", "腰椎麻酔", None),
        (9, "その他", "Other", "その他", None),
    ],
    "op_emergency": [
        (0, "予定手術", "Elective", "予定手術", None),
        (1, "緊急手術", "Emergency", "緊急手術", None),
    ],

    # --- 胃癌術式 ---
    "op_procedure_gastric": [
        (1, "幽門側胃切除術", "Distal gastrectomy", "DG", "幽門側胃切除"),
        (2, "胃全摘術", "Total gastrectomy", "TG", "胃全摘"),
        (3, "噴門側胃切除術", "Proximal gastrectomy", "PG", "噴門側胃切除"),
        (4, "幽門保存胃切除術", "PPG", "PPG", "幽門保存胃切除"),
        (5, "胃局所切除術", "Local resection", "LR", "胃局所切除"),
        (6, "胃分節切除術", "Segmental gastrectomy", "SG", "胃分節切除"),
        (7, "胃粘膜切除術・ポリペクトミー", "Mucosal resection/Polypectomy", "MR", "粘膜切除"),
        (8, "吻合術", "Anastomosis", "AN", "吻合術"),
        (9, "胃瘻・腸瘻造設術", "Gastrostomy/Jejunostomy", "ST", "胃瘻/腸瘻"),
        (10, "試験開腹（単開腹）術", "Exploratory laparotomy", "EL", "試験開腹"),
        (90, "その他の手術", "Other", "OT", "その他"),
        (99, "不明", "Unknown", "XX", "不明"),
    ],
    # --- 食道癌術式 ---
    "op_procedure_eso": [
        (11, "右開胸食道切除", "Right thoracotomy esophagectomy", None, "右開胸食道切除"),
        (12, "左開胸食道切除", "Left thoracotomy esophagectomy", None, "左開胸食道切除"),
        (13, "食道抜去術 (Blunt)", "Transhiatal esophagectomy", None, "食道抜去術"),
        (14, "非開胸食道切除 (Orringer)", "Non-thoracic esophagectomy", None, "非開胸食道切除"),
        (15, "鏡視下食道切除 (胸腔鏡)", "VATS esophagectomy", None, "鏡視下食道切除"),
        (16, "ロボット支援食道切除", "Robotic esophagectomy", None, "ロボット支援食道切除"),
        (17, "咽頭喉頭食道切除", "Pharyngolaryngo-esophagectomy", None, "咽頭喉頭食道切除"),
        (19, "その他", "Other", None, "その他"),
    ],

    "op_dissection_gastric": [
        (0, "D0", "D0", "0", "D0"),
        (1, "D1", "D1", "1", "D1"),
        (2, "D1+", "D1+", "1+", "D1+"),
        (3, "D2", "D2", "2", "D2"),
        (4, "D2+", "D2+", "2+", "D2+"),
        (99, "不明", "Unknown", "XX", "不明"),
    ],
    # NCD食道 Row74: 切除度D = DX/D0/D1/D2/D3
    "op_dissection_eso": [
        (0, "DX", "DX", "1", "DX"),
        (1, "D0", "D0", "2", "D0"),
        (2, "D1", "D1", "3", "D1"),
        (3, "D2", "D2", "4", "D2"),
        (4, "D3", "D3", "5", "D3"),
    ],

    # --- 再建法 ---
    "op_reconstruction_gastric": [
        (1, "Billroth I法", "Billroth I", "B1", "B-I"),
        (2, "Billroth II法", "Billroth II", "B2", "B-II"),
        (3, "Roux-en-Y法", "Roux-en-Y", "RY", "Roux-en-Y"),
        (4, "空腸間置法", "Jejunal interposition", "IP", "空腸間置"),
        (5, "食道残胃吻合", "Esophago-remnant gastric", "EG", "食道残胃吻合"),
        (6, "幽門保存", "Pylorus-preserving", "PP", "幽門保存"),
        (7, "double tract法", "Double tract", "DT", "ダブルトラクト"),
        (9, "その他の再建法", "Other", "OT", "その他"),
        (8, "非切除", "Non-resection", "Z", "非切除"),
        (99, "不明", "Unknown", "XX", "不明"),
    ],
    # NCD食道 Row80: 再建臓器 (1:全胃/2:胃管/8:残胃/3:有茎空腸/4:遊離空腸/5:有茎結腸/6:遊離結腸/7:皮膚管/8888:その他)
    "op_reconstruction_eso": [
        (1, "全胃（亜全胃）", "Whole stomach", "1", "全胃"),
        (2, "胃管", "Gastric conduit", "2", "胃管"),
        (3, "有茎空腸", "Pedicled jejunum", "3", "有茎空腸"),
        (4, "遊離空腸", "Free jejunum", "4", "遊離空腸"),
        (5, "有茎結腸", "Pedicled colon", "5", "有茎結腸"),
        (6, "遊離結腸", "Free colon", "6", "遊離結腸"),
        (7, "皮膚管", "Skin tube", "7", "皮膚管"),
        (8, "残胃", "Remnant stomach", "8", "残胃"),
        (99, "その他", "Other", "8888", "その他"),
    ],

    # --- 吻合法 ---
    "op_anastomosis_method": [
        (1, "三角吻合", "Triangulating", None, None),
        (2, "デルタ吻合", "Delta-shaped", None, None),
        (3, "T吻合 (Overlap+Side-to-side)", "T-anastomosis", None, None),
        (4, "DST", "DST", None, None),
        (5, "FEEA", "FEEA", None, None),
        (6, "Overlap", "Overlap", None, None),
        (7, "上川法", "Kamikawa method", None, None),
        (8, "mSOFY", "mSOFY", None, None),
        (9, "OrVil", "OrVil", None, None),
        (10, "手縫い吻合", "Hand-sewn", None, None),
        (11, "端端吻合", "End-to-end", None, None),
        (12, "端側吻合", "End-to-side", None, None),
        (99, "その他", "Other", None, None),
    ],

    # --- 蠕動方向 / 再建経路 ---
    "op_peristalsis_direction": [
        (1, "順蠕動", "Isoperistaltic", None, None),
        (2, "逆蠕動", "Antiperistaltic", None, None),
    ],
    # NCD食道 Row75: 再建経路 (0:なし/1:胸壁前/2:胸骨後/4:後縦隔/5:頸部/8888:その他/9999:不明)
    "op_reconstruction_route": [
        (0, "なし", "None", "0", "なし"),
        (1, "胸壁前（皮下）", "Antethoracic (subcutaneous)", "1", "胸壁前"),
        (2, "胸骨後", "Retrosternal", "2", "胸骨後"),
        (3, "後縦隔", "Posterior mediastinal", "4", "後縦隔"),
        (4, "頸部", "Cervical", "5", "頸部"),
        (9, "その他", "Other", "8888", "その他"),
        (99, "不明", "Unknown", "9999", "不明"),
    ],

    # NCD食道 Row77: 吻合部位 (0:なし/1:頸部/2:胸壁前/3:胸腔内高位/4:胸腔内低位/5:下縦隔内/8888:その他)
    "eso_anastomosis_site": [
        (0, "なし", "None", "0", "なし"),
        (1, "頸部", "Cervical", "1", "頸部"),
        (2, "胸壁前", "Antethoracic", "2", "胸壁前"),
        (3, "胸腔内（高位）", "Intrathoracic (high)", "3", "胸腔内高位"),
        (4, "胸腔内（低位）", "Intrathoracic (low)", "4", "胸腔内低位"),
        (5, "下縦隔内", "Lower mediastinal", "5", "下縦隔内"),
        (9, "その他", "Other", "8888", "その他"),
    ],

    # NCD食道 Row87: 病型分類 (13:0-Is/14:0-Ip/2:0-IIa/3:0-IIb/4:0-IIc/5:0-III/7:1型/8:2型/9:3型/10:4型/11:5a型/12:5b型/9999:不明)
    "eso_macroscopic_type": [
        (13, "0-Is型", "0-Is (Superficial, sessile)", "13", None),
        (14, "0-Ip型", "0-Ip (Superficial, pedunculated)", "14", None),
        (2, "0-IIa型", "0-IIa (Slightly elevated)", "2", None),
        (3, "0-IIb型", "0-IIb (Flat)", "3", None),
        (4, "0-IIc型", "0-IIc (Slightly depressed)", "4", None),
        (5, "0-III型", "0-III (Excavated)", "5", None),
        (7, "1型", "Type 1 (Polypoid)", "7", None),
        (8, "2型", "Type 2 (Ulcerated, circumscribed)", "8", None),
        (9, "3型", "Type 3 (Ulcerated, infiltrative)", "9", None),
        (10, "4型", "Type 4 (Diffuse)", "10", None),
        (11, "5a型", "Type 5a", "11", None),
        (12, "5b型", "Type 5b", "12", None),
        (99, "不明", "Unknown", "9999", None),
    ],

    # NCD食道 Row110: 組織学的治療効果判定
    "eso_treatment_effect": [
        (1, "Grade 0", "Grade 0", "1", None),
        (2, "Grade 1a", "Grade 1a", "2", None),
        (3, "Grade 1b", "Grade 1b", "3", None),
        (4, "Grade 2", "Grade 2", "4", None),
        (5, "Grade 3", "Grade 3", "5", None),
        (6, "術前治療なし", "No preoperative treatment", "6", None),
        (99, "不明", "Unknown", "9999", None),
    ],

    # ------------------------------------------------------------------
    # Clavien-Dindo
    # ------------------------------------------------------------------
    "op_cd_grade": [
        (1, "Grade I", "Grade I", None, None),
        (2, "Grade II", "Grade II", None, None),
        (3, "Grade IIIa", "Grade IIIa", None, None),
        (4, "Grade IIIb", "Grade IIIb", None, None),
        (5, "Grade IVa", "Grade IVa", None, None),
        (6, "Grade IVb", "Grade IVb", None, None),
        (7, "Grade V (死亡)", "Grade V (Death)", None, None),
    ],

    # ------------------------------------------------------------------
    # 組織型 (胃 + 食道共通)
    # ------------------------------------------------------------------
    # NCD胃癌テンプレート igan_sug_uplode_15 Row45 組織型分類 完全準拠 (20項目)
    "histology_gastric": [
        (1, "乳頭腺癌", "Papillary adenocarcinoma", "PAP", "PAP"),
        (2, "高分化型管状腺癌", "Tubular adeno, well diff.", "TUB1", "tub1"),
        (3, "中分化型管状腺癌", "Tubular adeno, mod. diff.", "TUB2", "tub2"),
        (4, "充実型低分化腺癌", "Poorly diff. adeno, solid", "POR1", "por1"),
        (5, "非充実型低分化腺癌", "Poorly diff. adeno, non-solid", "POR2", "por2"),
        (6, "印環細胞癌", "Signet-ring cell carcinoma", "SIG", "sig"),
        (7, "粘液癌", "Mucinous adenocarcinoma", "MUC", "muc"),
        (8, "カルチノイド腫瘍", "Carcinoid tumor", "CND", "CND"),
        (9, "内分泌細胞癌", "Endocrine carcinoma", "ECC", "ECC"),
        (10, "リンパ球浸潤癌", "Carcinoma with lymphoid stroma", "CLS", "CLS"),
        (11, "胎児消化管類似癌", "Adeno with enteroblastic diff.", "AED", "AED"),
        (12, "肝様腺癌", "Hepatoid adenocarcinoma", "HC", "HC"),
        (13, "胃底腺型腺癌", "Adeno of fundic gland type", "AFG", "AFG"),
        (14, "腺扁平上皮癌", "Adenosquamous carcinoma", "ASQ", "ASQ"),
        (15, "扁平上皮癌", "Squamous cell carcinoma", "SCC", "SCC"),
        (16, "未分化癌", "Undifferentiated carcinoma", "UC", "UC"),
        (17, "その他の癌", "Miscellaneous carcinoma", "MIS", "MIS"),
        (18, "その他", "Other", "OTH", "その他"),
        (99, "不明", "Unknown", "XX", "不明"),
    ],
    "histology_eso": [
        (1, "扁平上皮癌", "SCC", None, "SCC"),
        (5, "類基底細胞癌", "Basaloid carcinoma", None, "類基底細胞癌"),
        (6, "癌肉腫", "Carcinosarcoma", None, "癌肉腫"),
        (7, "腺癌", "Adenocarcinoma", None, "腺癌"),
        (9, "腺扁平上皮癌", "Adenosquamous", None, "腺扁平上皮癌"),
        (10, "粘表皮癌", "Mucoepidermoid", None, "粘表皮癌"),
        (11, "腺様嚢胞癌", "Adenoid cystic", None, "腺様嚢胞癌"),
        (12, "神経内分泌腫瘍", "Neuroendocrine tumor", None, "NET"),
        (13, "未分化癌", "Undifferentiated", None, "未分化癌"),
        (14, "その他の上皮性悪性腫瘍", "Other epithelial", None, None),
        (15, "非上皮性悪性腫瘍", "Non-epithelial", None, None),
        (16, "GIST", "GIST", None, "GIST"),
        (17, "悪性黒色腫", "Malignant melanoma", None, "悪性黒色腫"),
        (8888, "その他", "Other", None, "その他"),
        (9999, "不明", "Unknown", None, "不明"),
    ],

    # ------------------------------------------------------------------
    # RECIST
    # ------------------------------------------------------------------
    "recist_response": [
        (1, "CR", "CR", None, None),
        (2, "PR", "PR", None, None),
        (3, "SD", "SD", None, None),
        (4, "PD", "PD", None, None),
        (5, "NE", "NE", None, None),
    ],

    # ------------------------------------------------------------------
    # 化療完遂
    # ------------------------------------------------------------------
    "chemo_completion": [
        (1, "完遂", "Completed", None, None),
        (2, "減量完遂", "Completed with dose reduction", None, None),
        (3, "中止", "Discontinued", None, None),
    ],

    # ------------------------------------------------------------------
    # 残存腫瘍
    # ------------------------------------------------------------------
    "residual_tumor": [
        (0, "R0", "R0", None, "R0"),
        (1, "R1", "R1", None, "R1"),
        (2, "R2", "R2", None, "R2"),
        (8, "RX", "RX", None, "RX"),
        (99, "不明", "Unknown", None, None),
    ],

    # ------------------------------------------------------------------
    # 組織学的化療効果 (胃癌取扱い規約)
    # ------------------------------------------------------------------
    "chemo_effect_pathologic": [
        (0, "Grade 0 (無効)", "Grade 0", None, None),
        (1, "Grade 1a", "Grade 1a", None, None),
        (2, "Grade 1b", "Grade 1b", None, None),
        (3, "Grade 2", "Grade 2", None, None),
        (4, "Grade 3 (著効)", "Grade 3", None, None),
    ],

    # ------------------------------------------------------------------
    # 脈管侵襲 (ly, v, inf)
    # ------------------------------------------------------------------
    "lymphatic_invasion": [
        (0, "Ly0", "Ly0", "0", None),
        (1, "Ly1a", "Ly1a", "1a", None),
        (2, "Ly1b", "Ly1b", "1b", None),
        (3, "Ly1c", "Ly1c", "1c", None),
        (99, "不明", "Unknown", "XX", None),
    ],
    "venous_invasion": [
        (0, "V0", "V0", "0", None),
        (1, "V1a", "V1a", "1a", None),
        (2, "V1b", "V1b", "1b", None),
        (3, "V1c", "V1c", "1c", None),
        (99, "不明", "Unknown", "XX", None),
    ],
    "inf_pattern": [
        (1, "INFa", "INFa", None, None),
        (2, "INFb", "INFb", None, None),
        (3, "INFc", "INFc", None, None),
    ],

    # ------------------------------------------------------------------
    # 断端 (pm, dm)
    # ------------------------------------------------------------------
    "pm_status": [
        (0, "PM0", "PM0", None, None),
        (1, "PM1", "PM1", None, None),
        (9, "PMX", "PMX", None, None),
    ],
    "dm_status": [
        (0, "DM0", "DM0", None, None),
        (1, "DM1", "DM1", None, None),
        (9, "DMX", "DMX", None, None),
    ],

    # ------------------------------------------------------------------
    # 腹腔洗浄細胞診
    # ------------------------------------------------------------------
    "cytology": [
        (0, "CY0", "CY0", None, None),
        (1, "CY1", "CY1", None, None),
        (9, "CYX (未施行)", "CYX", None, None),
        (99, "不明", "Unknown", None, None),
    ],

    # ------------------------------------------------------------------
    # バイオマーカー
    # ------------------------------------------------------------------
    "msi_status": [
        (0, "MSS", "MSS", None, None),
        (1, "MSI-High", "MSI-H", None, None),
        (2, "MSI-Low", "MSI-L", None, None),
        (9, "未検", "Not tested", None, None),
    ],
    "her2_status": [
        (0, "陰性 (0, 1+)", "Negative", None, None),
        (1, "Equivocal (2+, FISH-)", "Equivocal (2+, FISH-)", None, None),
        (2, "陽性 (2+ FISH+, 3+)", "Positive", None, None),
        (9, "未検", "Not tested", None, None),
    ],
    "pdl1_status": [
        (0, "陰性", "Negative", None, None),
        (1, "陽性", "Positive", None, None),
        (9, "未検", "Not tested", None, None),
    ],
    "claudin18_status": [
        (0, "陰性", "Negative", None, None),
        (1, "陽性", "Positive", None, None),
        (9, "未検", "Not tested", None, None),
    ],
    "fgfr2b_status": [
        (0, "陰性", "Negative", None, None),
        (1, "陽性", "Positive", None, None),
        (9, "未検", "Not tested", None, None),
    ],
    "ebv_status": [
        (0, "陰性", "Negative", None, None),
        (1, "陽性", "Positive", None, None),
        (9, "未検", "Not tested", None, None),
    ],

    # ------------------------------------------------------------------
    # 転帰・予後
    # ------------------------------------------------------------------
    "vital_status": [
        (1, "生存", "Alive", None, None),
        (2, "原病死", "Cancer death", None, None),
        (3, "他病死", "Other cause death", None, None),
        (4, "手術関連死", "Surgery-related death", None, None),
        (5, "事故死", "Accidental death", None, None),
        (9, "不明", "Unknown", None, None),
    ],
    "recurrence_yn": [
        (0, "なし", "No", None, None),
        (1, "あり", "Yes", None, None),
    ],
    "mortality_30d": [
        (0, "なし", "No", "なし", None),
        (1, "あり", "Yes", "あり", None),
    ],
    "mortality_inhospital": [
        (0, "なし", "No", "なし", None),
        (1, "あり", "Yes", "あり", None),
    ],

    # ------------------------------------------------------------------
    # 重複癌 臓器
    # ------------------------------------------------------------------
    # cancer_organ: flag_group 形式に移行済み → app.py 内で定義
    # codebook は後方互換のため残すが UI では不使用
    "cancer_organ": [
        (1, "口腔または咽喉頭", "Oral/Pharynx/Larynx", None, None),
        (2, "食道", "Esophagus", None, None),
        (3, "胃", "Stomach", None, None),
        (4, "大腸", "Colorectum", None, None),
        (5, "肺", "Lung", None, None),
        (6, "肝胆道系", "Hepatobiliary", None, None),
        (7, "膵臓", "Pancreas", None, None),
        (8, "乳腺", "Breast", None, None),
        (9, "泌尿器(腎・尿管・膀胱・前立腺・尿道)", "Urological", None, None),
        (10, "婦人科臓器(子宮・卵巣・膣)", "Gynecological", None, None),
        (11, "甲状腺", "Thyroid", None, None),
        (12, "神経系臓器", "Nervous system", None, None),
        (13, "血液", "Hematologic", None, None),
        (14, "皮膚", "Skin", None, None),
        (99, "その他", "Other", None, None),
    ],

    # ------------------------------------------------------------------
    # 死因詳細
    # ------------------------------------------------------------------
    "death_cause": [
        (1, "原発巣増悪", "Primary tumor progression", None, None),
        (2, "腹膜播種", "Peritoneal dissemination", None, None),
        (3, "肝転移", "Liver metastasis", None, None),
        (4, "肺転移", "Lung metastasis", None, None),
        (5, "その他遠隔転移", "Other distant metastasis", None, None),
        (6, "手術関連死", "Surgery-related death", None, None),
        (7, "他病死 (心疾患)", "Cardiac disease", None, None),
        (8, "他病死 (脳血管)", "Cerebrovascular", None, None),
        (9, "他病死 (肺炎)", "Pneumonia", None, None),
        (10, "他病死 (他癌)", "Other cancer", None, None),
        (11, "事故死", "Accident", None, None),
        (99, "その他", "Other", None, None),
    ],

    # ------------------------------------------------------------------
    # データステータス
    # ------------------------------------------------------------------
    "data_status": [
        (1, "下書き", "Draft", None, None),
        (2, "提出済", "Submitted", None, None),
        (3, "確認済", "Verified", None, None),
        (4, "承認済", "Approved", None, None),
    ],

    # ------------------------------------------------------------------
    # EGJ分類 (Siewert)
    # ------------------------------------------------------------------
    "egj_siewert": [
        (1, "Siewert I", "Siewert I", None, None),
        (2, "Siewert II", "Siewert II", None, None),
        (3, "Siewert III", "Siewert III", None, None),
    ],

    # ------------------------------------------------------------------
    # 残胃の初回疾患
    # ------------------------------------------------------------------
    "remnant_initial_disease": [
        (1, "B_良性病変", "Benign", None, None),
        (2, "M_悪性腫瘍", "Malignant", None, None),
        (3, "X_不明", "Unknown", None, None),
    ],

    # ------------------------------------------------------------------
    # 残胃の癌 存在部位
    # ------------------------------------------------------------------
    "remnant_location": [
        (1, "A_断端吻合部", "Anastomotic site", None, None),
        (2, "S_断端縫合部", "Gastric stump", None, None),
        (3, "O_非断端部", "Other", None, None),
        (4, "T_残胃全体", "Remnant gastric body", None, None),
        (5, "E_食道", "Esophagus", None, None),
        (6, "D_十二指腸", "Duodenum", None, None),
        (7, "J_空腸", "Jejunum", None, None),
        (99, "その他", "Other", None, None),
    ],

    # ------------------------------------------------------------------
    # 食道癌占居部位
    # ------------------------------------------------------------------
    "eso_location": [
        (1, "Ce (頸部)", "Ce (Cervical)", "1", None),
        (2, "Ut (胸部上部)", "Ut (Upper thoracic)", "2", None),
        (3, "Mt (胸部中部)", "Mt (Middle thoracic)", "3", None),
        (4, "Lt (胸部下部)", "Lt (Lower thoracic)", "4", None),
        (5, "Ae (腹部)", "Ae (Abdominal)", None, None),
        (8, "Jz (接合部)", "Jz (EG junction)", "8", None),
        (99, "不明", "Unknown", "9999", None),
    ],

    # ------------------------------------------------------------------
    # GIST Fletcher分類
    # ------------------------------------------------------------------
    "gist_fletcher": [
        (1, "Very low risk", "Very low risk", None, None),
        (2, "Low risk", "Low risk", None, None),
        (3, "Intermediate risk", "Intermediate risk", None, None),
        (4, "High risk", "High risk", None, None),
    ],
    # GIST IHC
    "gist_ihc": [
        (0, "陰性", "Negative", None, None),
        (1, "陽性", "Positive", None, None),
        (9, "未検", "Not tested", None, None),
    ],

    # ------------------------------------------------------------------
    # 輸血
    # ------------------------------------------------------------------
    "transfusion_yn": [
        (0, "なし", "No", None, None),
        (1, "あり", "Yes", None, None),
    ],
    # ------------------------------------------------------------------
    # 腹膜転移 (P)
    # ------------------------------------------------------------------
    "peritoneal_status": [
        (0, "cP0", "cP0", None, None),
        (1, "cP1a", "cP1a", None, None),
        (2, "cP1b", "cP1b", None, None),
        (3, "cP1c", "cP1c", None, None),
        (4, "cP1x", "cP1x", None, None),
        (9, "cPX (不明)", "cPX", None, None),
    ],
    # ------------------------------------------------------------------
    # 肝転移 (H)
    # ------------------------------------------------------------------
    "liver_metastasis_status": [
        (0, "cH0", "cH0", None, None),
        (1, "cH1", "cH1", None, None),
        (9, "cHX (不明)", "cHX", None, None),
    ],
    # ------------------------------------------------------------------
    # 遠隔転移 cM/pM
    # ------------------------------------------------------------------
    "distant_metastasis": [
        (0, "cM0", "cM0", None, None),
        (1, "cM1", "cM1", None, None),
        (9, "cMX", "cMX", None, None),
    ],
    "yc_distant_metastasis": [
        (0, "ycM0", "ycM0", None, None),
        (1, "ycM1", "ycM1", None, None),
        (9, "ycMX", "ycMX", None, None),
    ],
    # ------------------------------------------------------------------
    # 病理的腹膜転移 (P) — 病理用
    # ------------------------------------------------------------------
    "p_peritoneal_status": [
        (0, "P0", "P0", None, None),
        (1, "P1a", "P1a", None, None),
        (2, "P1b", "P1b", None, None),
        (3, "P1c", "P1c", None, None),
        (4, "P1x", "P1x", None, None),
        (9, "PX (不明)", "PX", None, None),
    ],
    # ------------------------------------------------------------------
    # 病理的肝転移 (H) — 病理用
    # ------------------------------------------------------------------
    "p_liver_metastasis_status": [
        (0, "H0", "H0", None, None),
        (1, "H1", "H1", None, None),
        (9, "HX (不明)", "HX", None, None),
    ],
    # ------------------------------------------------------------------
    # 病理的遠隔転移 pM — 病理用
    # ------------------------------------------------------------------
    "p_distant_metastasis": [
        (0, "pM0", "pM0", None, None),
        (1, "pM1", "pM1", None, None),
        (9, "pMX", "pMX", None, None),
    ],

    # ------------------------------------------------------------------
    # NCD v4.0: 併存疾患 多値展開フィールド
    # ------------------------------------------------------------------
    "comor_diabetes": [
        (0, "なし", "None", "糖尿病なし", None),
        (1, "食事療法のみ", "Diet only", "食事療法のみ", None),
        (2, "経口薬", "Oral medication", "経口薬", None),
        (3, "インスリン", "Insulin", "インスリン", None),
        (4, "インスリン＋経口薬", "Insulin + Oral", "インスリン＋経口薬", None),
        (8, "詳細不明", "Details unknown", "詳細不明", None),
    ],
    "comor_hypertension": [
        (0, "なし", "None", "高血圧なし", None),
        (1, "未治療", "Untreated", "高血圧(未治療)", None),
        (2, "治療中", "On medication", "高血圧(治療中)", None),
        (8, "詳細不明", "Details unknown", "詳細不明", None),
    ],
    "comor_cirrhosis": [
        (0, "なし", "None", "肝硬変なし", None),
        (1, "Child-Pugh A", "Child-Pugh A", "Child-Pugh A", None),
        (2, "Child-Pugh B", "Child-Pugh B", "Child-Pugh B", None),
        (3, "Child-Pugh C", "Child-Pugh C", "Child-Pugh C", None),
    ],
    "comor_hepatitis_virus": [
        (0, "なし", "None", "肝炎ウイルスなし", None),
        (1, "HBV", "HBV", "HBV", None),
        (2, "HCV", "HCV", "HCV", None),
        (3, "HBV+HCV", "HBV+HCV", "HBV+HCV", None),
    ],
    "smoking_type": [
        (0, "紙巻たばこ", "Cigarette", "紙巻たばこ", None),
        (1, "加熱式たばこ", "Heated tobacco", "加熱式たばこ", None),
        (2, "両方", "Both", "両方", None),
    ],

    # ------------------------------------------------------------------
    # NCD v4.0: 合併症サブカラム ISGPS分類
    # ------------------------------------------------------------------
    "comp_anastomotic_leak_type": [
        (1, "Type A", "Type A", "Type A", None),
        (2, "Type B", "Type B", "Type B", None),
        (3, "Type C", "Type C", "Type C", None),
    ],
    "comp_pancreatic_fistula_isgpf": [
        (0, "なし", "None", "なし", None),
        (1, "BL (Biochemical Leak)", "BL", "BL", None),
        (2, "Grade A", "Grade A", "Grade A", None),
        (3, "Grade B", "Grade B", "Grade B", None),
        (4, "Grade C", "Grade C", "Grade C", None),
    ],
    "comp_dge_isgps": [
        (0, "なし", "None", "なし", None),
        (1, "Grade A", "Grade A", "Grade A", None),
        (2, "Grade B", "Grade B", "Grade B", None),
        (3, "Grade C", "Grade C", "Grade C", None),
    ],
}


# ------------------------------------------------------------------
# 胃癌取扱い規約 14版 (version_id=)   ←未設定
# ------------------------------------------------------------------
GASTRIC_V14_ENTRIES = {
    "c_depth_gastric": [
        (0, "cT0", "cT0", None, "cT0"),
        (1, "cT1a (M)", "cT1a (M)", None, "cT1a"),
        (2, "cT1b (SM)", "cT1b (SM)", None, "cT1b"),
        (3, "cT2 (MP)", "cT2 (MP)", None, "cT2"),
        (4, "cT3 (SS)", "cT3 (SS)", None, "cT3"),
        (5, "cT4a (SE)", "cT4a (SE)", None, "cT4a"),
        (6, "cT4b (SI)", "cT4b (SI)", None, "cT4b"),
        (9, "cTX", "cTX", None, "cTX"),
    ],
    "yc_depth_gastric": [
        (0, "ycT0", "ycT0", None, "ycT0"),
        (1, "ycT1a (M)", "ycT1a (M)", None, "ycT1a"),
        (2, "ycT1b (SM)", "ycT1b (SM)", None, "ycT1b"),
        (3, "ycT2 (MP)", "ycT2 (MP)", None, "ycT2"),
        (4, "ycT3 (SS)", "ycT3 (SS)", None, "ycT3"),
        (5, "ycT4a (SE)", "ycT4a (SE)", None, "ycT4a"),
        (6, "ycT4b (SI)", "ycT4b (SI)", None, "ycT4b"),
        (9, "ycTX", "ycTX", None, "ycTX"),
    ],
    "c_ln_gastric": [
        (0, "cN0", "cN0", None, "cN0"),
        (1, "cN1", "cN1", None, "cN1"),
        (2, "cN2", "cN2", None, "cN2"),
        (3, "cN3a", "cN3a", None, "cN3a"),
        (4, "cN3b", "cN3b", None, "cN3b"),
        (9, "cNX", "cNX", None, "cNX"),
    ],
    "yc_ln_gastric": [
        (0, "ycN0", "ycN0", None, "ycN0"),
        (1, "ycN1", "ycN1", None, "ycN1"),
        (2, "ycN2", "ycN2", None, "ycN2"),
        (3, "ycN3a", "ycN3a", None, "ycN3a"),
        (4, "ycN3b", "ycN3b", None, "ycN3b"),
        (9, "ycNX", "ycNX", None, "ycNX"),
    ],
    "c_stage_gastric": [
        (0, "0", "0", None, None),
        (1, "cIA", "cIA", None, "cIA"),
        (2, "cIB", "cIB", None, "cIB"),
        (3, "cIIA", "cIIA", None, "cIIA"),
        (4, "cIIB", "cIIB", None, "cIIB"),
        (5, "cIII", "cIII", None, "cIII"),
        (8, "cIV", "cIV", None, "cIV"),
    ],
    "p_depth_gastric": [
        (0, "pT0", "pT0", None, "pT0"),
        (1, "pT1a (M)", "pT1a (M)", None, "pT1a"),
        (2, "pT1b (SM)", "pT1b (SM)", None, "pT1b"),
        (3, "pT2 (MP)", "pT2 (MP)", None, "pT2"),
        (4, "pT3 (SS)", "pT3 (SS)", None, "pT3"),
        (5, "pT4a (SE)", "pT4a (SE)", None, "pT4a"),
        (6, "pT4b (SI)", "pT4b (SI)", None, "pT4b"),
        (9, "pTX", "pTX", None, "pTX"),
    ],
    "p_ln_gastric": [
        (0, "pN0", "pN0", None, "pN0"),
        (1, "pN1", "pN1", None, "pN1"),
        (2, "pN2", "pN2", None, "pN2"),
        (3, "pN3a", "pN3a", None, "pN3a"),
        (4, "pN3b", "pN3b", None, "pN3b"),
        (9, "pNX", "pNX", None, "pNX"),
    ],
    "p_stage_gastric": [
        (0, "0", "0", None, None),
        (1, "IA", "IA", None, "IA"),
        (2, "IB", "IB", None, "IB"),
        (3, "IIA", "IIA", None, "IIA"),
        (4, "IIB", "IIB", None, "IIB"),
        (5, "IIIA", "IIIA", None, "IIIA"),
        (6, "IIIB", "IIIB", None, "IIIB"),
        (7, "IIIC", "IIIC", None, "IIIC"),
        (8, "IV", "IV", None, "IV"),
        (9, "X", "X", None, "不明"),
    ],
}

# ------------------------------------------------------------------
# 胃癌取扱い規約 15版 (version_id=1)
# ------------------------------------------------------------------
GASTRIC_V15_ENTRIES = {
    "c_depth_gastric": [
        (1, "cT1a (M)", "cT1a (M)", None, "cT1a"),
        (2, "cT1b (SM)", "cT1b (SM)", None, "cT1b"),
        (3, "cT2 (MP)", "cT2 (MP)", None, "cT2"),
        (4, "cT3 (SS)", "cT3 (SS)", None, "cT3"),
        (5, "cT4a (SE)", "cT4a (SE)", None, "cT4a"),
        (6, "cT4b (SI)", "cT4b (SI)", None, "cT4b"),
        (9, "cTX", "cTX", None, "cTX"),
    ],
    "yc_depth_gastric": [
        (1, "ycT1a (M)", "ycT1a (M)", None, "ycT1a"),
        (2, "ycT1b (SM)", "ycT1b (SM)", None, "ycT1b"),
        (3, "ycT2 (MP)", "ycT2 (MP)", None, "ycT2"),
        (4, "ycT3 (SS)", "ycT3 (SS)", None, "ycT3"),
        (5, "ycT4a (SE)", "ycT4a (SE)", None, "ycT4a"),
        (6, "ycT4b (SI)", "ycT4b (SI)", None, "ycT4b"),
        (9, "ycTX", "ycTX", None, "ycTX"),
    ],
    "c_ln_gastric": [
        (0, "cN0", "cN0", None, "cN0"),
        (1, "cN1", "cN1", None, "cN1"),
        (2, "cN2", "cN2", None, "cN2"),
        (3, "cN3a", "cN3a", None, "cN3a"),
        (4, "cN3b", "cN3b", None, "cN3b"),
        (9, "cNX", "cNX", None, "cNX"),
    ],
    "yc_ln_gastric": [
        (0, "ycN0", "ycN0", None, "ycN0"),
        (1, "ycN1", "ycN1", None, "ycN1"),
        (2, "ycN2", "ycN2", None, "ycN2"),
        (3, "ycN3a", "ycN3a", None, "ycN3a"),
        (4, "ycN3b", "ycN3b", None, "ycN3b"),
        (9, "ycNX", "ycNX", None, "ycNX"),
    ],
    "c_stage_gastric": [
        (1, "I", "I", None, "I"),
        (2, "IIA", "IIA", None, "IIA"),
        (3, "IIB", "IIB", None, "IIB"),
        (4, "III", "III", None, "III"),
        (5, "IVA", "IVA", None, "IVA"),
        (6, "IVB", "IVB", None, "IVB"),
    ],
    "p_depth_gastric": [
        (0, "pT0", "pT0", None, "pT0"),
        (1, "pT1a (M)", "pT1a (M)", None, "pT1a"),
        (2, "pT1b (SM)", "pT1b (SM)", None, "pT1b"),
        (3, "pT2 (MP)", "pT2 (MP)", None, "pT2"),
        (4, "pT3 (SS)", "pT3 (SS)", None, "pT3"),
        (5, "pT4a (SE)", "pT4a (SE)", None, "pT4a"),
        (6, "pT4b (SI)", "pT4b (SI)", None, "pT4b"),
        (9, "pTX", "pTX", None, "pTX"),
    ],
    "p_ln_gastric": [
        (0, "pN0", "pN0", None, "pN0"),
        (1, "pN1", "pN1", None, "pN1"),
        (2, "pN2", "pN2", None, "pN2"),
        (3, "pN3a", "pN3a", None, "pN3a"),
        (4, "pN3b", "pN3b", None, "pN3b"),
        (9, "pNX", "pNX", None, "pNX"),
    ],
    "p_stage_gastric": [
        (0, "0", "0", None, None),
        (1, "IA", "IA", None, "IA"),
        (2, "IB", "IB", None, "IB"),
        (3, "IIA", "IIA", None, "IIA"),
        (4, "IIB", "IIB", None, "IIB"),
        (5, "IIIA", "IIIA", None, "IIIA"),
        (6, "IIIB", "IIIB", None, "IIIB"),
        (7, "IIIC", "IIIC", None, "IIIC"),
        (8, "IV", "IV", None, "IV"),
    ],
}

# ------------------------------------------------------------------
# UICC-TNM 8版 (version_id=2)
# ------------------------------------------------------------------
UICC8_ENTRIES = {
    "c_depth_uicc8": [
        (1, "cT1a", "cT1a", None, None),
        (2, "cT1b", "cT1b", None, None),
        (3, "cT2", "cT2", None, None),
        (4, "cT3", "cT3", None, None),
        (5, "cT4a", "cT4a", None, None),
        (6, "cT4b", "cT4b", None, None),
        (9, "cTX", "cTX", None, None),
    ],
}

# ------------------------------------------------------------------
# 食道癌取扱い規約 12版 (version_id=3)
# ------------------------------------------------------------------
ESO_V12_ENTRIES = {
    "c_depth_eso": [
        (11, "cT1a (EP)", "cT1a_EP", None, "cT1a_EP"),
        (12, "cT1a (LPM)", "cT1a_LPM", None, "cT1a_LPM"),
        (13, "cT1a (MM)", "cT1a_MM", None, "cT1a_MM"),
        (21, "cT1b (SM1)", "cT1b_SM1", None, "cT1b_SM1"),
        (22, "cT1b (SM2)", "cT1b_SM2", None, "cT1b_SM2"),
        (23, "cT1b (SM3)", "cT1b_SM3", None, "cT1b_SM3"),
        (30, "cT2 (MP)", "cT2", None, "cT2"),
        (41, "cT3r (AD)", "cT3r", None, "cT3r"),
        (42, "cT3br (AD)", "cT3br", None, "cT3br"),
        (50, "cT4a", "cT4a", None, "cT4a"),
        (60, "cT4b", "cT4b", None, "cT4b"),
        (99, "cTX", "cTX", None, "cTX"),
    ],
    "c_ln_eso": [
        (0, "cN0", "cN0", None, None),
        (1, "cN1 (1-2個)", "cN1", None, None),
        (2, "cN2 (3-6個)", "cN2", None, None),
        (3, "cN3 (7個以上)", "cN3", None, None),
        (9, "cNX", "cNX", None, None),
    ],
    "c_stage_eso": [
        (0, "0", "0", None, None),
        (1, "I", "I", None, None),
        (2, "II", "II", None, None),
        (3, "III", "III", None, None),
        (4, "IVA", "IVA", None, None),
        (5, "IVB", "IVB", None, None),
    ],
    "p_depth_eso": [
        (0, "pT0", "pT0", None, None),
        (1, "pT1a (EP/LPM)", "pT1a", None, None),
        (2, "pT1b (MM/SM)", "pT1b", None, None),
        (3, "pT2 (MP)", "pT2", None, None),
        (4, "pT3 (AD)", "pT3", None, None),
        (5, "pT4a", "pT4a", None, None),
        (6, "pT4b", "pT4b", None, None),
        (9, "pTX", "pTX", None, None),
    ],
    "p_ln_eso": [
        (0, "pN0", "pN0", None, None),
        (1, "pN1 (1-2個)", "pN1", None, None),
        (2, "pN2 (3-6個)", "pN2", None, None),
        (3, "pN3 (7個以上)", "pN3", None, None),
        (9, "pNX", "pNX", None, None),
    ],
    "p_stage_eso": [
        (0, "0", "0", None, None),
        (1, "I", "I", None, None),
        (2, "II", "II", None, None),
        (3, "III", "III", None, None),
        (4, "IVA", "IVA", None, None),
        (5, "IVB", "IVB", None, None),
    ],
    # 食道特有：腫瘍局在
    "eso_tumor_circumference": [
        (1, "前壁", "Anterior", None, None),
        (2, "後壁", "Posterior", None, None),
        (3, "右壁", "Right", None, None),
        (4, "左壁", "Left", None, None),
        (5, "全周", "Circumferential", None, None),
    ],
}

# ------------------------------------------------------------------
# レジメン（化学療法 — 規約非依存）
# ------------------------------------------------------------------
REGIMEN_ENTRIES = {
    "nac_regimen_gastric": [
        (1, "SOX", "SOX", None, None),
        (2, "SP", "SP", None, None),
        (3, "DOS", "DOS", None, None),
        (4, "FLOT", "FLOT", None, None),
        (5, "Nivo+SOX", "Nivo+SOX", None, None),
        (6, "Nivo+CAPOX", "Nivo+CAPOX", None, None),
        (99, "その他", "Other", None, None),
    ],
    "adj_regimen_gastric": [
        (1, "S-1", "S-1", None, None),
        (2, "CAPOX", "CAPOX", None, None),
        (3, "SOX", "SOX", None, None),
        (4, "DS", "DS", None, None),
        (5, "Nivo+CAPOX", "Nivo+CAPOX", None, None),
        (6, "Nivo+SOX", "Nivo+SOX", None, None),
        (99, "その他", "Other", None, None),
    ],
    "pal_regimen_gastric": [
        (1, "SOX", "SOX", None, None),
        (2, "CAPOX", "CAPOX", None, None),
        (3, "SP", "SP", None, None),
        (4, "Nivo+SOX", "Nivo+SOX", None, None),
        (5, "Nivo+CAPOX", "Nivo+CAPOX", None, None),
        (6, "T-Mab+CAPOX", "T-Mab+CAPOX", None, None),
        (7, "T-Mab+SOX", "T-Mab+SOX", None, None),
        (8, "nab-PTX", "nab-PTX", None, None),
        (9, "RAM+nab-PTX", "RAM+nab-PTX", None, None),
        (10, "RAM+PTX", "RAM+PTX", None, None),
        (11, "Nivolumab", "Nivolumab", None, None),
        (12, "Pembrolizumab", "Pembrolizumab", None, None),
        (13, "TAS-102", "TAS-102", None, None),
        (14, "Irinotecan", "Irinotecan", None, None),
        (15, "T-DXd", "T-DXd", None, None),
        (16, "Zolbetuximab+mFOLFOX6", "Zolbetuximab+mFOLFOX6", None, None),
        (99, "その他", "Other", None, None),
    ],
    "nac_regimen_eso": [
        (1, "CF", "CF (5-FU+CDDP)", None, None),
        (2, "DCF", "DCF", None, None),
        (3, "CF+RT", "CF+RT (CRT)", None, None),
        (4, "Nivo+Ipi+chemo", "Nivo+Ipi+Chemo", None, None),
        (5, "Nivo+CF", "Nivo+CF", None, None),
        (99, "その他", "Other", None, None),
    ],
    "adj_regimen_eso": [
        (1, "Nivolumab", "Nivolumab", None, None),
        (2, "S-1", "S-1", None, None),
        (3, "CF", "CF", None, None),
        (99, "その他", "Other", None, None),
    ],
}

# ------------------------------------------------------------------
# 放射線療法
# ------------------------------------------------------------------
RT_ENTRIES = {
    "rt_intent": [
        (1, "根治的", "Curative", None, None),
        (2, "術前", "Neoadjuvant", None, None),
        (3, "術後", "Adjuvant", None, None),
        (4, "緩和的", "Palliative", None, None),
    ],
    "rt_modality": [
        (1, "3D-CRT", "3D-CRT", None, None),
        (2, "IMRT", "IMRT", None, None),
        (3, "SRT/SBRT", "SRT/SBRT", None, None),
        (4, "粒子線", "Particle beam", None, None),
        (9, "その他", "Other", None, None),
    ],
}


# ============================================================
# Stage 自動計算テーブル
# ============================================================
# キー: (T_code, N_code)  → 値: Stage_code
# T_code / N_code はそれぞれ codebook 内の整数コード値
# ※対応表は要確認 — 規約改訂時にここを修正してください
# ------------------------------------------------------------------

# --- 胃癌取扱い規約 第14版 cStage (M0) ---
# T: 1=T1a, 2=T1b, 3=T2, 4=T3, 5=T4a, 6=T4b
# N: 0=N0, 1=N1, 2=N2, 3=N3a, 4=N3b
# Stage: 1=IA,2=IIA,3=IIB,4=III,5=IVA,6=IVB
GASTRIC15_CSTAGE_TABLE = {
    (1, 0): 1,  (1, 1): 2,  (1, 2): 2,  (1, 3): 2,  (1, 4): 2,   # T1a
    (2, 0): 1,  (2, 1): 2,  (2, 2): 2,  (2, 3): 2,  (2, 4): 2,   # T1b
    (3, 0): 1,  (3, 1): 2,  (3, 2): 2,  (3, 3): 2,  (3, 4): 2,   # T2
    (4, 0): 3,  (4, 1): 4,  (4, 2): 4,  (4, 3): 4,  (4, 4): 4,   # T3
    (5, 0): 3,  (5, 1): 4,  (5, 2): 4,  (5, 3): 4,  (5, 4): 4,   # T4a
    (6, 0): 6,  (6, 1): 6,  (6, 2): 6,  (6, 3): 6,  (6, 4): 6,   # T4b
}

# --- 胃癌取扱い規約 第15版 cStage (M0) ---
# T: 1=T1a, 2=T1b, 3=T2, 4=T3, 5=T4a, 6=T4b
# N: 0=N0, 1=N1, 2=N2, 3=N3a, 4=N3b
# Stage: 1=IA,2=IIA,3=IIB,4=III,5=IVA,6=IVB
GASTRIC15_CSTAGE_TABLE = {
    (1, 0): 1,  (1, 1): 2,  (1, 2): 2,  (1, 3): 2,  (1, 4): 2,   # T1a
    (2, 0): 1,  (2, 1): 2,  (2, 2): 2,  (2, 3): 2,  (2, 4): 2,   # T1b
    (3, 0): 1,  (3, 1): 2,  (3, 2): 2,  (3, 3): 2,  (3, 4): 2,   # T2
    (4, 0): 3,  (4, 1): 4,  (4, 2): 4,  (4, 3): 4,  (4, 4): 4,   # T3
    (5, 0): 3,  (5, 1): 4,  (5, 2): 4,  (5, 3): 4,  (5, 4): 4,   # T4a
    (6, 0): 6,  (6, 1): 6,  (6, 2): 6,  (6, 3): 6,  (6, 4): 6,   # T4b
}

# --- 胃癌取扱い規約 第15版 pStage (M0, P0, H0, CY0) ---
# pT: 0=pT0,1=pT1a,2=pT1b,3=pT2,4=pT3,5=pT4a,6=pT4b
# pN: 0=pN0,1=pN1,2=pN2,3=pN3a,4=pN3b
# pStage: 0=0(pCR等),1=IA,...,8=IV
GASTRIC15_PSTAGE_TABLE = {
    # pT0
    (0, 0): 0,
    # pT1a〜pT4b
    (1, 0): 1,  (1, 1): 2,  (1, 2): 3,  (1, 3): 4,  (1, 4): 6,   # T1a
    (2, 0): 1,  (2, 1): 2,  (2, 2): 3,  (2, 3): 4,  (2, 4): 6,   # T1b
    (3, 0): 2,  (3, 1): 3,  (3, 2): 4,  (3, 3): 5,  (3, 4): 6,   # T2
    (4, 0): 3,  (4, 1): 4,  (4, 2): 5,  (4, 3): 6,  (4, 4): 7,   # T3
    (5, 0): 4,  (5, 1): 5,  (5, 2): 5,  (5, 3): 6,  (5, 4): 7,   # T4a
    (6, 0): 5,  (6, 1): 6,  (6, 2): 6,  (6, 3): 7,  (6, 4): 7,   # T4b
}

# --- 食道癌取扱い規約 第12版 cStage (M0) ---
# T: 1=T1a,2=T1b,3=T2,4=T3,5=T4a,6=T4b
# N: 0=N0,1=N1,2=N2,3=N3
# Stage: 0=0,1=I,2=II,3=III,4=IVA,5=IVB
ESO12_CSTAGE_TABLE = {
    (1, 0): 0,  (1, 1): 1,  (1, 2): 2,  (1, 3): 2,   # T1a
    (2, 0): 1,  (2, 1): 2,  (2, 2): 2,  (2, 3): 3,   # T1b
    (3, 0): 2,  (3, 1): 2,  (3, 2): 3,  (3, 3): 3,   # T2
    (4, 0): 2,  (4, 1): 3,  (4, 2): 3,  (4, 3): 4,   # T3
    (5, 0): 3,  (5, 1): 4,  (5, 2): 4,  (5, 3): 4,   # T4a
    (6, 0): 4,  (6, 1): 4,  (6, 2): 4,  (6, 3): 4,   # T4b → IVA
}

# --- 食道癌取扱い規約 第12版 pStage (M0) ---
ESO12_PSTAGE_TABLE = {
    (0, 0): 0,  # pT0 → Stage 0
    (1, 0): 0,  (1, 1): 1,  (1, 2): 2,  (1, 3): 2,   # T1a
    (2, 0): 1,  (2, 1): 2,  (2, 2): 2,  (2, 3): 3,   # T1b
    (3, 0): 2,  (3, 1): 2,  (3, 2): 3,  (3, 3): 3,   # T2
    (4, 0): 2,  (4, 1): 3,  (4, 2): 3,  (4, 3): 4,   # T3
    (5, 0): 3,  (5, 1): 4,  (5, 2): 4,  (5, 3): 4,   # T4a
    (6, 0): 4,  (6, 1): 4,  (6, 2): 4,  (6, 3): 4,   # T4b
}


def compute_stage(
    t_code, n_code, m_code,
    is_gastric=True,
    context="clinical",
    p_peritoneal=None, p_liver=None, p_cytology=None,
):
    """
    T/N/M コードから Stage コードを自動計算する。

    Parameters
    ----------
    t_code, n_code, m_code : int or None
        codebook の整数コード値。TX=9, NX=9, MX=9。
    is_gastric : bool
        True=胃癌, False=食道癌
    context : str
        "clinical" = cStage / ycStage, "pathological" = pStage
    p_peritoneal, p_liver, p_cytology : int or None
        P: 0=P0, 1-4=P1a-P1x (陽性), 9=PX
        H: 0=H0, 1=H1 (陽性), 9=HX
        CY: 0=CY0, 1=CY1 (陽性)
        臨床・病理どちらでも使用。

    Returns
    -------
    int or None
        Stage コード。計算不能 (TX/NX/MX 含む) の場合は None。
    """
    # --- 不明因子が含まれる場合は計算しない ---
    if t_code is None or n_code is None or m_code is None:
        return None
    if t_code == 9 or n_code == 9 or m_code == 9:
        return None

    # --- M1 の処理 ---
    if m_code == 1:
        if is_gastric:
            # 胃癌取扱い規約15版: M1 → IVB (code=9)
            return 9
        else:
            # 食道癌取扱い規約12版: M1 → IVB (code=5)
            return 5

    # --- P/H陽性判定 ---
    _p_positive = p_peritoneal is not None and p_peritoneal in (1, 2, 3, 4)
    _h_positive = p_liver is not None and p_liver == 1

    # --- 臨床: P陽性/H陽性 → IVB ---
    if context == "clinical":
        if _p_positive or _h_positive:
            if is_gastric:
                return 6  # IVB (胃癌取扱い規約15版 version_id=1)

    # --- 病理: P陽性/H陽性/CY1 → IV ---
    if context == "pathological":
        if _p_positive or _h_positive or (p_cytology == 1):
            if is_gastric:
                return 8  # IV (胃癌取扱い規約15版 version_id=1)
            else:
                return 4  # IVA (食道癌)

    # --- T×N テーブル参照 (M0) ---
    if is_gastric:
        table = GASTRIC15_CSTAGE_TABLE if context == "clinical" else GASTRIC15_PSTAGE_TABLE
    else:
        table = ESO12_CSTAGE_TABLE if context == "clinical" else ESO12_PSTAGE_TABLE

    return table.get((t_code, n_code))


# ============================================================
# データ投入関数
# ============================================================
def populate_codebook(reset=False):
    """コードブックマスタにデータを投入する。"""
    with get_db() as conn:
        if reset:
            conn.execute("DELETE FROM codebook")
            print("既存コードブックデータを削除しました。")

        inserted = 0

        def _upsert_row(version_id, field_name, code, label, label_en,
                         sort_order, ncd, reg):
            """version_id が NULL でも正しく upsert する。"""
            if version_id is None:
                existing = conn.execute(
                    """SELECT id FROM codebook
                       WHERE version_id IS NULL AND field_name=? AND code=?""",
                    (field_name, code),
                ).fetchone()
            else:
                existing = conn.execute(
                    """SELECT id FROM codebook
                       WHERE version_id=? AND field_name=? AND code=?""",
                    (version_id, field_name, code),
                ).fetchone()

            if existing:
                conn.execute(
                    """UPDATE codebook SET label=?, label_en=?, sort_order=?,
                       ncd_mapping=?, registry_mapping=?, is_active=1
                       WHERE id=?""",
                    (label, label_en, sort_order, ncd, reg, existing[0]),
                )
            else:
                conn.execute(
                    """INSERT INTO codebook
                       (version_id, field_name, code, label, label_en,
                        sort_order, is_active, ncd_mapping, registry_mapping)
                       VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)""",
                    (version_id, field_name, code, label, label_en,
                     sort_order, ncd, reg),
                )

        def _insert_entries(version_id, entries_dict):
            nonlocal inserted
            for field_name, items in entries_dict.items():
                for sort_order, (code, label, label_en, ncd, reg) in enumerate(items):
                    _upsert_row(version_id, field_name, code, label, label_en,
                                sort_order, ncd, reg)
                    inserted += 1

        # 共通 (version_id=NULL)
        _insert_entries(None, COMMON_ENTRIES)
        print(f"  共通項目: {inserted} 件投入")

        mark = inserted
        _insert_entries(1, GASTRIC_V15_ENTRIES)
        print(f"  胃癌取扱い規約15版: {inserted - mark} 件投入")

        mark = inserted
        _insert_entries(2, UICC8_ENTRIES)
        print(f"  UICC-TNM 8版: {inserted - mark} 件投入")

        mark = inserted
        _insert_entries(3, ESO_V12_ENTRIES)
        print(f"  食道癌取扱い規約12版: {inserted - mark} 件投入")

        mark = inserted
        _insert_entries(None, REGIMEN_ENTRIES)
        print(f"  レジメン: {inserted - mark} 件投入")

        mark = inserted
        _insert_entries(None, RT_ENTRIES)
        print(f"  放射線療法: {inserted - mark} 件投入")

        # --- CODEBOOK dict にないコードを is_active=0 に ---
        all_dicts = [
            (None, COMMON_ENTRIES), (None, REGIMEN_ENTRIES), (None, RT_ENTRIES),
            (1, GASTRIC_V15_ENTRIES), (2, UICC8_ENTRIES), (3, ESO_V12_ENTRIES),
        ]
        for vid, entries_dict in all_dicts:
            for field_name, items in entries_dict.items():
                valid_codes = {code for code, *_ in items}
                if vid is None:
                    rows = conn.execute(
                        "SELECT id, code FROM codebook WHERE version_id IS NULL AND field_name=? AND is_active=1",
                        (field_name,),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        "SELECT id, code FROM codebook WHERE version_id=? AND field_name=? AND is_active=1",
                        (vid, field_name),
                    ).fetchall()
                for row in rows:
                    if row[1] not in valid_codes:
                        conn.execute("UPDATE codebook SET is_active=0 WHERE id=?", (row[0],))

        print(f"\n✅ コードブック投入完了: 合計 {inserted} 件")


# ============================================================
# カラム名 → 日本語ラベル（一元管理）
# ============================================================
# 全モジュール（analytics.py, statistical_analysis.py, smart_query.py 等）で
# get_column_label() を通じて参照する。新規カラム追加時はここに追記する。
COLUMN_LABELS = {
    # --- 患者背景 ---
    # ルール: _ は単位専用(_cm,_kg,_min,_mo,_mL,_mm,_pct)
    #         並列はそのまま結合, 英語略語はハイフン・スペースなし
    #         _yn カラムは _有無 付与
    "age_at_surgery": "手術時年齢",
    "sex": "性別",
    "bmi": "BMI",
    "bmi_change_pct": "体重変化率_pct",
    "height_cm": "身長_cm",
    "weight_admission": "入院時体重_kg",
    "weight_discharge": "退院時体重_kg",
    "ps": "PS",
    "asa": "ASA",
    "disease_class": "疾患分類",
    "disease_category": "疾患カテゴリ",
    "surgery_year": "手術年",
    "smoking": "喫煙",
    "alcohol": "飲酒",
    "adl_status": "ADL",
    "preop_weight_loss_10pct": "術前10pct以上体重減少",
    "hp_eradication": "ピロリ除菌歴",
    # --- 併存疾患 ---
    "comor_hypertension": "高血圧",
    "comor_cardiovascular": "心血管疾患",
    "comor_cerebrovascular": "脳血管疾患",
    "comor_respiratory": "呼吸器疾患",
    "comor_renal": "腎疾患",
    "comor_renal_dialysis": "透析",
    "comor_hepatic": "肝疾患",
    "comor_diabetes": "糖尿病",
    "comor_endocrine": "内分泌疾患",
    "comor_collagen": "膠原病",
    "comor_hematologic": "血液疾患",
    "comor_neurologic": "神経疾患",
    "comor_psychiatric": "精神疾患",
    "comor_other": "その他併存疾患",
    # --- NCD併存疾患サブカラム ---
    "comor_ihd": "虚血性心疾患",
    "comor_chf": "心不全",
    "comor_arrhythmia": "不整脈",
    "comor_valvular": "弁膜症",
    "comor_aortic": "大動脈疾患",
    "comor_pvd": "末梢血管疾患",
    "comor_cerebral_infarction": "脳梗塞",
    "comor_cerebral_hemorrhage": "脳出血",
    "comor_tia": "TIA",
    "comor_sah": "くも膜下出血",
    "comor_cirrhosis": "肝硬変",
    "comor_portal_htn": "門脈圧亢進",
    "comor_hepatitis_virus": "肝炎ウイルス",
    "comor_dyspnea": "呼吸困難",
    "comor_ventilator": "人工呼吸器使用",
    "smoking_type": "喫煙種類",
    "smoking_bi": "BI指数",
    "adl_status_preop": "術直前ADL",
    # --- 内服薬 ---
    "med_antihypertensive": "降圧薬",
    "med_antithrombotic": "抗血栓薬",
    "med_oral_hypoglycemic": "経口血糖降下薬",
    "med_insulin": "インスリン",
    "med_steroid_immunosup": "ステロイド免疫抑制薬",
    "med_antineoplastic": "抗腫瘍薬",
    "med_thyroid": "甲状腺薬",
    "med_psychotropic": "向精神薬",
    "med_other": "その他内服薬",
    # --- 症状 ---
    "sym_asymptomatic": "無症状",
    "sym_epigastric_pain": "心窩部痛",
    "sym_dysphagia": "嚥下困難",
    "sym_weight_loss": "体重減少",
    "sym_anemia": "貧血",
    "sym_melena": "下血",
    "sym_hematemesis": "吐血",
    "sym_nausea_vomiting": "悪心嘔吐",
    "sym_abdominal_distension": "腹部膨満",
    "sym_obstruction": "通過障害",
    "sym_other": "その他症状",
    # --- 術前検査 ---
    "preop_alb": "術前Alb",
    "preop_hb": "術前Hb",
    "preop_crp": "術前CRP",
    "c_tumor_size_major_mm": "腫瘍径_mm",
    "c_macroscopic_type": "肉眼型",
    "c_histology1": "組織型1",
    "c_histology2": "組織型2",
    "c_histology3": "組織型3",
    # --- Stage ---
    "c_depth": "cT",
    "c_ln_metastasis": "cN",
    "c_distant_metastasis": "cM",
    "c_stage": "cStage",
    "p_depth": "pT",
    "p_ln_metastasis": "pN",
    "p_stage": "pStage",
    "p_residual_tumor": "残存腫瘍R",
    "p_histology1": "病理組織型",
    "p_ly": "ly",
    "p_v": "v",
    "p_inf": "INF",
    # --- 手術 ---
    "op_approach": "到達法",
    "op_procedure": "術式",
    "op_dissection": "郭清度",
    "op_reconstruction": "再建法",
    "op_anastomosis_method": "吻合法",
    "op_surgeon": "執刀医",
    "op_assistant1": "第1助手",
    "op_assistant2": "第2助手",
    "op_scopist": "スコピスト",
    "op_emergency": "緊急手術",
    "op_time_min": "手術時間_min",
    "op_console_time_min": "コンソール時間_min",
    "op_blood_loss_ml": "出血量_mL",
    "op_transfusion_preop": "術前輸血（72h以内）",
    "op_transfusion_preop_rbc": "術前RBC_単位",
    "op_transfusion_preop_ffp": "術前FFP_単位",
    "op_transfusion_preop_pc": "術前PC_単位",
    "op_transfusion_intra": "術中輸血",
    "op_transfusion_intra_rbc": "術中RBC_単位",
    "op_transfusion_intra_ffp": "術中FFP_単位",
    "op_transfusion_intra_pc": "術中PC_単位",
    "op_transfusion_post": "術後輸血",
    "op_transfusion_post_rbc": "術後RBC_単位",
    "op_transfusion_post_ffp": "術後FFP_単位",
    "op_transfusion_post_pc": "術後PC_単位",
    "op_icu_days": "ICU日数",
    "op_reop_yn": "再手術_有無",
    # --- 合併症 ---
    "op_complication_yn": "合併症有無",
    "op_cd_grade_max": "最大CD分類",
    "comp_anastomotic_leak": "縫合不全",
    "comp_pancreatic_fistula": "膵液瘻",
    "comp_pneumonia": "肺炎",
    "comp_ileus": "イレウス",
    "comp_dge": "DGE",
    "comp_ssi": "SSI",
    "comp_rln_palsy": "反回神経麻痺",
    "comp_bleeding": "出血",
    "comp_wound_dehiscence": "創離開",
    "comp_intra_abd_abscess": "腹腔内膿瘍",
    "comp_bile_leak": "胆汁漏",
    "comp_duodenal_stump_leak": "十二指腸断端瘻",
    "comp_chylothorax": "乳び胸",
    "comp_cardiac": "心合併症",
    "comp_delirium": "せん妄",
    "comp_dvt_pe": "DVTPE",
    "comp_anastomotic_stricture": "吻合部狭窄",
    "comp_anastomotic_bleeding": "吻合部出血",
    "comp_perforation": "穿孔",
    "comp_cholelithiasis": "胆石症",
    "comp_empyema": "膿胸",
    "comp_pneumothorax": "気胸",
    "comp_ards": "ARDS",
    "comp_dic": "DIC",
    "comp_sepsis": "敗血症",
    "comp_renal_failure": "腎不全",
    "comp_hepatic_failure": "肝不全",
    "comp_uti": "尿路感染",
    "comp_atelectasis": "無気肺",
    # --- NCD合併症サブカラム ---
    "comp_ssi_superficial": "表層SSI",
    "comp_ssi_deep": "深部SSI",
    "comp_ssi_organ": "臓器体腔SSI",
    "comp_dvt": "DVT",
    "comp_pe": "PE",
    "comp_septic_shock": "敗血症性ショック",
    "comp_anastomotic_leak_type": "縫合不全分類",
    "comp_pancreatic_fistula_isgpf": "膵液瘻ISGPF",
    "comp_dge_isgps": "DGE_ISGPS",
    # --- アウトカム ---
    "readmission_30d": "30日再入院",
    "mortality_30d": "30日死亡",
    "mortality_inhospital": "在院死亡",
    "s_mortality_30d": "術後30日死亡",
    "s_mortality_inhospital": "術後在院死亡",
    "vital_status": "生存状態",
    "recurrence_yn": "再発_有無",
    "os_event": "死亡イベント",
    "rfs_event": "再発死亡イベント",
    "os_months": "全生存期間_mo",
    "rfs_months": "無再発生存期間_mo",
    # --- 化学療法 ---
    "nac_yn": "術前化学療法_有無",
    "nac_regimen": "NACレジメン",
    "recist_overall": "RECIST総合効果",
    "adj_yn": "術後補助化学療法_有無",
    "adj_regimen": "術後化学療法レジメン",
    # --- バイオマーカー ---
    "msi_status": "MSI",
    "her2_status": "HER2",
    "pdl1_cps": "PDL1_CPS",
    "claudin18_status": "Claudin182",
    "fgfr2b_status": "FGFR2b",
    "ebv_status": "EBV",
    # --- 食道腫瘍 (eso_tumor) ---
    "c_location_eso": "食道腫瘍局在",
    "c_multiple_cancer_eso": "食道多発癌",
    "c_depth_jce": "cT_規約",
    "c_depth_uicc": "cT_UICC",
    "c_ln_jce": "cN_規約",
    "c_ln_uicc": "cN_UICC",
    "c_distant_jce": "cM_規約",
    "c_distant_uicc": "cM_UICC",
    "c_stage_jce": "cStage_規約",
    "c_stage_uicc": "cStage_UICC",
    "c_pet_yn": "PET実施",
    "c_pet_accumulation": "PET集積",
    "c_pet_site": "PET集積部位",
    "c_ln_detail": "LN所見詳細",
    "yc_depth_jce": "ycT_規約",
    "yc_depth_uicc": "ycT_UICC",
    "yc_ln_jce": "ycN_規約",
    "yc_ln_uicc": "ycN_UICC",
    "yc_stage_jce": "ycStage_規約",
    "yc_stage_uicc": "ycStage_UICC",
    "nac_endoscopy_response": "NAC後内視鏡効果",
    # --- 食道手術 (eso_surgery) ---
    "op_type": "手術区分",
    "op_surgery_type": "術式区分_食道",
    "op_surgery_type_other": "術式その他_食道",
    "op_approach_detail": "アプローチ詳細",
    "op_approach_other": "アプローチその他",
    "op_endoscopic": "鏡視下手術",
    "op_endoscopic_other": "鏡視下手術その他",
    "op_conversion_detail": "開胸開腹移行",
    "op_conversion_reason": "移行理由",
    "op_resection_extent": "切除範囲_食道",
    "op_resection_other": "切除範囲その他",
    "op_reconstruction_route_eso": "再建経路_食道",
    "op_reconstruction_route_other": "再建経路その他",
    "op_reconstruction_organ": "再建臓器_食道",
    "op_reconstruction_organ_other": "再建臓器その他",
    "op_anastomosis_site": "吻合部位_食道",
    "op_anastomosis_site_other": "吻合部位その他",
    "op_dissection_field": "郭清領域_食道",
    "op_anesthesia_time_min": "麻酔時間_min",
    "op_thoracic_time_min": "胸腔操作時間_min",
    "op_thoracic_blood_loss_ml": "胸腔内出血量_mL",
    "op_surgeons": "術者",
    "hiatal_hernia_yn": "裂孔ヘルニア_有無",
    "hiatal_hernia_type": "ヘルニア型",
    "gerd_la": "GERD_LA分類",
    "hiatal_hernia_op": "ヘルニア手術",
    "hiatal_hernia_gate_mm": "ヘルニア門径_mm",
    "hiatal_mesh": "メッシュ",
    "fundoplication": "噴門形成術",
    "vagus_nerve": "迷走神経",
    # --- 食道病理 (eso_pathology) ---
    "p_pretreatment": "術前治療_食道",
    "p_depth_jce": "pT_規約",
    "p_depth_uicc": "pT_UICC",
    "p_ln_jce": "pN_規約",
    "p_ln_uicc": "pN_UICC",
    "p_stage_jce": "pStage_規約",
    "p_stage_uicc": "pStage_UICC",
    "p_rm": "断端RM",
    "p_rm_mm": "断端距離_mm",
    "p_im_eso": "食道IM",
    "p_im_stomach": "胃IM",
    "p_multiple_cancer_eso": "病理多発癌_食道",
    "p_curability": "根治度",
    "p_residual_factor": "残存腫瘍詳細",
    # --- 食道経過 (eso_course) ---
    "icu_discharge_date": "ICU退室日",
    "meal_water_date": "飲水開始日",
    "meal_liquid_date": "流動食開始日",
    "meal_3bu_date": "3分粥開始日",
    "meal_5bu_date": "5分粥開始日",
    "meal_zenkayu_date": "全粥開始日",
    "npo_date": "絶食日",
    "meal_water_date2": "飲水再開日",
    "meal_liquid_date2": "流動食再開日",
    "meal_3bu_date2": "3分粥再開日",
    "meal_5bu_date2": "5分粥再開日",
    "meal_zenkayu_date2": "全粥再開日",
    "drain_left_chest_date": "左胸腔ドレーン抜去日",
    "drain_right_chest_date": "右胸腔ドレーン抜去日",
    "drain_neck_date": "頸部ドレーン抜去日",
    "tube_feeding_yn": "経腸栄養_有無",
    "tube_feeding_start": "経腸栄養開始日",
    "tube_feeding_end": "経腸栄養終了日",
    "icu_type": "ICU種別",
    "reintubation_yn": "再挿管_有無",
    "readmission_yn": "再入院_有無",
    "readmission_reason": "再入院理由",
    "reop_date": "再手術日1",
    "reop2_date": "再手術日2",
    "reop_detail": "再手術詳細",
    "course_notes": "経過メモ",
    "stricture_yn": "吻合部狭窄_有無",
    "stricture_first_date": "初回拡張日",
    "stricture_count": "拡張回数",
    # --- 患者背景（追加分） ---
    "id": "DB_ID",
    "study_id": "症例ID",
    "patient_id": "カルテNo",
    "birthdate": "生年月日",
    "admission_date": "入院日",
    "discharge_date": "退院日",
    "surgery_date": "手術日",
    "discharge_destination": "退院先",
    "initials": "イニシャル",
    "first_visit_date": "初診日",
    "first_treatment_completion_date": "初回治療完了日",
    "ncd_case_id": "NCD症例ID",
    "facility_id": "施設ID",
    "classification_version_id": "分類バージョンID",
    "is_deleted": "削除フラグ",
    "data_status": "データステータス",
    "created_at": "作成日時",
    "updated_at": "更新日時",
    "updated_by": "更新者",
    # --- Phase管理 ---
    "phase1_status": "Phase1ステータス",
    "phase1_submitted_at": "Phase1提出日時",
    "phase1_submitted_by": "Phase1提出者",
    "phase1_approved_at": "Phase1承認日時",
    "phase1_approved_by": "Phase1承認者",
    "phase3_status": "Phase3ステータス",
    "phase3_submitted_at": "Phase3提出日時",
    "phase3_submitted_by": "Phase3提出者",
    "phase3_approved_at": "Phase3承認日時",
    "phase3_approved_by": "Phase3承認者",
    "phase4_status": "Phase4ステータス",
    "phase4_submitted_at": "Phase4提出日時",
    "phase4_submitted_by": "Phase4提出者",
    "phase4_approved_at": "Phase4承認日時",
    "phase4_approved_by": "Phase4承認者",
    # --- 併存疾患（追加分） ---
    "comor_confirmed": "併存疾患確認済",
    "comor_dm_treatment": "糖尿病治療",
    "comor_ht_treatment": "高血圧治療",
    "comor_other_detail": "その他併存疾患詳細",
    # --- 家族歴 ---
    "fhx_confirmed": "家族歴確認済",
    "fhx_gastric": "家族歴_胃癌",
    "fhx_esophageal": "家族歴_食道癌",
    "fhx_colorectal": "家族歴_大腸癌",
    "fhx_liver": "家族歴_肝癌",
    "fhx_pancreas": "家族歴_膵癌",
    "fhx_lung": "家族歴_肺癌",
    "fhx_breast": "家族歴_乳癌",
    "fhx_other": "家族歴_その他",
    "fhx_other_detail": "家族歴その他詳細",
    # --- 重複癌 ---
    "synchronous_cancer_yn": "同時性重複癌_有無",
    "synchronous_cancer_organ": "同時性重複癌臓器",
    "synchronous_cancer_other": "同時性重複癌その他",
    "metachronous_cancer_yn": "異時性重複癌_有無",
    "metachronous_cancer_organ": "異時性重複癌臓器",
    "metachronous_cancer_other": "異時性重複癌その他",
    "sync_org_confirmed": "同時性重複癌確認済",
    "sync_org_stomach": "同時性_胃",
    "sync_org_esophagus": "同時性_食道",
    "sync_org_colorectum": "同時性_大腸",
    "sync_org_lung": "同時性_肺",
    "sync_org_hepatobiliary": "同時性_肝胆",
    "sync_org_pancreas": "同時性_膵",
    "sync_org_breast": "同時性_乳腺",
    "sync_org_urological": "同時性_泌尿器",
    "sync_org_gynecological": "同時性_婦人科",
    "sync_org_oral_pharynx": "同時性_口腔咽頭",
    "sync_org_thyroid": "同時性_甲状腺",
    "sync_org_hematologic": "同時性_血液",
    "sync_org_skin": "同時性_皮膚",
    "sync_org_nervous_system": "同時性_神経",
    "sync_org_other": "同時性_その他",
    "meta_org_confirmed": "異時性重複癌確認済",
    "meta_org_stomach": "異時性_胃",
    "meta_org_esophagus": "異時性_食道",
    "meta_org_colorectum": "異時性_大腸",
    "meta_org_lung": "異時性_肺",
    "meta_org_hepatobiliary": "異時性_肝胆",
    "meta_org_pancreas": "異時性_膵",
    "meta_org_breast": "異時性_乳腺",
    "meta_org_urological": "異時性_泌尿器",
    "meta_org_gynecological": "異時性_婦人科",
    "meta_org_oral_pharynx": "異時性_口腔咽頭",
    "meta_org_thyroid": "異時性_甲状腺",
    "meta_org_hematologic": "異時性_血液",
    "meta_org_skin": "異時性_皮膚",
    "meta_org_nervous_system": "異時性_神経",
    "meta_org_other": "異時性_その他",
    # --- 内服薬・症状（追加分） ---
    "med_confirmed": "内服薬確認済",
    "med_other_detail": "その他内服薬詳細",
    "sym_confirmed": "症状確認済",
    "sym_other_detail": "その他症状詳細",
    # --- 術前診断（追加分） ---
    "c_type0_subclass": "0型亜分類_c",
    "c_tumor_size_minor_mm": "腫瘍短径_mm_c",
    "c_tumor_number": "腫瘍個数_c",
    "c_location_long": "長軸局在_c",
    "c_location_short": "短軸局在_c",
    "c_location_egj": "EGJ局在_c",
    "c_egj_distance_mm": "EGJ距離_mm_c",
    "c_esophageal_invasion_mm": "食道浸潤距離_mm_c",
    "c_peritoneal": "c腹膜転移",
    "c_liver_metastasis": "c肝転移",
    "c_inv_pancreas": "c浸潤_膵",
    "c_inv_liver": "c浸潤_肝",
    "c_inv_transverse_colon": "c浸潤_横行結腸",
    "c_inv_spleen": "c浸潤_脾",
    "c_inv_diaphragm": "c浸潤_横隔膜",
    "c_inv_esophagus": "c浸潤_食道",
    "c_inv_duodenum": "c浸潤_十二指腸",
    "c_inv_aorta": "c浸潤_大動脈",
    "c_inv_abdominal_wall": "c浸潤_腹壁",
    "c_inv_adrenal": "c浸潤_副腎",
    "c_inv_kidney": "c浸潤_腎",
    "c_inv_small_intestine": "c浸潤_小腸",
    "c_inv_retroperitoneum": "c浸潤_後腹膜",
    "c_inv_transverse_mesocolon": "c浸潤_横行結腸間膜",
    "c_inv_other": "c浸潤_その他",
    "c_inv_other_detail": "c浸潤_その他詳細",
    "c_inv_confirmed": "c浸潤確認済",
    "c_inv_unknown": "c浸潤_不明",
    "c_meta_peritoneal": "c転移_腹膜",
    "c_meta_liver": "c転移_肝",
    "c_meta_lung": "c転移_肺",
    "c_meta_lymph_node": "c転移_リンパ節",
    "c_meta_bone": "c転移_骨",
    "c_meta_brain": "c転移_脳",
    "c_meta_ovary": "c転移_卵巣",
    "c_meta_adrenal": "c転移_副腎",
    "c_meta_skin": "c転移_皮膚",
    "c_meta_marrow": "c転移_骨髄",
    "c_meta_pleura": "c転移_胸膜",
    "c_meta_meninges": "c転移_髄膜",
    "c_meta_other": "c転移_その他",
    "c_meta_other_detail": "c転移_その他詳細",
    "c_meta_confirmed": "c転移確認済",
    "c_meta_cytology": "c転移_細胞診",
    "c_meta_unknown": "c転移_不明",
    "disease_class_other": "疾患分類その他",
    "remnant_stomach_yn": "残胃_有無",
    "remnant_location": "残胃局在",
    "remnant_initial_disease": "初回疾患",
    "remnant_initial_disease_other": "初回疾患その他",
    "remnant_interval_years": "初回手術後年数",
    "preop_cea": "術前CEA",
    "preop_ca199": "術前CA19-9",
    "preop_cr": "術前Cr",
    "preop_tbil": "術前T-Bil",
    "preop_hba1c": "術前HbA1c",
    "preop_wbc": "術前WBC",
    "preop_plt": "術前PLT",
    # --- 術前療法（追加分） ---
    "nac_start_date": "NAC開始日",
    "nac_courses": "NACコース数",
    "nac_regimen_other": "NACレジメンその他",
    "nac_completion": "NAC完遂",
    "nac_adverse_event": "NAC有害事象",
    "yc_depth": "ycT",
    "yc_nodal": "ycN",
    "yc_ln_metastasis": "ycN",
    "yc_distant_metastasis": "ycM",
    "yc_stage": "ycStage",
    "yc_macroscopic_type": "yc肉眼型",
    "yc_histology": "yc組織型",
    "yc_tumor_size_major_mm": "yc腫瘍長径_mm",
    "yc_tumor_size_minor_mm": "yc腫瘍短径_mm",
    "yc_location_long": "yc長軸局在",
    "yc_location_short": "yc短軸局在",
    "yc_tumor_number": "yc腫瘍個数",
    "yc_peritoneal": "yc腹膜転移",
    "yc_liver_metastasis": "yc肝転移",
    "primary_overall_response": "原発巣総合効果",
    "primary_shrinkage_pct": "原発巣縮小率_pct",
    "primary_depression": "原発巣陥凹変化",
    "primary_elevation": "原発巣隆起変化",
    "primary_stenosis": "原発巣狭窄変化",
    "recist_target1": "RECIST標的1_mm",
    "recist_target2": "RECIST標的2_mm",
    "recist_target3": "RECIST標的3_mm",
    "recist_target_response": "RECIST標的効果",
    "recist_shrinkage_pct": "RECIST縮小率_pct",
    "recist_nontarget1": "RECIST非標的1",
    "recist_nontarget2": "RECIST非標的2",
    "recist_nontarget3": "RECIST非標的3",
    "recist_nontarget_response": "RECIST非標的効果",
    "recist_new_lesion": "RECIST新病変",
    "recist_new_lesion_detail": "RECIST新病変詳細",
    # --- 手術（追加分） ---
    "op_conversion_yn": "開腹移行_有無",
    "op_completion": "手術完遂",
    "op_anesthesia_type": "麻酔種類",
    "op_procedure_other": "術式その他",
    "op_reconstruction_other": "再建法その他",
    "op_anastomosis_method_other": "吻合法その他",
    "op_peristalsis_direction": "蠕動方向",
    "op_reconstruction_route": "再建経路",
    "op_reop_30d": "30日以内再手術",
    "readmission_30d_reason": "再入院理由",
    # --- 合併切除 ---
    "comb_confirmed": "合併切除確認済",
    "comb_splenectomy": "合併_脾摘",
    "comb_cholecystectomy": "合併_胆摘",
    "comb_distal_pancreatectomy": "合併_膵体尾部切除",
    "comb_pancreatoduodenectomy": "合併_膵頭十二指腸切除",
    "comb_partial_hepatectomy": "合併_肝部分切除",
    "comb_transverse_colectomy": "合併_横行結腸切除",
    "comb_small_intestine": "合併_小腸切除",
    "comb_adrenalectomy": "合併_副腎摘出",
    "comb_diaphragm": "合併_横隔膜合併切除",
    "comb_abdominal_wall": "合併_腹壁合併切除",
    "comb_thoracic_esophagus": "合併_胸部食道切除",
    "comb_portal_vein": "合併_門脈合併切除",
    "comb_ovary": "合併_卵巣摘出",
    "comb_appleby": "合併_Appleby手術",
    "comb_other": "合併_その他",
    "comb_other_detail": "合併切除その他詳細",
    # --- 合併症（追加分：日付・治療） ---
    "comp_confirmed": "合併症確認済",
    "comp_other": "その他合併症",
    "comp_other_detail": "その他合併症詳細",
    "comp_other_date": "その他合併症発症日",
    "comp_other_tx": "その他合併症治療",
    "comp_anastomotic_leak_date": "縫合不全発症日",
    "comp_anastomotic_leak_tx": "縫合不全治療",
    "comp_pancreatic_fistula_date": "膵液瘻発症日",
    "comp_pancreatic_fistula_tx": "膵液瘻治療",
    "comp_pneumonia_date": "肺炎発症日",
    "comp_pneumonia_tx": "肺炎治療",
    "comp_ileus_date": "イレウス発症日",
    "comp_ileus_tx": "イレウス治療",
    "comp_dge_date": "DGE発症日",
    "comp_dge_tx": "DGE治療",
    "comp_ssi_date": "SSI発症日",
    "comp_ssi_tx": "SSI治療",
    "comp_ssi_superficial_date": "表層SSI発症日",
    "comp_ssi_superficial_tx": "表層SSI治療",
    "comp_ssi_deep_date": "深部SSI発症日",
    "comp_ssi_deep_tx": "深部SSI治療",
    "comp_ssi_organ_date": "臓器体腔SSI発症日",
    "comp_ssi_organ_tx": "臓器体腔SSI治療",
    "comp_rln_palsy_date": "反回神経麻痺発症日",
    "comp_rln_palsy_tx": "反回神経麻痺治療",
    "comp_bleeding_date": "出血発症日",
    "comp_bleeding_tx": "出血治療",
    "comp_wound_dehiscence_date": "創離開発症日",
    "comp_wound_dehiscence_tx": "創離開治療",
    "comp_intra_abd_abscess_date": "腹腔内膿瘍発症日",
    "comp_intra_abd_abscess_tx": "腹腔内膿瘍治療",
    "comp_bile_leak_date": "胆汁漏発症日",
    "comp_bile_leak_tx": "胆汁漏治療",
    "comp_duodenal_stump_leak_date": "十二指腸断端瘻発症日",
    "comp_duodenal_stump_leak_tx": "十二指腸断端瘻治療",
    "comp_chylothorax_date": "乳び胸発症日",
    "comp_chylothorax_tx": "乳び胸治療",
    "comp_cardiac_date": "心合併症発症日",
    "comp_cardiac_tx": "心合併症治療",
    "comp_delirium_date": "せん妄発症日",
    "comp_delirium_tx": "せん妄治療",
    "comp_dvt_date": "DVT発症日",
    "comp_dvt_tx": "DVT治療",
    "comp_dvt_pe_date": "DVTPE発症日",
    "comp_dvt_pe_tx": "DVTPE治療",
    "comp_pe_date": "PE発症日",
    "comp_pe_tx": "PE治療",
    "comp_anastomotic_stricture_date": "吻合部狭窄発症日",
    "comp_anastomotic_stricture_tx": "吻合部狭窄治療",
    "comp_anastomotic_bleeding_date": "吻合部出血発症日",
    "comp_anastomotic_bleeding_tx": "吻合部出血治療",
    "comp_perforation_date": "穿孔発症日",
    "comp_perforation_tx": "穿孔治療",
    "comp_cholelithiasis_date": "胆石症発症日",
    "comp_cholelithiasis_tx": "胆石症治療",
    "comp_empyema_date": "膿胸発症日",
    "comp_empyema_tx": "膿胸治療",
    "comp_pneumothorax_date": "気胸発症日",
    "comp_pneumothorax_tx": "気胸治療",
    "comp_ards_date": "ARDS発症日",
    "comp_ards_tx": "ARDS治療",
    "comp_dic_date": "DIC発症日",
    "comp_dic_tx": "DIC治療",
    "comp_sepsis_date": "敗血症発症日",
    "comp_sepsis_tx": "敗血症治療",
    "comp_septic_shock_date": "敗血症性ショック発症日",
    "comp_septic_shock_tx": "敗血症性ショック治療",
    "comp_renal_failure_date": "腎不全発症日",
    "comp_renal_failure_tx": "腎不全治療",
    "comp_hepatic_failure_date": "肝不全発症日",
    "comp_hepatic_failure_tx": "肝不全治療",
    "comp_uti_date": "尿路感染発症日",
    "comp_uti_tx": "尿路感染治療",
    "comp_atelectasis_date": "無気肺発症日",
    "comp_atelectasis_tx": "無気肺治療",
    # --- 病理（追加分） ---
    "p_macroscopic_type": "肉眼型_p",
    "p_type0_subclass": "0型亜分類_p",
    "p_size_major_mm": "腫瘍長径_mm_p",
    "p_size_minor_mm": "腫瘍短径_mm_p",
    "p_tumor_number": "腫瘍個数_p",
    "p_location_long": "長軸局在_p",
    "p_location_short": "短軸局在_p",
    "p_location_egj": "EGJ局在_p",
    "p_egj_distance_mm": "EGJ距離_mm_p",
    "p_esoph_invasion_mm": "食道浸潤距離_mm_p",
    "p_dm": "DM_p",
    "p_dm_mm": "DM距離_mm",
    "p_pm": "PM_p",
    "p_pm_mm": "PM距離_mm",
    "p_histology2": "病理組織型2",
    "p_histology3": "病理組織型3",
    "p_chemo_effect": "化学療法効果判定",
    "p_cytology": "腹腔洗浄細胞診",
    "p_distant_metastasis": "pM",
    "p_ln_chemo_effect": "リンパ節化学療法効果",
    "p_peritoneal": "p腹膜転移",
    "p_liver": "p肝転移",
    "p_inv_pancreas": "p浸潤_膵",
    "p_inv_liver": "p浸潤_肝",
    "p_inv_transverse_colon": "p浸潤_横行結腸",
    "p_inv_spleen": "p浸潤_脾",
    "p_inv_diaphragm": "p浸潤_横隔膜",
    "p_inv_esophagus": "p浸潤_食道",
    "p_inv_duodenum": "p浸潤_十二指腸",
    "p_inv_aorta": "p浸潤_大動脈",
    "p_inv_abdominal_wall": "p浸潤_腹壁",
    "p_inv_adrenal": "p浸潤_副腎",
    "p_inv_kidney": "p浸潤_腎",
    "p_inv_small_intestine": "p浸潤_小腸",
    "p_inv_retroperitoneum": "p浸潤_後腹膜",
    "p_inv_transverse_mesocolon": "p浸潤_横行結腸間膜",
    "p_inv_other": "p浸潤_その他",
    "p_inv_other_detail": "p浸潤_その他詳細",
    "p_inv_confirmed": "p浸潤確認済",
    "p_meta_peritoneal": "p転移_腹膜",
    "p_meta_liver": "p転移_肝",
    "p_meta_lung": "p転移_肺",
    "p_meta_lymph_node": "p転移_リンパ節",
    "p_meta_bone": "p転移_骨",
    "p_meta_brain": "p転移_脳",
    "p_meta_ovary": "p転移_卵巣",
    "p_meta_adrenal": "p転移_副腎",
    "p_meta_skin": "p転移_皮膚",
    "p_meta_marrow": "p転移_骨髄",
    "p_meta_pleura": "p転移_胸膜",
    "p_meta_meninges": "p転移_髄膜",
    "p_meta_other": "p転移_その他",
    "p_meta_other_detail": "p転移_その他詳細",
    "p_meta_confirmed": "p転移確認済",
    "pdl1_status": "PD-L1ステータス",
    "pdl1_tps": "PD-L1_TPS",
    # --- リンパ節（追加分） ---
    "ln_1_l": "No1_郭清数",
    "ln_1_m": "No1_転移数",
    "ln_5_l": "No5_郭清数",
    "ln_5_m": "No5_転移数",
    "ln_11p_l": "No11p_郭清数",
    "ln_11p_m": "No11p_転移数",
    "ln_19_l": "No19_郭清数",
    "ln_19_m": "No19_転移数",
    # --- 術後化学療法（追加分） ---
    "adj_start_date": "術後化療開始日",
    "adj_courses": "術後化療コース数",
    "adj_regimen_other": "術後化療レジメンその他",
    "adj_completion": "術後化療完遂",
    "adj_adverse_event": "術後化療有害事象",
    # --- 転帰（追加分） ---
    "recurrence_date": "再発日",
    "rec_local": "再発_局所",
    "rec_peritoneal": "再発_腹膜",
    "rec_liver": "再発_肝",
    "rec_lung": "再発_肺",
    "rec_lymph_node": "再発_リンパ節",
    "rec_bone": "再発_骨",
    "rec_brain": "再発_脳",
    "rec_ovary": "再発_卵巣",
    "rec_adrenal": "再発_副腎",
    "rec_other": "再発_その他",
    "rec_other_detail": "再発その他詳細",
    "rec_confirmed": "再発確認済",
    "death_date": "死亡日",
    "death_cause": "死因",
    "death_cause_other": "死因その他",
    "last_alive_date": "最終生存確認日",
    "outcome_detail": "転帰詳細",
    # --- 放射線療法 ---
    "rt_yn": "放射線療法_有無",
    "rt_purpose": "放射線目的",
    "rt_purpose_other": "放射線目的その他",
    "rt_start_date": "放射線開始日",
    "rt_total_dose_gy": "総線量_Gy",
    "rt_fractions": "分割回数",
    "rt_planning": "照射計画",
    "rt_planning_other": "照射計画その他",
    "rt_device": "照射装置",
    "rt_target_volume1": "照射野1",
    "rt_target_volume2": "照射野2",
    "rt_target_volume_other": "照射野その他",
    "rt_combination": "放射線併用薬",
    "rt_combination_other": "放射線併用薬その他",
    "rt_completion": "放射線完遂",
    "rt_adverse_event": "放射線有害事象",
    "rt_prophylactic_ln": "予防リンパ節照射",
    # --- 腫瘍マーカー ---
    "cea": "CEA",
    "ca199": "CA19-9",
    "ca125": "CA125",
    "afp": "AFP",
    "p53_antibody": "p53抗体",
    "cyfra": "シフラ",
    "scc_ag": "SCC抗原",
    "kl6": "KL-6",
    "measurement_date": "測定日",
    "timing": "タイミング",
    "locked_by_phase": "Phase確定",
    "notes": "備考",
    # --- GIST ---
    "gist_mitosis": "GIST核分裂像",
    "gist_fletcher": "GISTフレッチャー分類",
    "gist_kit": "GIST_KIT",
    "gist_cd34": "GIST_CD34",
    "gist_s100": "GIST_S100",
    "gist_desmin": "GIST_デスミン",
    "gist_rupture": "GIST破裂",
    # --- エクスポート統合（追加分） ---
    "c_histology": "c組織型",
    "drain_other1": "その他ドレーン1",
    "drain_other1_date": "その他ドレーン1抜去日",
    "drain_other2": "その他ドレーン2",
    "drain_other2_date": "その他ドレーン2抜去日",
}


import re as _re


def get_column_label(col_name: str) -> str:
    """カラム名を解析用日本語ラベルに変換する。未登録ならカラム名をそのまま返す。"""
    return COLUMN_LABELS.get(col_name, col_name)


def get_form_label(col_name: str) -> str:
    """カラム名をフォーム表示用ラベルに変換する。
    解析用ラベルの末尾 _単位 を (単位) に自動変換して可読性を高める。
    例: 身長_cm → 身長(cm), 手術時間_min → 手術時間(min)
    """
    lbl = COLUMN_LABELS.get(col_name, col_name)
    lbl = _re.sub(r'_([a-zA-Z%]+)$', r'(\1)', lbl)
    return lbl


def get_all_column_labels() -> dict:
    """全カラムラベル辞書を返す。"""
    return COLUMN_LABELS.copy()


# ============================================================
# コードブック取得ヘルパー（app.py で使用）
# ============================================================
def get_codebook(field_name, version_id=None):
    """
    field_name に対応する選択肢を {code: label} の OrderedDict で返す。
    version_id 指定がなければ version_id IS NULL のレコードを返す。
    """
    from collections import OrderedDict
    with get_db() as conn:
        if version_id is None:
            rows = conn.execute(
                """SELECT code, label FROM codebook
                   WHERE field_name=? AND (version_id IS NULL) AND is_active=1
                   ORDER BY sort_order""",
                (field_name,),
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT code, label FROM codebook
                   WHERE field_name=? AND version_id=? AND is_active=1
                   ORDER BY sort_order""",
                (field_name, version_id),
            ).fetchall()
        # version_id指定で見つからなければfallback to NULL
        if not rows and version_id is not None:
            rows = conn.execute(
                """SELECT code, label FROM codebook
                   WHERE field_name=? AND version_id IS NULL AND is_active=1
                   ORDER BY sort_order""",
                (field_name,),
            ).fetchall()
    return OrderedDict((row["code"], row["label"]) for row in rows)


def get_codebook_with_en(field_name, version_id=None):
    """code → (label, label_en) を返す。エクスポート用。"""
    from collections import OrderedDict
    with get_db() as conn:
        if version_id is None:
            rows = conn.execute(
                """SELECT code, label, label_en FROM codebook
                   WHERE field_name=? AND (version_id IS NULL) AND is_active=1
                   ORDER BY sort_order""",
                (field_name,),
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT code, label, label_en FROM codebook
                   WHERE field_name=? AND version_id=? AND is_active=1
                   ORDER BY sort_order""",
                (field_name, version_id),
            ).fetchall()
    return OrderedDict((row["code"], (row["label"], row["label_en"])) for row in rows)


# ============================================================
# メイン
# ============================================================
if __name__ == "__main__":
    # DB が未初期化なら先に init_db() を呼ぶ
    init_db()
    reset = "--reset" in sys.argv
    populate_codebook(reset=reset)
