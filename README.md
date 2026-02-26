# Replication (Vietnam) — Corporate Political Connections & Environmental Outcomes

This repository contains a minimal Vietnam-based replication of the paper **“Corporate Political Connections and Favorable Environmental Regulatory Enforcement”** (Heitz, Wang, and Wang).  
Goal (per advisor request): **reproduce Table 1 and Table 2** in the spirit of the original paper, using feasible Vietnam proxies.

- **Political connection proxy (replacement for WinRatio):** SOE-based measures (`SOE_final`, robustness `SOE_share10`)
- **Environmental outcome proxy (replacement for firm-level penalties):** annual **PM2.5** assigned by headquarters **province-year**
- **Deliverables:** Table 1 (summary stats), Table 2 (baseline regressions), plus an exploratory DiD appendix.

---

## For quick review (recommended reading order)

1. **`Replication Memo.pdf`** — short memo following the original paper structure (Introduction → Background → Identification → Data → Empirics).
2. **`stata/output/Table1_SummaryStats.xlsx`** — Table 1 (Panels A–C).
3. **`stata/output/Table2_Baseline.xlsx`** — Table 2 (4 columns, FE/cluster, robustness with `SOE_share10`).
4. *(Optional/Appendix)* **`stata/output/Table_DiD_LEP2022_province_intensity.xlsx`** — exploratory province-year DiD around 2022 policy milestone.
5. **Logs (reproducibility):**
   - `2_clean/log_07_merge_pm25.txt`
   - `2_clean/log_07d_soe_broad_stats.txt`
   - `stata/output/log_10b_did_lep2022.txt`
6. **Code (pipeline entry points):**
   - `code_python/07_merge_pm25_into_panel.py`
   - `code_python/07d_make_soe_broad_and_stats.py`
   - `code_python/08_table1_summary.py`
   - `code_python/09_table2_baseline_regs.py`
   - `code_python/10b_did_lep2022_province_intensity.py`

---

## Repository structure (high level)

- `0_input/` — small inputs (e.g., boundaries metadata, sample list)
- `1_raw/` — raw downloads/scrapes (CAFEF + PM2.5 GeoTIFFs)
- `2_clean/` — cleaned datasets + merge logs
- `code_python/` — end-to-end pipeline scripts
- `stata/output/` — final tables (Excel) + DiD outputs/logs
- `session_info.txt` — Python version + `pip freeze`

---

## How to reproduce the tables (quick)

> Run these from the project root: `pipeline_vn_data_1week/`

### 1) Install dependencies
```bash
pip install -r requirements.txt
```
### 2) Merge PM2.5 into the firm panel

```bash
python code_python/07_merge_pm25_into_panel.py > 2_clean/log_07_merge_pm25.txt 2>&1
```
### 3) Build SOE “broad” variables (used for robustness / diagnostics)
```bash
python code_python/07d_make_soe_broad_and_stats.py > 2_clean/log_07d_soe_broad_stats.txt 2>&1
```
### 4) Create Table 1 (Summary Statistics)
```bash
python code_python/08_table1_summary.py
```
Expected output:
`stata/output/Table1_SummaryStats.xlsx`

### 5) Create Table 2 (Baseline Regressions)
```bash
python code_python/09_table2_baseline_regs.py
```
Expected output:
`stata/output/Table2_Baseline.xlsx`

### Optional: Exploratory DiD appendix (province-year intensity)
This appendix runs a province-year DiD where treatment intensity is the **pre-2022 share of “high” pollution-group firms** in each province.
```bash
python code_python/10b_did_lep2022_province_intensity.py > stata/output/log_10b_did_lep2022.txt 2>&1
```
Expected output:
`stata/output/Table_DiD_LEP2022_province_intensity.xlsx`

---

## Notes on proxies and interpretation
- **WinRatio (paper) → SOE proxy (Vietnam)**: Vietnam lacks a close-election PAC setting to reconstruct WinRatio, so SOE-based proxies are used.

- **EPA enforcement outcomes → PM2.5**: Firm-level environmental penalty microdata are not available in a comparable format; PM2.5 is used as an outcome proxy.

- **Fixed effects / SE**: Table 2 includes year FE and (in some specs) province FE, with standard errors clustered at the firm (ticker) level.

- Results should be interpreted as **baseline associations** under controls/FE, not as causal RDD estimates.

---

## Environment
See `session_info.txt` for Python and package versions.