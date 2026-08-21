# -*- coding: utf-8 -*-
"""
app.py — لوحة التحكم التفاعلية المتقدمة لخط البيانات الهجين وتحليلات الـ 30 مليون سجل
Advanced Interactive Web Dashboard for Hybrid ELT Pipeline & 30M PySpark Analytics
مقرر البيانات الضخمة (العملي) | جامعة الرازي | إشراف: م. عمر أبو سند
الطالب: زياد (السنة الرابعة)
"""

import os
import sys
import json
import time
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pymongo import MongoClient

from config.settings import (
    MONGO_URI, DB_NAME, RAW_COLLECTION, VALIDATED_COLLECTION,
    QUARANTINE_COLLECTION, SMALL_FILE_THRESHOLD_MB
)

st.set_page_config(
    page_title="منصة خط البيانات الضخمة | Hybrid ELT Pipeline",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Cairo:wght@400;600;700;900&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Cairo', sans-serif !important;
    }
    
    .stApp {
        background: linear-gradient(135deg, #0f172a 0%, #1e1b4b 50%, #0f172a 100%);
        color: #f8fafc;
        direction: rtl;
        text-align: right;
    }
    
    /* منع تشويه الرسوم البيانية لـ Plotly */
    .js-plotly-plot, .plotly, [data-testid="stPlotlyChart"] {
        direction: ltr !important;
        text-align: left !important;
    }
    
    .metric-card {
        background: rgba(30, 41, 59, 0.7);
        border: 1px solid rgba(148, 163, 184, 0.15);
        border-radius: 16px;
        padding: 20px;
        backdrop-filter: blur(12px);
        box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);
        text-align: center;
        transition: transform 0.3s ease, border-color 0.3s ease;
    }
    .metric-card:hover {
        transform: translateY(-4px);
        border-color: #6366f1;
    }
    
    .metric-value {
        font-size: 2.2rem;
        font-weight: 900;
        background: linear-gradient(90deg, #38bdf8, #818cf8);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    
    .metric-label {
        font-size: 0.95rem;
        color: #94a3b8;
        margin-top: 4px;
    }
    
    .badge-success {
        background: rgba(34, 197, 94, 0.2);
        color: #4ade80;
        border: 1px solid #22c55e;
        padding: 4px 12px;
        border-radius: 20px;
        font-weight: 700;
        font-size: 0.85rem;
        display: inline-block;
    }

    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background-color: rgba(15, 23, 42, 0.6);
        padding: 8px;
        border-radius: 12px;
    }
    
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px;
        padding: 10px 20px;
        color: #94a3b8;
        font-weight: 600;
    }
    
    .stTabs [aria-selected="true"] {
        background-color: #4f46e5 !important;
        color: white !important;
    }
</style>
""", unsafe_allow_html=True)

# قاموس ترجمة رموز الأخطاء لأسماء عربية واضحة
ERROR_LABELS = {
    "MISSING_CUSTOMER_ID": "معرف العميل مفقود (MISSING_CUSTOMER_ID)",
    "CORRUPTED_ITEMS_JSON": "كود المنتجات تالف (CORRUPTED_ITEMS_JSON)",
    "INVALID_PAYMENT_STATUS": "حالة دفع غير مسموحة (INVALID_PAYMENT_STATUS)",
    "INVALID_NUMERIC": "مبلغ غير صالح (INVALID_NUMERIC)",
    "IMPOSSIBLE_DATE": "تاريخ مستحيل/تالف (IMPOSSIBLE_DATE)",
    "MISSING_ORDER_ID": "معرف الطلب مفقود (MISSING_ORDER_ID)",
    "INVALID_STATUS": "حالة طلب غير مسموحة (INVALID_STATUS)",
    "INVALID_CURRENCY": "عملة غير مسموحة (INVALID_CURRENCY)",
    "INVALID_PHONE": "رقم هاتف غير مطابق (INVALID_PHONE)",
    "UNKNOWN_PRICE": "سعر مجهول (UNKNOWN_PRICE)",
    "EMPTY_ITEMS": "قائمة منتجات فارغة (EMPTY_ITEMS)",
    "SYMBOLIC_VALUE": "قيمة رمزية مجهولة ??? (SYMBOLIC_VALUE)",
    "INVALID_QUANTITY": "كمية سالبة أو صفرية (INVALID_QUANTITY)",
    "CORRUPTED_EMAIL": "بريد إلكتروني تالف (CORRUPTED_EMAIL)",
    "MISSING_NUMERIC": "حقل رقمي مفقود (MISSING_NUMERIC)",
}

@st.cache_resource
def get_mongo_db():
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        client.admin.command('ping')
        return client[DB_NAME]
    except Exception:
        return None

def load_json_report(filename):
    p = PROJECT_ROOT / "reports" / filename
    if p.exists():
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    return None

db = get_mongo_db()

with st.sidebar:
    st.markdown("### ⚡ منصة خط البيانات الضخمة")
    st.markdown("**مقرر البيانات الضخمة (العملي)**\n\nجامعة الرازي | إشراف: **م. عمر أبو سند**\n\nالطالب: **زياد (السنة الرابعة)**")
    
    st.markdown("---")
    
    if db is not None:
        st.markdown("<span class='badge-success'>🟢 متصل بـ MongoDB محلياً</span>", unsafe_allow_html=True)
    else:
        st.error("🔴 غير متصل بـ MongoDB")
        
    st.markdown("---")
    st.markdown("#### 🚀 التحكم السريع في خط البيانات")
    
    if st.button("⚡ تشغيل خط البيانات الآن (ELT Pipeline)", use_container_width=True):
        with st.spinner("جاري تشغيل خط البيانات والمعالجة التدفقية..."):
            from src.elt_pipeline import run as run_elt
            target_csv = PROJECT_ROOT / "data" / "sample_orders.csv"
            rep = run_elt(str(target_csv))
            st.success(f"✅ اكتمل التشغيل في {rep['elapsed_seconds']:.2f} ثانية!")
            st.rerun()

    st.markdown("---")
    st.info("💡 **المستودع السحابي:**\n\n[github.com/ziad0110/big-data-pipeline](https://github.com/ziad0110/big-data-pipeline)")

st.markdown("""
<div style='text-align: center; margin-bottom: 25px;'>
    <h1 style='font-size: 2.3rem; font-weight: 900; background: linear-gradient(90deg, #38bdf8, #818cf8, #c084fc); -webkit-background-clip: text; -webkit-text-fill-color: transparent;'>
        لوحة التحكم التفاعلية لخط البيانات الهجين (Hybrid ELT Platform)
    </h1>
    <p style='color: #94a3b8; font-size: 1.05rem;'>
        معالجة وتدقيق فواتير المتاجر الإلكترونية مع التحليل الموزع لـ 30 مليون سجل بـ Apache Spark
    </p>
</div>
""", unsafe_allow_html=True)

tab1, tab2, tab3, tab4 = st.tabs([
    "📊 مراقبة خط البيانات (Live Pipeline Monitor)",
    "⚡ تحليلات الـ 30 مليون سجل (PySpark 30M Analytics)",
    "🛡️ مركز جودة البيانات وسلة العزل (Quality & Quarantine)",
    "🏗️ المعمارية ومحاكي التوجيه (Architecture & Router)"
])

# =============================================================================
# TAB 1: مراقبة خط البيانات الحية ومجموعات MongoDB
# =============================================================================
with tab1:
    results_report = load_json_report("results.json")
    
    if results_report:
        c1, c2, c3, c4, c5 = st.columns(5)
        with c1:
            st.markdown(f"<div class='metric-card'><div class='metric-value'>{results_report.get('total_raw_records', 0):,}</div><div class='metric-label'>📥 إجمالي السجلات الخام</div></div>", unsafe_allow_html=True)
        with c2:
            st.markdown(f"<div class='metric-card'><div class='metric-value'>{results_report.get('valid_records', 0):,}</div><div class='metric-label'>✅ سجلات سليمة 100%</div></div>", unsafe_allow_html=True)
        with c3:
            st.markdown(f"<div class='metric-card'><div class='metric-value'>{results_report.get('corrected_records', 0):,}</div><div class='metric-label'>🛠️ مصححة (Audit Trail)</div></div>", unsafe_allow_html=True)
        with c4:
            st.markdown(f"<div class='metric-card'><div class='metric-value'>{results_report.get('quarantined_records', 0):,}</div><div class='metric-label'>⛔ معزولة في Quarantine</div></div>", unsafe_allow_html=True)
        with c5:
            st.markdown(f"<div class='metric-card'><div class='metric-value'>{results_report.get('throughput_records_per_sec', 0):,.1f}</div><div class='metric-label'>⚡ السرعة (سجل/ثانية)</div></div>", unsafe_allow_html=True)
            
        st.markdown("<br>", unsafe_allow_html=True)
        
        raw_t = results_report.get('total_raw_records', 0)
        v_t = results_report.get('valid_records', 0)
        c_t = results_report.get('corrected_records', 0)
        q_t = results_report.get('quarantined_records', 0)
        is_ok = (raw_t == v_t + c_t + q_t)
        
        if is_ok:
            st.success(f"✅ **فحص الاتساق الرياضي الحتمي (Consistency Check): PASSED** — إجمالي السجلات ({raw_t:,}) = السليمة ({v_t:,}) + المصححة ({c_t:,}) + المعزولة ({q_t:,})")
        else:
            st.error("❌ فشل فحص الاتساق الرياضي!")
            
    st.markdown("### 🗄️ إحصائيات مجموعات MongoDB الحية")
    
    if db is not None:
        col_raw = db[RAW_COLLECTION].count_documents({})
        col_val = db[VALIDATED_COLLECTION].count_documents({})
        col_qua = db[QUARANTINE_COLLECTION].count_documents({})
        
        m1, m2, m3 = st.columns(3)
        with m1:
            st.info(f"**`orders_raw` (البيانات الخام):**\n\n### `{col_raw:,}` مستند")
        with m2:
            st.success(f"**`orders_validated` (البيانات النظيفة):**\n\n### `{col_val:,}` مستند فريد")
        with m3:
            st.warning(f"**`orders_quarantine` (سلة العزل):**\n\n### `{col_qua:,}` مستند معزول")
            
        st.markdown("#### 🔍 مستعرض المستندات المباشر من MongoDB")
        target_coll = st.selectbox("اختر المجموعة للعرض الفوري:", [VALIDATED_COLLECTION, RAW_COLLECTION, QUARANTINE_COLLECTION])
        
        docs = list(db[target_coll].find().limit(3))
        if docs:
            for d in docs:
                if '_id' in d:
                    d['_id'] = str(d['_id'])
            st.json(docs)
    else:
        st.warning("تعذر الاتصال بـ MongoDB محلياً لعرض السجلات الحية.")

# =============================================================================
# TAB 2: تحليلات الـ 30 مليون سجل بـ Apache Spark
# =============================================================================
with tab2:
    st.markdown("### ⚡ مركز تحليلات البيانات الضخمة (30 Million Records Analysis via PySpark)")
    st.markdown("يقوم هذا القسم بقراءة ملف الـ **13.26 GB** كاملاً في الذاكرة العشوائية عبر محرك **Apache Spark** بتوازي 16 بارتشن واستخراج المؤشرات الإحصائية والتجارية الكبرى دون حفظها في القرص.")
    
    spark_rep = load_json_report("spark_analysis_30m.json")
    
    col_btn, col_info = st.columns([1, 2])
    with col_btn:
        run_30m = st.button("🔥 تشغيل تحليل ملف الـ 13.26 GB كاملاً الآن بـ PySpark", use_container_width=True)
        
    if run_30m:
        with st.spinner("جاري قراءة وتجميع الـ 13.26 جيجابايت كاملاً عبر Apache Spark (قد يستغرق 1-3 دقائق)..."):
            from src.spark_analyzer import analyze_dataset
            huge_csv = PROJECT_ROOT.parent / "big data" / "orders_huge_mixed_quality.csv"
            spark_rep = analyze_dataset(str(huge_csv))
            st.success("✅ اكتمل تحليل البيانات الضخمة بنجاح تام!")
            st.rerun()
                
    if spark_rep:
        k1, k2, k3, k4 = st.columns(4)
        with k1:
            st.markdown(f"<div class='metric-card'><div class='metric-value'>{spark_rep.get('total_records', 0):,}</div><div class='metric-label'>📦 إجمالي السجلات المحللة</div></div>", unsafe_allow_html=True)
        with k2:
            gmv = spark_rep.get('financial_summary', {}).get('estimated_total_gmv_yer', 0)
            st.markdown(f"<div class='metric-card'><div class='metric-value'>{gmv:,.0f} YER</div><div class='metric-label'>💰 إجمالي المبيعات التقديرية (GMV)</div></div>", unsafe_allow_html=True)
        with k3:
            aov = spark_rep.get('financial_summary', {}).get('average_order_value_yer', 0)
            st.markdown(f"<div class='metric-card'><div class='metric-value'>{aov:,.0f} YER</div><div class='metric-label'>🏷️ متوسط قيمة الطلب (AOV)</div></div>", unsafe_allow_html=True)
        with k4:
            st.markdown(f"<div class='metric-card'><div class='metric-value'>{spark_rep.get('num_spark_partitions', 16)}</div><div class='metric-label'>⚙️ عدد الـ Partitions المتوازية</div></div>", unsafe_allow_html=True)
            
        st.markdown("<br>", unsafe_allow_html=True)
        
        g1, g2 = st.columns(2)
        with g1:
            cities_data = spark_rep.get('top_cities', {})
            if cities_data:
                df_cities = pd.DataFrame(list(cities_data.items()), columns=['المدينة', 'عدد الطلبات'])
                fig_city = px.bar(
                    df_cities, x='المدينة', y='عدد الطلبات',
                    title='🏙️ توزيع الطلبات عبر المدن اليمنية (Top Cities)',
                    color='عدد الطلبات',
                    color_continuous_scale='Viridis',
                    template='plotly_dark'
                )
                fig_city.update_layout(font_family="Cairo", margin=dict(l=40, r=40, t=50, b=40))
                st.plotly_chart(fig_city, use_container_width=True)
                
        with g2:
            pm_data = spark_rep.get('payment_methods', {})
            if pm_data:
                df_pm = pd.DataFrame(list(pm_data.items()), columns=['طريقة الدفع', 'العدد'])
                fig_pm = px.pie(
                    df_pm, names='طريقة الدفع', values='العدد',
                    title='💳 الحصص السوقية لطرق الدفع (Payment Methods)',
                    hole=0.45,
                    template='plotly_dark'
                )
                fig_pm.update_layout(font_family="Cairo", margin=dict(l=40, r=40, t=50, b=40))
                st.plotly_chart(fig_pm, use_container_width=True)
                
        g3, g4 = st.columns(2)
        with g3:
            status_data = spark_rep.get('status_distribution', {})
            if status_data:
                df_st = pd.DataFrame(list(status_data.items()), columns=['الحالة', 'العدد'])
                fig_st = px.pie(
                    df_st, names='الحالة', values='العدد',
                    title='📋 توزيع حالات الطلبات (Order Statuses)',
                    template='plotly_dark'
                )
                fig_st.update_layout(font_family="Cairo", margin=dict(l=40, r=40, t=50, b=40))
                st.plotly_chart(fig_st, use_container_width=True)
                
        with g4:
            deliv_data = spark_rep.get('delivery_types', {})
            if deliv_data:
                df_del = pd.DataFrame(list(deliv_data.items()), columns=['نوع التوصيل', 'العدد'])
                fig_del = px.bar(
                    df_del, x='نوع التوصيل', y='العدد',
                    title='🚚 توزيع نوع التوصيل (سريع مقابل عادي)',
                    color='نوع التوصيل',
                    template='plotly_dark'
                )
                fig_del.update_layout(font_family="Cairo", margin=dict(l=40, r=40, t=50, b=40))
                st.plotly_chart(fig_del, use_container_width=True)
    else:
        st.warning("⚠️ **لم يتم تشغيل تحليل ملف الـ 13.26 GB بعد.**\n\nاضغط على الزر أعلاه: **🔥 تشغيل تحليل ملف الـ 13.26 GB كاملاً الآن بـ PySpark** لتشغيل محرك Apache Spark الموزع واستخراج الأرقام والإحصائيات الحقيقية أمامك!")

# =============================================================================
# TAB 3: مركز جودة البيانات وسلة العزل ومستكشف الـ Audit Trail
# =============================================================================
with tab3:
    st.markdown("### 🛡️ مركز جودة البيانات وسلة العزل (Data Quality & Quarantine)")
    
    results_report = load_json_report("results.json")
    if results_report and 'error_breakdown' in results_report:
        err_dict = results_report['error_breakdown']
        
        # تحويل الرموز الإنجليزية لأسماء عربية واضحة ومقروءة
        formatted_errs = []
        for k, v in err_dict.items():
            label = ERROR_LABELS.get(k, k)
            formatted_errs.append({"الخطأ": label, "العدد": v})
            
        df_err = pd.DataFrame(formatted_errs).sort_values("العدد", ascending=True)
        
        fig_err = px.bar(
            df_err, x="العدد", y="الخطأ",
            orientation="h",
            title="⛔ أكثر الأخطاء تكراراً في سلة العزل (Top Quarantine Reasons)",
            color="العدد",
            color_continuous_scale="Reds",
            template="plotly_dark"
        )
        fig_err.update_layout(
            font_family="Cairo",
            margin=dict(l=280, r=40, t=50, b=40),
            yaxis=dict(autorange="reversed")
        )
        st.plotly_chart(fig_err, use_container_width=True)
        
    st.markdown("---")
    st.markdown("### 🔬 مستكشف سجل التدقيق (Audit Trail Inspector)")
    st.markdown("ابحث عن أي فاتورة تم تصحيحها وشاهد الفروقات الدقيقة بين القيم الأصلية والقيم المنظفة:")
    
    if db is not None:
        corrected_samples = list(db[VALIDATED_COLLECTION].find({"corrections": {"$exists": True}}).limit(10))
        if corrected_samples:
            order_ids = [doc['order_id'] for doc in corrected_samples]
            chosen_order = st.selectbox("اختر رقم طلب مصحح لفحصه:", order_ids)
            
            selected_doc = next(d for d in corrected_samples if d['order_id'] == chosen_order)
            
            st.markdown(f"#### تفاصيل التدقيق للطلب: `{chosen_order}`")
            
            corrs = selected_doc.get('corrections', [])
            if corrs:
                df_corr = pd.DataFrame(corrs)
                st.table(df_corr)
                
            st.markdown("**المستند المنظف النهائي:**")
            if '_id' in selected_doc:
                selected_doc['_id'] = str(selected_doc['_id'])
            st.json(selected_doc)
    else:
        st.warning("يرجى تشغيل MongoDB لعرض سجلات التدقيق.")

# =============================================================================
# TAB 4: المعمارية ومحاكي موجه الملفات (Architecture & Router Simulator)
# =============================================================================
with tab4:
    st.markdown("### 🏗️ معمارية خط البيانات الهجين ومحاكي الـ File Router")
    
    st.markdown("#### 🎛️ محاكي التوجيه التفاعلي (Live Engine Decision Simulator)")
    sim_size = st.slider("حرّك المؤشر لاختيار حجم ملف افتراضي (بالميجابايت):", min_value=1, max_value=15000, value=40, step=10)
    
    if sim_size <= SMALL_FILE_THRESHOLD_MB:
        st.success(f"🚀 **القرار:** تم اختيار **`python_batch`** (لأن الحجم {sim_size:,} MB <= {SMALL_FILE_THRESHOLD_MB} MB) — أسرع وأخف بدون تكلفة إقلاع سبارك!")
    else:
        st.warning(f"⚡ **القرار:** تم اختيار **`pyspark`** (لأن الحجم {sim_size:,} MB > {SMALL_FILE_THRESHOLD_MB} MB) — توزيع تلقائي على 16 بارتشن متوازي!")
        
    st.markdown("---")
    st.markdown("### 📐 ملخص القواعد العشر ومعادلة الاتساق")
    
    col_r1, col_r2 = st.columns(2)
    with col_r1:
        st.markdown('''
        **قواعد التنظيف والتصحيح:**
        1. `ARABIC_NUMERALS`: تحويل الأرقام المشرقية `٠-٩`.
        2. `CURRENCY_TEXT`: توحيد العملة النصية إلى `YER`.
        3. `THOUSAND_SEPARATOR`: إزالة فواصل الآلاف من الأرقام.
        4. `PRICE_WORDS`: تحويل الكلمات العربية إلى أرقام.
        5. `PHONE_FORMAT`: تنظيف وفحص رقم الهاتف اليمني.
        ''')
    with col_r2:
        st.markdown('''
        **قواعد الموثوقية والعزل:**
        6. `EMAIL_FIX`: إصلاح `@@` وعزل الإيميل التالف.
        7. `DATE_NORMALIZE`: توحيد التواريخ لـ ISO وعزل المستحيلة.
        8. `TRIM_NORMALIZE`: إزالة المسافات وتوحيد مرادفات الحالات.
        9. `TOTAL_RECALC`: إعادة حساب المجموع من المنتجات والتوصيل.
        10. `SYMBOLIC_VALUES`: عزل المعرفات المفقودة و `???` و `not-json`.
        ''')
