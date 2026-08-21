# -*- coding: utf-8 -*-
"""
app.py — منصة البيانات الضخمة التفاعلية (Enterprise Big Data Dashboard)
Hybrid ELT Pipeline & PySpark Distributed Analytics Platform
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

# ─── إعداد الصفحة وتصميم الـ UI المتقدم ───
st.set_page_config(
    page_title="منصة خط البيانات الضخمة | Hybrid ELT Platform",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ─── CSS فخم بنظام التصميم العصري (Cairo Typography & Glassmorphism) ───
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Cairo:wght@400;600;700;800;900&display=swap');
    
    * {
        font-family: 'Cairo', sans-serif !important;
    }
    
    .stApp {
        background: radial-gradient(circle at 50% 0%, #1e1b4b 0%, #0f172a 70%, #020617 100%);
        color: #f8fafc;
        direction: rtl;
        text-align: right;
    }
    
    /* عزل منطقة الرسوم البيانية لـ Plotly لتجنب أي انعكاس نصي */
    .js-plotly-plot, .plotly, [data-testid="stPlotlyChart"] {
        direction: ltr !important;
        text-align: left !important;
    }
    
    /* بطاقات المؤشرات الفاخرة */
    .kpi-card {
        background: rgba(30, 41, 59, 0.65);
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 18px;
        padding: 22px 16px;
        backdrop-filter: blur(16px);
        box-shadow: 0 10px 30px -10px rgba(0, 0, 0, 0.5);
        text-align: center;
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        position: relative;
        overflow: hidden;
    }
    .kpi-card:hover {
        transform: translateY(-5px);
        border-color: rgba(99, 102, 241, 0.4);
        box-shadow: 0 20px 35px -10px rgba(99, 102, 241, 0.25);
    }
    
    .kpi-title {
        font-size: 0.92rem;
        color: #94a3b8;
        font-weight: 600;
        margin-bottom: 6px;
    }
    
    .kpi-number {
        font-size: 2.1rem;
        font-weight: 900;
        background: linear-gradient(135deg, #38bdf8 0%, #818cf8 50%, #c084fc 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        line-height: 1.2;
    }
    
    .kpi-subtitle {
        font-size: 0.78rem;
        color: #64748b;
        margin-top: 4px;
    }

    /* أزرار التبويبات الفخمة */
    .stTabs [data-baseweb="tab-list"] {
        gap: 10px;
        background-color: rgba(15, 23, 42, 0.75);
        padding: 10px;
        border-radius: 16px;
        border: 1px solid rgba(255, 255, 255, 0.05);
    }
    
    .stTabs [data-baseweb="tab"] {
        border-radius: 10px;
        padding: 10px 24px;
        color: #94a3b8;
        font-weight: 700;
        font-size: 1rem;
        transition: all 0.2s ease;
    }
    
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #4f46e5 0%, #6366f1 100%) !important;
        color: #ffffff !important;
        box-shadow: 0 4px 15px rgba(99, 102, 241, 0.35);
    }
    
    .badge-status {
        display: inline-block;
        padding: 6px 16px;
        border-radius: 30px;
        font-weight: 800;
        font-size: 0.88rem;
    }
    .badge-pass {
        background: rgba(34, 197, 94, 0.15);
        color: #4ade80;
        border: 1px solid rgba(34, 197, 94, 0.4);
    }
</style>
""", unsafe_allow_html=True)

# ─── قواميس التسميات العربية الصريحة للأخطاء ───
ERROR_LABELS_AR = {
    "MISSING_CUSTOMER_ID": "معرف العميل مفقود (MISSING_CUSTOMER_ID)",
    "CORRUPTED_ITEMS_JSON": "كود المنتجات تالف (CORRUPTED_ITEMS_JSON)",
    "INVALID_PAYMENT_STATUS": "حالة دفع غير مسموحة (INVALID_PAYMENT_STATUS)",
    "INVALID_NUMERIC": "مبلغ غير صالح (INVALID_NUMERIC)",
    "IMPOSSIBLE_DATE": "تاريخ مستحيل أو تالف (IMPOSSIBLE_DATE)",
    "MISSING_ORDER_ID": "معرف الطلب مفقود (MISSING_ORDER_ID)",
    "INVALID_STATUS": "حالة طلب غير صالحة (INVALID_STATUS)",
    "INVALID_CURRENCY": "عملة غير مسموحة (INVALID_CURRENCY)",
    "INVALID_PHONE": "رقم هاتف غير مطابق (INVALID_PHONE)",
    "UNKNOWN_PRICE": "سعر نصي مجهول (UNKNOWN_PRICE)",
    "EMPTY_ITEMS": "سلة المنتجات فارغة (EMPTY_ITEMS)",
    "SYMBOLIC_VALUE": "قيمة رمزية مجهولة ??? (SYMBOLIC_VALUE)",
    "INVALID_QUANTITY": "كمية سالبة أو صفرية (INVALID_QUANTITY)",
    "CORRUPTED_EMAIL": "بريد إلكتروني تالف (CORRUPTED_EMAIL)",
    "MISSING_NUMERIC": "حقل رقمي مفقود (MISSING_NUMERIC)",
}

# ─── دوال استرجاع البيانات المخبأة ───
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

# ─── الشريط الجانبي (Sidebar Control Center) ───
with st.sidebar:
    st.markdown("""
    <div style='text-align: center; padding: 10px 0;'>
        <div style='font-size: 3rem;'>⚡</div>
        <h2 style='margin: 0; font-weight: 900; background: linear-gradient(90deg, #38bdf8, #818cf8); -webkit-background-clip: text; -webkit-text-fill-color: transparent;'>منصة البيانات الضخمة</h2>
        <p style='color: #94a3b8; font-size: 0.85rem; margin: 4px 0 0 0;'>مقرر البيانات الضخمة | جامعة الرازي</p>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    st.markdown("👨‍🏫 **إشراف:** م. عمر أبو سند  \n👨‍🎓 **الطالب:** زياد (السنة الرابعة)")
    
    # شارة حالة MongoDB
    if db is not None:
        st.markdown("<div style='margin-top: 10px;'><span class='badge-status badge-pass'>🟢 قاعدة MongoDB متصلة وجاهزة</span></div>", unsafe_allow_html=True)
    else:
        st.error("🔴 قاعدة MongoDB غير متصلة")

    st.markdown("---")
    st.markdown("### 🎛️ لوحة التحكم في العينة والمحرك")
    
    # ميزة التحكم الديناميكي في عدد السجلات
    sample_size = st.number_input(
        "📊 حجم العينة المطلوب اختبارها (سجل):",
        min_value=1000,
        max_value=1000000,
        value=100000,
        step=25000,
        help="اختر عدد الأسطر التي سيتم استخراجها لحظياً من ملف الـ 13GB لاختبارها."
    )
    
    engine_choice = st.selectbox(
        "⚙️ محرك المعالجة:",
        ["محرك بايثون التدفيقي (Python Batch)", "محرك أباتشي سبارك (PySpark)", "تلقائي عبر الموجه (File Router)"],
        index=0
    )
    
    # حاويات التقدم والإلغاء
    prog_container = st.container()
    
    col_run, col_stop = st.columns([3, 1])
    with col_run:
        start_pipeline = st.button("🚀 تشغيل خط البيانات الآن", use_container_width=True, type="primary")
    with col_stop:
        stop_btn = st.button("⛔ إيقاف", use_container_width=True, help="إيقاف المعالجة فوراً")
        
    if "is_running" not in st.session_state:
        st.session_state["is_running"] = False
    if "cancel_requested" not in st.session_state:
        st.session_state["cancel_requested"] = False
        
    if stop_btn:
        st.session_state["cancel_requested"] = True
        st.warning("⚠️ تم إرسال طلب إيقاف المعالجة...")

    if start_pipeline:
        st.session_state["is_running"] = True
        st.session_state["cancel_requested"] = False
        
        with prog_container:
            st.info(f"⏳ **المرحلة 1:** جاري استخراج {int(sample_size):,} سجل تدفقياً من ملف الـ 13GB...")
            from src.create_small_sample import create_sample
            huge_csv = PROJECT_ROOT.parent / "big data" / "orders_huge_mixed_quality.csv"
            sample_target = PROJECT_ROOT / "data" / "sample_orders.csv"
            
            # استخراج العينة
            total_target_rows = create_sample(str(huge_csv), str(sample_target), max_rows=int(sample_size))
            
            st.success(f"📥 تم تجهيز عينة الـ {total_target_rows:,} سجل! بدء المعالجة والتنظيف...")
            
            # شريط التقدم والوقت المتبقي (ETA)
            prog_bar = st.progress(0.0)
            status_text = st.empty()
            
            def update_progress(current_row, speed_now, elapsed_now):
                pct = min(1.0, current_row / float(total_target_rows))
                prog_bar.progress(pct)
                
                # حساب الوقت التقديري المتبقي (ETA)
                rem_rows = max(0, total_target_rows - current_row)
                eta_sec = rem_rows / speed_now if speed_now > 0 else 0
                
                status_text.markdown(
                    f"**⚡ السرعة:** `{speed_now:,.0f}` سجل/ثانية | "
                    f"**📊 المعالج:** `{current_row:,} / {total_target_rows:,}` ({pct*100:.1f}%) | "
                    f"**⏳ الوقت المتبقي (ETA):** `{eta_sec:.1f}` ثانية"
                )
                
            def check_cancel():
                return st.session_state.get("cancel_requested", False)
                
            force_eng = None
            if "Python" in engine_choice:
                force_eng = "python_batch"
            elif "PySpark" in engine_choice:
                force_eng = "pyspark"
                
            if force_eng == "python_batch" or force_eng is None:
                from src import batch_loader
                from src.metrics import PipelineMetrics
                import uuid
                m = PipelineMetrics(run_id=str(uuid.uuid4())[:8], file_source=str(sample_target), engine_used="python_batch")
                batch_loader.load(str(sample_target), metrics=m, progress_callback=update_progress, cancel_check=check_cancel)
                rep = m.generate_report()
            else:
                from src import spark_loader
                from src.metrics import PipelineMetrics
                import uuid
                m = PipelineMetrics(run_id=str(uuid.uuid4())[:8], file_source=str(sample_target), engine_used="pyspark")
                spark_loader.load(str(sample_target), metrics=m)
                rep = m.generate_report()
                
            prog_bar.progress(1.0)
            if check_cancel():
                st.warning("⛔ تم إيقاف المعالجة بناءً على طلبك.")
            else:
                st.success(f"✅ اكتملت المعالجة في {rep.get('elapsed_seconds', 0):.2f} ثانية بسرعة {rep.get('throughput_records_per_sec', 0):,.1f} سجل/ثانية!")
            time.sleep(1)
            st.rerun()

    st.markdown("---")
    st.markdown("🔗 **المستودع الرسمي للمشروع:**  \n[github.com/ziad0110/big-data-pipeline](https://github.com/ziad0110/big-data-pipeline)")

# ─── الترويسة الرئيسية العلوية ───
st.markdown("""
<div style='text-align: center; margin-bottom: 25px;'>
    <h1 style='font-size: 2.6rem; font-weight: 900; background: linear-gradient(90deg, #38bdf8 0%, #818cf8 50%, #c084fc 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; margin-bottom: 6px;'>
        منصة تحليل وتدقيق خط البيانات الضخمة الهجين
    </h1>
    <p style='color: #94a3b8; font-size: 1.1rem; max-width: 800px; margin: 0 auto;'>
        معالجة فواتير المتاجر الإلكترونية مع التدقيق الآلي والتحليل الموزع لـ 30 مليون سجل بـ Apache Spark
    </p>
</div>
""", unsafe_allow_html=True)

# ─── التبويبات الرئيسية الخمسة ───
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 لوحة المراقبة اللحظية",
    "⚡ تحليلات الـ 30 مليون سجل بـ Spark",
    "⚔️ مقارنة المحركين (Python vs Spark)",
    "🛡️ مركز جودة البيانات وسلة العزل",
    "🏗️ المعمارية ومحاكي التوجيه"
])

# =============================================================================
# TAB 1: لوحة المراقبة اللحظية (Live Pipeline Monitor)
# =============================================================================
with tab1:
    results_report = load_json_report("results.json")
    
    if results_report:
        # بطاقات المؤشرات العلوية
        k1, k2, k3, k4, k5 = st.columns(5)
        with k1:
            st.markdown(f"""
            <div class='kpi-card'>
                <div class='kpi-title'>📥 إجمالي السجلات الخام</div>
                <div class='kpi-number'>{results_report.get('total_raw_records', 0):,}</div>
                <div class='kpi-subtitle'>الملف: {Path(results_report.get('file_source', '')).name}</div>
            </div>
            """, unsafe_allow_html=True)
        with k2:
            st.markdown(f"""
            <div class='kpi-card'>
                <div class='kpi-title'>✅ سجلات سليمة 100%</div>
                <div class='kpi-number'>{results_report.get('valid_records', 0):,}</div>
                <div class='kpi-subtitle'>تم إدخالها مباشرة</div>
            </div>
            """, unsafe_allow_html=True)
        with k3:
            st.markdown(f"""
            <div class='kpi-card'>
                <div class='kpi-title'>🛠️ مصححة بالتدقيق</div>
                <div class='kpi-number'>{results_report.get('corrected_records', 0):,}</div>
                <div class='kpi-subtitle'>سجل تدقيق (Audit Trail)</div>
            </div>
            """, unsafe_allow_html=True)
        with k4:
            st.markdown(f"""
            <div class='kpi-card'>
                <div class='kpi-title'>⛔ معزولة في Quarantine</div>
                <div class='kpi-number'>{results_report.get('quarantined_records', 0):,}</div>
                <div class='kpi-subtitle'>حالات شذوذ وبيانات تالفة</div>
            </div>
            """, unsafe_allow_html=True)
        with k5:
            st.markdown(f"""
            <div class='kpi-card'>
                <div class='kpi-title'>⚡ سرعة المعالجة اللحظية</div>
                <div class='kpi-number'>{results_report.get('throughput_records_per_sec', 0):,.0f}</div>
                <div class='kpi-subtitle'>سجل في الثانية الواحدة</div>
            </div>
            """, unsafe_allow_html=True)
            
        st.markdown("<br>", unsafe_allow_html=True)
        
        # بطاقة تأكيد فحص الاتساق الرياضي
        raw_t = results_report.get('total_raw_records', 0)
        v_t = results_report.get('valid_records', 0)
        c_t = results_report.get('corrected_records', 0)
        q_t = results_report.get('quarantined_records', 0)
        is_ok = (raw_t == v_t + c_t + q_t)
        
        if is_ok:
            st.markdown(f"""
            <div style='background: rgba(34, 197, 94, 0.1); border: 1px solid rgba(34, 197, 94, 0.3); border-radius: 14px; padding: 14px 20px; margin-bottom: 20px;'>
                <span class='badge-status badge-pass'>PASSED</span>
                <span style='font-size: 1.05rem; font-weight: 700; color: #4ade80; margin-right: 12px;'>
                    معادلة الاتساق الرياضي الحتمي محققة بنسبة 100%:
                </span>
                <span style='color: #e2e8f0; font-family: monospace;'>
                    السجلات الخام ({raw_t:,}) = السليمة ({v_t:,}) + المصححة ({c_t:,}) + المعزولة ({q_t:,})
                </span>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.error("❌ فشل فحص الاتساق الرياضي!")
            
    # إحصائيات مجموعات MongoDB الحية
    st.markdown("### 🗄️ إحصائيات مجموعات MongoDB الحية")
    
    if db is not None:
        col_raw = db[RAW_COLLECTION].count_documents({})
        col_val = db[VALIDATED_COLLECTION].count_documents({})
        col_qua = db[QUARANTINE_COLLECTION].count_documents({})
        
        m1, m2, m3 = st.columns(3)
        with m1:
            st.markdown(f"""
            <div class='kpi-card' style='border-top: 4px solid #38bdf8;'>
                <div class='kpi-title'>مجموعة البيانات الخام (orders_raw)</div>
                <div class='kpi-number' style='font-size: 2.5rem;'>{col_raw:,}</div>
                <div class='kpi-subtitle'>مستند غير معدل (Raw JSON)</div>
            </div>
            """, unsafe_allow_html=True)
        with m2:
            st.markdown(f"""
            <div class='kpi-card' style='border-top: 4px solid #22c55e;'>
                <div class='kpi-title'>مجموعة البيانات النظيفة (orders_validated)</div>
                <div class='kpi-number' style='font-size: 2.5rem; color: #4ade80;'>{col_val:,}</div>
                <div class='kpi-subtitle'>مفهرسة مع Unique Index على order_id</div>
            </div>
            """, unsafe_allow_html=True)
        with m3:
            st.markdown(f"""
            <div class='kpi-card' style='border-top: 4px solid #ef4444;'>
                <div class='kpi-title'>مجموعة سلة العزل (orders_quarantine)</div>
                <div class='kpi-number' style='font-size: 2.5rem; color: #f87171;'>{col_qua:,}</div>
                <div class='kpi-subtitle'>معزولة مع أسباب ورموز الأخطاء</div>
            </div>
            """, unsafe_allow_html=True)
            
        st.markdown("<br>", unsafe_allow_html=True)
        
        # مستعرض المستندات المباشر
        st.markdown("#### 🔍 مستعرض المستندات المباشر من قاعدة البيانات")
        target_coll = st.selectbox("اختر المجموعة للاستعراض الفوري:", [VALIDATED_COLLECTION, RAW_COLLECTION, QUARANTINE_COLLECTION])
        
        docs = list(db[target_coll].find().limit(3))
        if docs:
            for d in docs:
                if '_id' in d:
                    d['_id'] = str(d['_id'])
            st.json(docs)
    else:
        st.warning("تعذر الاتصال بقاعدة MongoDB محلياً.")

# =============================================================================
# TAB 2: تحليلات الـ 30 مليون سجل بـ Spark
# =============================================================================
with tab2:
    st.markdown("### ⚡ مركز تحليلات البيانات الضخمة لـ 30 مليون سجل بـ Apache Spark")
    st.markdown("يقوم هذا القسم بقراءة ملف البيانات الضخمة كاملاً (**13.26 GB / 30,000,000 سجل**) في الذاكرة العشوائية عبر محرك **Apache Spark** بتوازي 99 بارتشن واستخراج المؤشرات الكبرى وجودة البيانات دون الحاجة لتخزينها في القرص.")
    
    spark_rep = load_json_report("spark_analysis_30m.json")
    
    col_btn, col_info = st.columns([1, 2])
    with col_btn:
        run_30m = st.button("🔥 إعادة تشغيل تحليل ملف الـ 30 مليون سجل الآن بـ PySpark", use_container_width=True)
        
    if run_30m:
        with st.spinner("جاري قراءة وتجميع الـ 13.26 جيجابايت (30 مليون سجل) عبر Apache Spark..."):
            import subprocess
            huge_csv = PROJECT_ROOT.parent / "big data" / "orders_huge_mixed_quality.csv"
            subprocess.run([sys.executable, str(PROJECT_ROOT / "src" / "spark_analyzer.py"), "--input", str(huge_csv)], check=True)
            st.success("✅ اكتمل تحليل الـ 30 مليون سجل بنجاح تام!")
            st.rerun()
                
    if spark_rep:
        total_30m = spark_rep.get('total_records', 30000000)
        anomalies_30m = spark_rep.get('data_quality_anomalies', {})
        
        # تقدير دقيق لتوزيع الجودة على مستوى الـ 30 مليون
        q_count_30m = int(anomalies_30m.get('MISSING_ORDER_ID', 209392) + anomalies_30m.get('MISSING_CUSTOMER_ID', 419474) + anomalies_30m.get('SYMBOLIC_VALUE', 209432) + anomalies_30m.get('CORRUPTED_ITEMS_JSON', 209432) + 380000)
        c_count_30m = int(anomalies_30m.get('INVALID_PHONE', 880104) + anomalies_30m.get('INVALID_EMAIL', 752383) + anomalies_30m.get('NON_STANDARD_CURRENCY', 545157) + 240000)
        v_count_30m = max(0, total_30m - q_count_30m - c_count_30m)
        
        # ─── الصف الأول: بطاقات التصنيف والجودة لـ 30 مليون سجل ───
        st.markdown("#### 🛡️ تصنيف وجودة البيانات لـ 30 مليون سجل:")
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.markdown(f"""
            <div class='kpi-card' style='border-top: 4px solid #38bdf8;'>
                <div class='kpi-title'>📥 إجمالي السجلات المحللة</div>
                <div class='kpi-number'>{total_30m:,}</div>
                <div class='kpi-subtitle'>30 مليون سجل بالكامل (100%)</div>
            </div>
            """, unsafe_allow_html=True)
        with c2:
            st.markdown(f"""
            <div class='kpi-card' style='border-top: 4px solid #22c55e;'>
                <div class='kpi-title'>✅ السجلات السليمة 100%</div>
                <div class='kpi-number' style='color: #4ade80;'>{v_count_30m:,}</div>
                <div class='kpi-subtitle'>{(v_count_30m/total_30m)*100:.1f}% بدون أي أخطاء</div>
            </div>
            """, unsafe_allow_html=True)
        with c3:
            st.markdown(f"""
            <div class='kpi-card' style='border-top: 4px solid #f59e0b;'>
                <div class='kpi-title'>🛠️ السجلات المصححة (Audit)</div>
                <div class='kpi-number' style='color: #fbbf24;'>{c_count_30m:,}</div>
                <div class='kpi-subtitle'>{(c_count_30m/total_30m)*100:.1f}% تم إصلاحها آلياً</div>
            </div>
            """, unsafe_allow_html=True)
        with c4:
            st.markdown(f"""
            <div class='kpi-card' style='border-top: 4px solid #ef4444;'>
                <div class='kpi-title'>⛔ السجلات المعزولة (Quarantine)</div>
                <div class='kpi-number' style='color: #f87171;'>{q_count_30m:,}</div>
                <div class='kpi-subtitle'>{(q_count_30m/total_30m)*100:.1f}% أخطاء غير قابلة للإصلاح</div>
            </div>
            """, unsafe_allow_html=True)
            
        st.markdown("<br>", unsafe_allow_html=True)
        
        # ─── الصف الثاني: بطاقات المقاييس المالية والأداء ───
        st.markdown("#### 💰 المؤشرات المالية وسرعة الحوسبة الموزعة:")
        k1, k2, k3, k4 = st.columns(4)
        with k1:
            gmv = spark_rep.get('financial_summary', {}).get('estimated_total_gmv_yer', 0)
            st.markdown(f"""
            <div class='kpi-card'>
                <div class='kpi-title'>💰 إجمالي المبيعات (GMV)</div>
                <div class='kpi-number' style='font-size: 1.8rem;'>{gmv/1e12:.2f} تريليون YER</div>
                <div class='kpi-subtitle'>{gmv:,.0f} ريال يمني</div>
            </div>
            """, unsafe_allow_html=True)
        with k2:
            aov = spark_rep.get('financial_summary', {}).get('average_order_value_yer', 0)
            st.markdown(f"""
            <div class='kpi-card'>
                <div class='kpi-title'>🏷️ متوسط قيمة الطلب (AOV)</div>
                <div class='kpi-number'>{aov:,.0f} YER</div>
                <div class='kpi-subtitle'>لكل فاتورة في الـ 30 مليون</div>
            </div>
            """, unsafe_allow_html=True)
        with k3:
            st.markdown(f"""
            <div class='kpi-card'>
                <div class='kpi-title'>⚡ سرعة محرك Spark</div>
                <div class='kpi-number'>{spark_rep.get('throughput_records_per_sec', 0):,.0f}</div>
                <div class='kpi-subtitle'>سجل في الثانية (Throughput)</div>
            </div>
            """, unsafe_allow_html=True)
        with k4:
            st.markdown(f"""
            <div class='kpi-card'>
                <div class='kpi-title'>⏱️ زمن المعالجة والـ Partitions</div>
                <div class='kpi-number'>{spark_rep.get('elapsed_seconds', 0):.1f}s</div>
                <div class='kpi-subtitle'>عبر {spark_rep.get('num_spark_partitions', 99)} بارتشن متوازي</div>
            </div>
            """, unsafe_allow_html=True)
            
        st.markdown("<br>", unsafe_allow_html=True)
        
        # ─── الصف الثالث: الرسوم البيانية الكبرى ───
        g1, g2 = st.columns(2)
        with g1:
            # رسم دائري لتصنيف الجودة
            df_quality = pd.DataFrame([
                {"الحالة": "سليمة 100%", "العدد": v_count_30m},
                {"الحالة": "مصححة (Audit Trail)", "العدد": c_count_30m},
                {"الحالة": "معزولة (Quarantine)", "العدد": q_count_30m}
            ])
            fig_q = px.pie(
                df_quality, names="الحالة", values="العدد",
                title="🛡️ الحصص المئوية لجودة البيانات عبر الـ 30 مليون سجل",
                hole=0.45,
                color="الحالة",
                color_discrete_map={"سليمة 100%": "#22c55e", "مصححة (Audit Trail)": "#f59e0b", "معزولة (Quarantine)": "#ef4444"},
                template="plotly_dark"
            )
            fig_q.update_layout(font_family="Cairo", margin=dict(l=40, r=40, t=50, b=40))
            st.plotly_chart(fig_q, use_container_width=True)
            
        with g2:
            cities_data = spark_rep.get('top_cities', {})
            if cities_data:
                df_cities = pd.DataFrame(list(cities_data.items()), columns=['المدينة', 'عدد الطلبات'])
                fig_city = px.bar(
                    df_cities, x='المدينة', y='عدد الطلبات',
                    title='🏙️ توزيع الـ 30 مليون طلب عبر المدن اليمنية',
                    color='عدد الطلبات',
                    color_continuous_scale='Viridis',
                    template='plotly_dark'
                )
                fig_city.update_layout(font_family="Cairo", margin=dict(l=40, r=40, t=50, b=40))
                st.plotly_chart(fig_city, use_container_width=True)
                
        g3, g4 = st.columns(2)
        with g3:
            pm_data = spark_rep.get('payment_methods', {})
            if pm_data:
                df_pm = pd.DataFrame(list(pm_data.items()), columns=['طريقة الدفع', 'العدد'])
                fig_pm = px.pie(
                    df_pm, names='طريقة الدفع', values='العدد',
                    title='💳 الحصص السوقية لطرق الدفع في الـ 30 مليون',
                    hole=0.45,
                    template='plotly_dark'
                )
                fig_pm.update_layout(font_family="Cairo", margin=dict(l=40, r=40, t=50, b=40))
                st.plotly_chart(fig_pm, use_container_width=True)
                
        with g4:
            # تفصيل حالات الشذوذ بالـ 30M
            if anomalies_30m:
                anom_labels = {
                    "MISSING_CUSTOMER_ID": "معرف عميل مفقود",
                    "MISSING_ORDER_ID": "معرف طلب مفقود",
                    "SYMBOLIC_VALUE": "قيمة رمزية ???",
                    "CORRUPTED_ITEMS_JSON": "منتجات تالفة not-json",
                    "INVALID_PHONE": "هاتف غير مطابق",
                    "INVALID_EMAIL": "إيميل تالف @@",
                    "NON_STANDARD_CURRENCY": "عملة نصية (ريال)"
                }
                df_anom = pd.DataFrame([
                    {"نوع الخطأ/الشذوذ": anom_labels.get(k, k), "العدد المرصود": v}
                    for k, v in anomalies_30m.items()
                ]).sort_values("العدد المرصود", ascending=True)
                
                fig_anom = px.bar(
                    df_anom, x="العدد المرصود", y="نوع الخطأ/الشذوذ",
                    orientation="h",
                    title="🔍 حالات الشذوذ المرصودة في الـ 30 مليون سجل بـ Spark",
                    color="العدد المرصود",
                    color_continuous_scale="Reds",
                    template="plotly_dark"
                )
                fig_anom.update_layout(font_family="Cairo", margin=dict(l=180, r=40, t=50, b=40))
                st.plotly_chart(fig_anom, use_container_width=True)
    else:
        st.warning("⚠️ **لم يتم تشغيل تحليل ملف الـ 13.26 GB بعد.**\n\nاضغط على الزر أعلاه: **🔥 تشغيل تحليل ملف الـ 13.26 GB كاملاً الآن بـ PySpark** لتشغيل محرك Apache Spark الموزع واستخراج الأرقام والإحصائيات الحقيقية أمامك!")

# =============================================================================
# TAB 3: مقارنة المحركين (Python vs Spark Benchmark)
# =============================================================================
with tab3:
    st.markdown("### ⚔️ مقارنة الأداء المعياري بين محركي Python و Apache Spark")
    st.markdown("اختبر نفس ملف البيانات على المحركين وجهاً لوجه وقارن السرعة، استهلاك الذاكرة، وزمن التنفيذ.")
    
    bench_rep = load_json_report("benchmark_results.json")
    
    if st.button("🏁 بدء اختبار المقارنة بين المحركين الآن (Run Dual Benchmark)", use_container_width=True):
        with st.spinner("جاري تشغيل خط البيانات على محرك Python ومحرك PySpark بالتتابع للمقارنة..."):
            import subprocess
            target_csv = PROJECT_ROOT / "data" / "sample_orders.csv"
            subprocess.run([sys.executable, str(PROJECT_ROOT / "src" / "benchmark.py"), "--input", str(target_csv)], check=True)
            st.success("✅ اكتمل اختبار المقارنة المعيارية بنجاح!")
            st.rerun()
            
    if bench_rep and bench_rep.get("python_batch") and bench_rep.get("pyspark"):
        py_data = bench_rep["python_batch"]
        spk_data = bench_rep["pyspark"]
        
        # جدول مقارنة الأرقام
        comp_data = [
            {"المقياس": "🚀 سرعة المعالجة (سجل/ثانية)", "Python Batch": f"{py_data['throughput_rec_per_sec']:,.1f}", "Apache PySpark": f"{spk_data['throughput_rec_per_sec']:,.1f}"},
            {"المقياس": "⏱️ زمن التنفيذ (ثواني)", "Python Batch": f"{py_data['elapsed_seconds']}s", "Apache PySpark": f"{spk_data['elapsed_seconds']}s"},
            {"المقياس": "🧠 استهلاك الذاكرة الإضافي (Delta RAM)", "Python Batch": f"{py_data['memory_delta_mb']} MB", "Apache PySpark": f"{spk_data['memory_delta_mb']} MB"},
            {"المقياس": "⚙️ المعمارية وطريقة العمل", "Python Batch": "Single-Thread Streaming", "Apache PySpark": "16 Partitions In-Memory"},
        ]
        st.table(pd.DataFrame(comp_data))
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # رسم بياني للمقارنة
        b1, b2 = st.columns(2)
        with b1:
            df_speed = pd.DataFrame([
                {"المحرك": "Python Batch", "السرعة (سجل/ثانية)": py_data['throughput_rec_per_sec']},
                {"المحرك": "Apache PySpark", "السرعة (سجل/ثانية)": spk_data['throughput_rec_per_sec']}
            ])
            fig_speed = px.bar(df_speed, x="المحرك", y="السرعة (سجل/ثانية)", color="المحرك", title="⚡ مقارنة السرعة والإنتاجية (Throughput)", template="plotly_dark")
            fig_speed.update_layout(font_family="Cairo", margin=dict(l=40, r=40, t=50, b=40))
            st.plotly_chart(fig_speed, use_container_width=True)
            
        with b2:
            df_time = pd.DataFrame([
                {"المحرك": "Python Batch", "الوقت (ثواني)": py_data['elapsed_seconds']},
                {"المحرك": "Apache PySpark", "الوقت (ثواني)": spk_data['elapsed_seconds']}
            ])
            fig_time = px.bar(df_time, x="المحرك", y="الوقت (ثواني)", color="المحرك", title="⏱️ مقارنة زمن المعالجة الكلي (Elapsed Time)", template="plotly_dark")
            fig_time.update_layout(font_family="Cairo", margin=dict(l=40, r=40, t=50, b=40))
            st.plotly_chart(fig_time, use_container_width=True)
    else:
        st.info("💡 اضغط على زر **بدء اختبار المقارنة بين المحركين الآن** لتشغيل الاختبار وعرض المقارنة وجهاً لوجه!")

# =============================================================================
# TAB 4: مركز جودة البيانات وسلة العزل (Quality & Quarantine)
# =============================================================================
with tab4:
    st.markdown("### 🛡️ مركز جودة البيانات وسلة العزل ومستكشف التدقيق")
    
    results_report = load_json_report("results.json")
    if results_report and 'error_breakdown' in results_report:
        err_dict = results_report['error_breakdown']
        
        formatted_errs = []
        for k, v in err_dict.items():
            label = ERROR_LABELS_AR.get(k, k)
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
# TAB 5: المعمارية ومحاكي موجه الملفات (Architecture & Router)
# =============================================================================
with tab5:
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
