"""
╔══════════════════════════════════════════════════════════════╗
║       배상면주가 출고현황 대시보드 v8 - DB 연동 버전         ║
║                                                              ║
║  실행: run.bat 더블클릭 (또는 python app.py)                 ║
║  접속: http://localhost:5000  /  사내: http://61.33.23.171:5000  ║
╠══════════════════════════════════════════════════════════════╣
║  [구성]                                                      ║
║  - Flask 3.x / Python 3.x / PyODBC 5.x                     ║
║  - MS-SQL (61.33.23.137) / BSM_SALE / 뷰 기반               ║
║  - 내부망 전용 (192.168.10.x / 61.33.23.x)                  ║
╠══════════════════════════════════════════════════════════════╣
║  [탭 구성 - 10개]                                            ║
║  1.월별실적      2.계획대비실적    3.지역별매출지도           ║
║  4.거래처별지도  5.거래처매출목록  6.부서사원별매출           ║
║  7.품목별매출목록  8.품목분류별분석  9.과거대비매출비교       ║
║  10.영업요약보고  11.주류업계동향                             ║
╠══════════════════════════════════════════════════════════════╣
║  [v7 변경 사항]                                              ║
║  - 탭1 명칭 변경: 매출대시보드 → 월별실적                    ║
║  - 탭명 변경: 경영요약보고 → 영업요약보고                    ║
║  - 탭 순서 변경: 영업요약보고(10) → 주류업계동향(11)         ║
╠══════════════════════════════════════════════════════════════╣
║  [주요 기능]                                                 ║
║  - 부가세 포함/별도 전환 체크박스 (전 탭 API 동시 적용)      ║
║  - 년월 선택 → 1일~말일 자동 변환                           ║
║  - 계획대비실적: 마지막월 기준 목표/실적/달성률/전월/전년동월 ║
║  - 품목분류별: 중분류 클릭 → 소분류 드릴다운                ║
║  - 부서 정렬: DEPT_ORDER 상수 한 곳에서 관리                ║
╚══════════════════════════════════════════════════════════════╝
"""

from flask import Flask, jsonify, send_from_directory, request, abort
import pandas as pd
import pyodbc
import re
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import html
import logging
import warnings
from logging.handlers import RotatingFileHandler
import os
import threading
import collections
from collections import defaultdict
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
import calendar
import math

# ══════════════════════════════════════════════
# ■ 설정값  (변경 시 여기만 수정)
# ══════════════════════════════════════════════
DB_CONFIG = {
    "server":   "61.33.23.137",
    "database": "BSM_SALE",
    "username": "sa",
    "password": "B$mbFi04!*)$18",
}

TABLE = "dbo.V_ACE_Invoice"
PLAN_TABLE = "dbo.V_ACE_SalesPlanItem"
ORG_TABLE = "dbo.V_ACE_OrgDeptHierarchy"  # 조직 계층 (OrgCd 기준 정렬용)

BIZ_UNITS = {1:"배상면주가", 4:"포천LB", 5:"고창LB", 6:"에드푸드"}

# ── 부서 정렬 순서 (변경 필요시 여기만 수정)
DEPT_ORDER = ['유통1팀','유통2팀','지역영업1팀','지역영업2팀','지역영업3팀',
              '지역영업4팀','지역영업5팀','영업관리팀','수출팀','전략사업부',
              '프렌차이즈사업부','산사원','채권관리']

def sort_by_dept(data, name_key='name'):
    """부서 정렬 함수 - DEPT_ORDER 순, 없는 부서는 뒤에"""
    return sorted(data, key=lambda r: DEPT_ORDER.index(r[name_key])
                  if r[name_key] in DEPT_ORDER else len(DEPT_ORDER))

ALLOWED_NETWORKS = [
    "127.0.0.1",
    "192.168.10.",
    "61.33.23.",
]

# ══════════════════════════════════════════════
# ■ Flask 앱 초기화 & 내부망 접근 제한
# ══════════════════════════════════════════════
app = Flask(__name__, static_folder='static')

# ── 로그 설정
# 판다스 UserWarning 억제 (SQLAlchemy 관련 반복 경고)
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy')
warnings.filterwarnings('ignore', category=DeprecationWarning)

# Flask/Werkzeug 접속 로그 억제 (GET /api/... 200 같은 일반 로그)
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

# ERROR 이상만 파일에 기록 (최대 5MB, 최대 3개 파일)
_handler = RotatingFileHandler(
    'dashboard.log', maxBytes=5*1024*1024, backupCount=2, encoding='utf-8'
)
_handler.setLevel(logging.ERROR)
_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
app.logger.addHandler(_handler)
app.logger.setLevel(logging.ERROR)

# [news] 디버그 print 대신 logging 사용
_news_logger = logging.getLogger('news')
_news_logger.setLevel(logging.ERROR)  # 뉴스 디버그 로그 억제

# ══════════════════════════════════════════════
# ■ 접속 로그
# ══════════════════════════════════════════════
ACCESS_LOG_FILE = 'access.log'
_access_history = collections.deque(maxlen=500)  # 최근 500건 메모리 보관

def log_access(ip, path):
    # API 요청 / 정적파일은 제외하고 주요 페이지만 기록
    skip = ['/api/', '/static/', '.js', '.css', '.jpg', '.png', '.ico']
    if any(s in path for s in skip):
        return
    now = datetime.now()
    ts  = now.strftime('%Y-%m-%d %H:%M:%S')
    _access_history.append({'ip': ip, 'ts': ts, 'path': path})
    # 파일에도 기록
    try:
        with open(ACCESS_LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(f"[{ts}] {ip:>15}  {path}\n")
        # 콘솔 출력
        print(f"  [{ts}] 접속: {ip}  {path}")
    except Exception:
        pass

@app.before_request
def restrict_to_internal():
    ip = request.remote_addr or ''
    if not any(ip.startswith(net) for net in ALLOWED_NETWORKS):
        abort(403)
    log_access(ip, request.path)

# ══════════════════════════════════════════════
# ■ DB 커넥션 풀링
#   API 호출마다 새 연결 맺는 오버헤드(100~300ms) 제거
#   스레드별 연결 재사용 → 전 탭 공통 속도 향상
#   연결 끊김 감지 시 자동 재연결
# ══════════════════════════════════════════════
MAX_POOL_SIZE = 5   # 동시 연결 최대 수 (사용자 수에 맞게 조정)
_pool_lock   = threading.Lock()
_pool_conns  = []   # 대기 중인 연결 목록
_pool_driver = None # 드라이버명 최초 1회 탐색 후 캐싱

def _get_driver():
    """ODBC 드라이버 최초 1회만 탐색 후 캐싱"""
    global _pool_driver
    if _pool_driver:
        return _pool_driver
    DRIVER_PRIORITY = [
        'ODBC Driver 18 for SQL Server',
        'ODBC Driver 17 for SQL Server',
        'ODBC Driver 13 for SQL Server',
        'SQL Server Native Client 11.0',
        'SQL Server',
    ]
    _pool_driver = next((d for d in DRIVER_PRIORITY if d in pyodbc.drivers()), None)
    if not _pool_driver:
        raise Exception(f"SQL Server ODBC 드라이버 없음. 설치된 드라이버: {pyodbc.drivers()}")
    return _pool_driver

def _new_connection():
    """새 DB 연결 생성"""
    driver = _get_driver()
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"UID={DB_CONFIG['username']};"
        f"PWD={DB_CONFIG['password']};"
        f"TrustServerCertificate=yes;"
        f"Encrypt=yes;"
        f"Connection Timeout=10;"
    )
    return pyodbc.connect(conn_str)

def get_connection():
    """풀에서 연결 꺼내 반환. 비었으면 새 연결 생성. 끊겼으면 자동 재연결."""
    with _pool_lock:
        while _pool_conns:
            conn = _pool_conns.pop()
            try:
                conn.execute("SELECT 1")
                return conn  # 정상 연결 재사용
            except Exception:
                try: conn.close()
                except Exception: pass
    return _new_connection()

def release_connection(conn):
    """쿼리 완료 후 연결을 풀에 반납. 풀이 가득 차면 닫아버림."""
    with _pool_lock:
        if len(_pool_conns) < MAX_POOL_SIZE:
            _pool_conns.append(conn)
        else:
            try: conn.close()
            except Exception: pass

def query_df(sql, params=None):
    """커넥션 풀에서 연결 가져와 쿼리 후 반납. 오류 시 연결 폐기."""
    conn = None
    try:
        conn = get_connection()
        df = pd.read_sql(sql, conn, params=params)
        release_connection(conn)
        return df, None
    except Exception as e:
        if conn:
            try: conn.close()
            except Exception: pass
        return None, str(e)

# ══════════════════════════════════════════════
# ■ SQL WHERE절 생성 헬퍼
# ══════════════════════════════════════════════
def date_where(date_from, date_to, col="InvoiceDate"):
    clauses, params = [], []
    if date_from:
        clauses.append(f"{col} >= ?")
        params.append(date_from)
    if date_to:
        clauses.append(f"{col} <= ?")
        params.append(date_to)
    return clauses, params

def biz_where(biz_unit):
    try:
        b = int(biz_unit) if str(biz_unit) in ('1','4','5','6') else 1
    except Exception:
        b = 1
    return [f'BizUnit = {b}'], []

def build_where(date_from, date_to, biz_unit):
    # BizUnit 먼저 → 인덱스 순서 (BizUnit, InvoiceDate) 와 일치
    bc, bp = biz_where(biz_unit)
    dc, dp = date_where(date_from, date_to)
    clauses = bc + dc
    params  = bp + dp
    return ("WHERE " + " AND ".join(clauses)) if clauses else "", params

# ══════════════════════════════════════════════
# ■ 집계/변환 헬퍼 함수
# ══════════════════════════════════════════════
def apply_vat(df, use_vat):
    """부가세 포함 여부에 따라 금액 컬럼을 TotAmt 기준으로 대체"""
    if not use_vat or df is None or 'TotAmt' not in df.columns:
        return df
    df = df.copy()
    tot = pd.to_numeric(df['TotAmt'], errors='coerce').fillna(0)
    # CurAmt 또는 sales/actual alias 모두 교체
    for col in ['CurAmt', 'sales', 'actual']:
        if col in df.columns:
            df[col] = tot
    return df

def get_vat_flag(request):
    """요청에서 VAT 플래그 추출"""
    return request.args.get('vat', 'false').lower() == 'true'

def get_region(addr):
    addr = re.sub(r'^\(?\d{3,6}[-\d]*\)?\s*', '', str(addr).strip())
    m = re.search(
        r'(서울특별시|서울|부산광역시|부산|대구광역시|대구|인천광역시|인천|'
        r'광주광역시|광주|대전광역시|대전|울산광역시|울산|세종특별자치시|세종|'
        r'경기도|경기|강원특별자치도|강원도|강원|충청북도|충북|충청남도|충남|'
        r'전북특별자치도|전라북도|전북|전라남도|전남|경상북도|경북|경상남도|경남|'
        r'제주특별자치도|제주)',
        addr
    )
    if not m:
        return '기타'
    mapping = {
        '서울특별시':'서울','부산광역시':'부산','대구광역시':'대구',
        '인천광역시':'인천','광주광역시':'광주','대전광역시':'대전',
        '울산광역시':'울산','세종특별자치시':'세종',
        '경기도':'경기','강원특별자치도':'강원','강원도':'강원',
        '충청북도':'충북','충청남도':'충남',
        '전북특별자치도':'전북','전라북도':'전북','전라남도':'전남',
        '경상북도':'경북','경상남도':'경남',
        '제주특별자치도':'제주',
    }
    return mapping.get(m.group(1), m.group(1))

def safe_top(df, group_col, value_col, n=10):
    if group_col not in df.columns or value_col not in df.columns:
        return [], []
    agg = df.groupby(group_col)[value_col].sum().sort_values(ascending=False).head(n)
    return list(agg.index), [int(v) for v in agg.values]

def calc_summary(df):
    return {
        "total_sales":  int(df['CurAmt'].sum())      if 'CurAmt'   in df.columns else 0,
        "total_qty":    int(df['STDQty'].sum())        if 'STDQty'   in df.columns else 0,
        "customer_cnt": int(df['CustNo'].nunique())   if 'CustNo'   in df.columns else 0,
        "product_cnt":  int(df['ItemName'].nunique()) if 'ItemName' in df.columns else 0,
        "order_cnt":    len(df),
        "avg_price":    int(df['CurAmt'].sum() / max(df['STDQty'].sum(), 1))
                        if 'CurAmt' in df.columns and 'STDQty' in df.columns else 0,
    }

def calc_charts(df):
    daily_labels, daily_values = [], []
    if 'InvoiceDate' in df.columns and 'CurAmt' in df.columns:
        df['_date'] = pd.to_datetime(df['InvoiceDate'], format='%Y-%m-%d', errors='coerce')
        d = df.groupby(df['_date'].dt.strftime('%m/%d'))['CurAmt'].sum().sort_index()
        daily_labels = list(d.index)
        daily_values = [int(v) for v in d.values]

    ch_lbl, ch_val = safe_top(df, 'ChannelName', 'CurAmt', 10)
    pr_lbl, pr_val = safe_top(df, 'ItemName',    'CurAmt', 10)
    de_lbl, de_val = safe_top(df, 'DeptName',    'CurAmt', 20)
    cu_lbl, cu_val = safe_top(df, 'CustName',    'CurAmt', 10)
    qt_lbl, qt_val = safe_top(df, 'ItemName',    'STDQty', 5)

    dow_labels, dow_values = [], []
    if 'InvoiceDate' in df.columns and 'CurAmt' in df.columns:
        DOW = ['월','화','수','목','금','토','일']
        df['_dow'] = pd.to_datetime(df['InvoiceDate'], format='%Y-%m-%d', errors='coerce').dt.dayofweek
        dow_s = df.groupby('_dow')['CurAmt'].sum().reindex(range(7), fill_value=0)
        dow_labels = DOW
        dow_values = [int(v) for v in dow_s.values]

    return {
        "daily":    {"labels": daily_labels, "values": daily_values},
        "channel":  {"labels": ch_lbl,       "values": ch_val},
        "product":  {"labels": pr_lbl,       "values": pr_val},
        "dept":     {"labels": de_lbl,       "values": de_val},
        "customer": {"labels": cu_lbl,       "values": cu_val},
        "qty_top":  {"labels": qt_lbl,       "values": qt_val},
        "dow":      {"labels": dow_labels,   "values": dow_values},
    }

# ══════════════════════════════════════════════
# ■ API 라우트
# ══════════════════════════════════════════════
# [API 목록]
# GET /api/file-info          : DB 연결 정보 및 날짜 범위
# GET /api/data               : 매출 대시보드 집계 (KPI + 차트)
# GET /api/map-data           : 지역별/거래처별 지도 데이터
# GET /api/customer-list      : 거래처 매출 목록
# GET /api/product-list       : 품목별 매출 목록
# GET /api/staff              : 부서/사원별 매출
# GET /api/classify           : 품목분류별 분석
# GET /api/classify/detail    : 소분류 품목 상세 (클릭 시)
# GET /api/compare            : 과거대비 매출 비교
# GET /api/yearly-trend       : 연도별 매출 추이 (전체)
# GET /api/plan               : 계획대비실적
#
# [공통 파라미터]
# from=YYYY-MM-DD  to=YYYY-MM-DD  biz=1|4|5|6  vat=true|false
@app.route('/guide')
def guide():
    return send_from_directory('static', 'guide.html')

@app.route('/api/access-log')
def get_access_log():
    from collections import Counter
    now = datetime.now()
    # 최근 1시간 접속 기록
    recent = [r for r in _access_history
              if (now - datetime.strptime(r['ts'], '%Y-%m-%d %H:%M:%S')).seconds < 3600]
    ip_counts = Counter(r['ip'] for r in recent)
    # 최근 24시간 로그 파일 읽기
    log_lines = []
    try:
        with open(ACCESS_LOG_FILE, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            log_lines = [l.strip() for l in lines[-50:] if l.strip()]
    except Exception:
        pass
    return jsonify({
        'recent_ips': [{'ip':ip,'count':cnt} for ip,cnt in ip_counts.most_common()],
        'recent_logs': log_lines[-20:]
    })

@app.route('/')
def index():
    return send_from_directory('static', 'index.html')

@app.route('/<path:filename>')
def static_files(filename):
    return send_from_directory('static', filename)

@app.route('/api/file-info')
def get_file_info():
    biz_unit = request.args.get('biz', '1')
    try:
        biz_int = int(biz_unit) if str(biz_unit) in ('1','4','5','6') else 1
    except Exception:
        biz_int = 1
    sql = f"""
        SELECT MIN(InvoiceDate) AS min_date, MAX(InvoiceDate) AS max_date, COUNT(*) AS total_rows
        FROM {TABLE}
        WHERE BizUnit = {biz_int} AND InvoiceDate IS NOT NULL AND InvoiceDate != ''
    """
    df, err = query_df(sql)
    if err:
        return jsonify({"error": err})
    row = df.iloc[0]
    def fmt(d):
        s = str(d) if d else ''
        return s[:10] if len(s) >= 10 else s
    return jsonify({
        "filename":   "MS-SQL DB (V_ACE_Invoice)",
        "file_date":  fmt(row['max_date']),
        "min_date":   fmt(row['min_date']),
        "max_date":   fmt(row['max_date']),
        "total_rows": int(row['total_rows']),
        "biz_units":  BIZ_UNITS,
    })

@app.route('/api/data')
def get_data():
    date_from = request.args.get('from', '')
    date_to   = request.args.get('to',   '')
    biz_unit  = request.args.get('biz',  '1')
    use_vat   = get_vat_flag(request)
    where, params = build_where(date_from, date_to, biz_unit)

    sql = f"""
        SELECT InvoiceDate, DeptName, CustName, CustNo,
               ChannelName, ItemName, Qty, STDQty, CurAmt, CurVat, TotAmt
        FROM {TABLE} {where}
    """
    df, err = query_df(sql, params or None)
    df = apply_vat(df, use_vat)
    if err:
        return jsonify({"error": err})

    if 'InvoiceDate' in df.columns:
        df['InvoiceDate'] = df['InvoiceDate'].astype(str).str.strip().str[:10]
    for col in ['CurAmt', 'Qty', 'STDQty', 'CurVat', 'TotAmt']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    def fmt_date(s):
        s = str(s)
        return s[:10] if len(s) >= 10 else s

    return jsonify({
        "summary": calc_summary(df),
        "charts":  calc_charts(df),
        "period": {
            "from": date_from or (fmt_date(df['InvoiceDate'].min()) if len(df) else ''),
            "to":   date_to   or (fmt_date(df['InvoiceDate'].max()) if len(df) else ''),
            "rows": len(df),
        }
    })

@app.route('/api/map-data')
def get_map_data():
    date_from = request.args.get('from',    '')
    date_to   = request.args.get('to',      '')
    use_vat   = get_vat_flag(request)
    channel   = request.args.get('channel', '전체')
    biz_unit  = request.args.get('biz',     '1')

    # BizUnit 먼저 → 인덱스 순서 (BizUnit, InvoiceDate) 와 일치
    bc, bp = biz_where(biz_unit)
    dc, dp = date_where(date_from, date_to)
    clauses = bc + dc
    params  = bp + dp
    if channel != '전체':
        clauses.append("ChannelName = ?")
        params.append(channel)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""

    sql = f"""
        SELECT CustNo, MAX(CustName) AS CustName, SUM(CurAmt) AS CurAmt, SUM(TotAmt) AS TotAmt,
               SUM(STDQty) AS Qty, COUNT(*) AS cnt,
               MAX(ChannelName) AS ChannelName, MAX(Addr) AS Addr
        FROM {TABLE} {where}
        GROUP BY CustNo
        ORDER BY SUM(CurAmt) DESC
    """
    df, err = query_df(sql, params or None)
    df = apply_vat(df, use_vat)
    if err:
        return jsonify({"error": err})

    df['Addr']   = df['Addr'].fillna('').astype(str).str.strip()
    df['지역']   = df['Addr'].apply(get_region)
    df['CurAmt'] = pd.to_numeric(df['CurAmt'], errors='coerce').fillna(0).astype(int)
    df['Qty']    = pd.to_numeric(df['Qty'],    errors='coerce').fillna(0).astype(int)
    df['cnt']    = df['cnt'].astype(int)

    records = [
        {
            "거래처번호": r['CustNo'],  "거래처명": r['CustName'],
            "공급가액":   r['CurAmt'],  "수량":     r['Qty'],
            "건수":       r['cnt'],     "유통구조": r['ChannelName'],
            "지역":       r['지역'],    "주소":     r['Addr'],
        }
        for _, r in df.iterrows()
    ]
    return jsonify({"customers": records})

@app.route('/api/customer-list')
def get_customer_list():
    date_from = request.args.get('from', '')
    date_to   = request.args.get('to',   '')
    biz_unit  = request.args.get('biz',  '1')
    use_vat   = get_vat_flag(request)
    where, params = build_where(date_from, date_to, biz_unit)

    sql = f"""
        SELECT CustNo, MAX(CustName) AS CustName, MAX(ChannelName) AS ChannelName,
               MAX(DeptName) AS DeptName, SUM(CurAmt) AS CurAmt, SUM(TotAmt) AS TotAmt,
               SUM(STDQty) AS Qty, COUNT(*) AS cnt, MAX(Addr) AS Addr
        FROM {TABLE} {where}
        GROUP BY CustNo
        ORDER BY SUM(CurAmt) DESC
    """
    df, err = query_df(sql, params or None)
    df = apply_vat(df, use_vat)
    if err:
        return jsonify({"error": err})

    df['CurAmt'] = pd.to_numeric(df['CurAmt'], errors='coerce').fillna(0).astype(int)
    df['Qty']    = pd.to_numeric(df['Qty'],    errors='coerce').fillna(0).astype(int)
    df['cnt']    = df['cnt'].astype(int)
    df['Addr']   = df['Addr'].fillna('')
    df['지역']   = df['Addr'].apply(get_region)

    records = [
        {
            "거래처번호": r['CustNo'],     "거래처명": r['CustName'],
            "유통구조":   r['ChannelName'],"부서":     r['DeptName'],
            "공급가액":   r['CurAmt'],     "수량":     r['Qty'],
            "건수":       r['cnt'],        "지역":     r['지역'],
            "주소":       r['Addr'],
        }
        for _, r in df.iterrows()
    ]
    return jsonify({"customers": records})

@app.route('/api/product-list')
def get_product_list():
    date_from = request.args.get('from', '')
    date_to   = request.args.get('to',   '')
    biz_unit  = request.args.get('biz',  '1')
    use_vat   = get_vat_flag(request)
    where, params = build_where(date_from, date_to, biz_unit)

    sql = f"""
        SELECT ItemName, SUM(CurAmt) AS CurAmt, SUM(TotAmt) AS TotAmt, SUM(STDQty) AS Qty, COUNT(*) AS cnt
        FROM {TABLE} {where}
        GROUP BY ItemName
        ORDER BY SUM(CurAmt) DESC
    """
    df, err = query_df(sql, params or None)
    df = apply_vat(df, use_vat)
    if err:
        return jsonify({"error": err})

    df['CurAmt'] = pd.to_numeric(df['CurAmt'], errors='coerce').fillna(0).astype(int)
    df['Qty']    = pd.to_numeric(df['Qty'],    errors='coerce').fillna(0).astype(int)
    df['cnt']    = df['cnt'].astype(int)

    return jsonify({"products": [
        {"품명": r['ItemName'], "공급가액": r['CurAmt'], "수량": r['Qty'], "건수": r['cnt']}
        for _, r in df.iterrows()
    ]})

@app.route('/api/staff')
def get_staff():
    date_from = request.args.get('from', '')
    date_to   = request.args.get('to',   '')
    biz_unit  = request.args.get('biz',  '1')
    use_vat   = get_vat_flag(request)
    where, params = build_where(date_from, date_to, biz_unit)

    sql_emp = f"""
        SELECT DeptName, EmpName, SUM(CurAmt) AS sales, SUM(TotAmt) AS TotAmt, SUM(STDQty) AS qty, COUNT(*) AS cnt
        FROM {TABLE} {where}
        GROUP BY DeptName, EmpName
        ORDER BY SUM(CurAmt) DESC
    """
    sql_dept = f"""
        SELECT DeptName, SUM(CurAmt) AS sales, SUM(TotAmt) AS TotAmt, SUM(STDQty) AS qty, COUNT(*) AS cnt
        FROM {TABLE} {where}
        GROUP BY DeptName
        ORDER BY SUM(CurAmt) DESC
    """
    df_emp,  err1 = query_df(sql_emp,  params or None)
    df_dept, err2 = query_df(sql_dept, params or None)
    df_emp  = apply_vat(df_emp,  use_vat)
    df_dept = apply_vat(df_dept, use_vat)
    if err1: return jsonify({"error": err1})
    if err2: return jsonify({"error": err2})

    for df in [df_emp, df_dept]:
        for col in ['sales', 'qty', 'cnt']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    return jsonify({
        "staff":       [{"dept_name": r['DeptName'] or '', "emp_name": r['EmpName'] or '',
                         "sales": int(r['sales']), "qty": int(r['qty']), "cnt": int(r['cnt'])}
                        for _, r in df_emp.iterrows()],
        "dept":        [{"dept_name": r['DeptName'] or '',
                         "sales": int(r['sales']), "qty": int(r['qty']), "cnt": int(r['cnt'])}
                        for _, r in df_dept.iterrows()],
        "total_sales": int(df_emp['sales'].sum()) if len(df_emp) else 0,
        "total_qty":   int(df_emp['qty'].sum())   if len(df_emp) else 0,
        "emp_cnt":     len(df_emp),
        "dept_cnt":    len(df_dept),
    })


@app.route('/api/classify')
def get_classify():
    date_from = request.args.get('from', '')
    date_to   = request.args.get('to',   '')
    biz_unit  = request.args.get('biz',  '1')
    use_vat   = get_vat_flag(request)
    where, params = build_where(date_from, date_to, biz_unit)

    sql = f"""
        SELECT ItemClassLName, ItemClassMName, ItemClassSName,
               InvoiceDate, CurAmt, TotAmt, STDQty, CustNo
        FROM {TABLE} {where}
    """
    df, err = query_df(sql, params or None)
    df = apply_vat(df, use_vat)
    if err:
        return jsonify({"error": err})

    # 전월대비 비교용: 시작월 기준 전월부터 종료월까지 조회
    try:
        if date_from and date_to:
            dt_f = datetime.strptime(date_from[:7]+'-01', '%Y-%m-%d')
            # 시작월의 전월 1일
            prev_month = (dt_f - timedelta(days=1)).replace(day=1)
            ext_from = prev_month.strftime('%Y-%m-%d')
            ext_where, ext_params = build_where(ext_from, date_to, biz_unit)
            sql_ext = f"""
                SELECT ItemClassLName, ItemClassMName, ItemClassSName,
                       InvoiceDate, CurAmt, TotAmt, STDQty, CustNo
                FROM {TABLE} {ext_where}
            """
            df_ext, _ = query_df(sql_ext, ext_params or None)
            if df_ext is not None and len(df_ext):
                df_ext = apply_vat(df_ext, use_vat)
                for col in ['CurAmt', 'STDQty']:
                    if col in df_ext.columns:
                        df_ext[col] = pd.to_numeric(df_ext[col], errors='coerce').fillna(0)
                for col in ['ItemClassLName','ItemClassMName','ItemClassSName','InvoiceDate']:
                    if col in df_ext.columns:
                        df_ext[col] = df_ext[col].fillna('미분류').astype(str).str.strip()
            else:
                df_ext = df.copy()
        else:
            df_ext = df.copy()
    except Exception:
        df_ext = df.copy()

    for col in ['CurAmt', 'STDQty']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    for col in ['ItemClassLName','ItemClassMName','ItemClassSName','InvoiceDate']:
        if col in df.columns:
            df[col] = df[col].fillna('미분류').astype(str).str.strip()

    def grp_sum(d, col):
        if col not in d.columns: return []
        g = d.groupby(col).agg(sales=('CurAmt','sum'), qty=('STDQty','sum'),
                                cust=('CustNo','nunique')).reset_index()
        g = g.sort_values('sales', ascending=False)
        total = g['sales'].sum() or 1
        return [{'name': r[col], 'sales': int(r['sales']), 'qty': int(r['qty']),
                 'cust': int(r['cust']), 'pct': round(r['sales']/total*100, 1)}
                for _, r in g.iterrows()]

    # 월별 중분류 매트릭스 (df_ext 사용 - 전전월까지 포함)
    monthly_matrix = []
    if 'InvoiceDate' in df_ext.columns and 'ItemClassMName' in df_ext.columns:
        df_ext['_month'] = df_ext['InvoiceDate'].str[:7]
        months = sorted(df_ext['_month'].unique())
        mnames = df_ext.groupby('ItemClassMName')['CurAmt'].sum().sort_values(ascending=False).index.tolist()
        for mn in mnames:
            row = {'name': mn, 'months': {}}
            sub = df_ext[df_ext['ItemClassMName'] == mn]
            for mo in months:
                row['months'][mo] = int(sub[sub['_month']==mo]['CurAmt'].sum())
            monthly_matrix.append(row)
        monthly_labels = months
    else:
        monthly_labels = []

    # 월별 전체 매출 추이 (df_ext 사용)
    monthly_total = []
    if 'InvoiceDate' in df_ext.columns:
        df_ext['_month'] = df_ext['InvoiceDate'].str[:7]
        mg = df_ext.groupby('_month').agg(sales=('CurAmt','sum'), qty=('STDQty','sum')).reset_index()
        mg = mg.sort_values('_month')
        monthly_total = [{'month': r['_month'], 'sales': int(r['sales']), 'qty': int(r['qty'])}
                         for _, r in mg.iterrows()]

    # 전월 대비 중분류 증감 (df_ext 사용)
    mom_change = []
    if 'ItemClassMName' in df_ext.columns:
        df_ext['_month'] = df_ext['InvoiceDate'].str[:7]
        all_months = sorted(df_ext['_month'].unique())
        if len(all_months) >= 2:
            cur_m  = all_months[-1]
            prev_m = all_months[-2]
            cur_g  = df_ext[df_ext['_month']==cur_m].groupby('ItemClassMName')['CurAmt'].sum()
            prev_g = df_ext[df_ext['_month']==prev_m].groupby('ItemClassMName')['CurAmt'].sum()
            all_names = set(cur_g.index) | set(prev_g.index)
            for n in all_names:
                c = int(cur_g.get(n, 0))
                p = int(prev_g.get(n, 0))
                rate = round((c-p)/p*100, 1) if p > 0 else None
                mom_change.append({'name': n, 'cur': c, 'prev': p, 'rate': rate})
            mom_change.sort(key=lambda x: abs(x['rate'] or 0), reverse=True)

    # 중분류별 소분류 전월대비 증감 (df_ext 사용 - 전전월까지 포함)
    mid_small_mom = {}
    if 'ItemClassMName' in df_ext.columns and 'ItemClassSName' in df_ext.columns:
        df_ext['_month'] = df_ext['InvoiceDate'].str[:7]
        all_months_ext = sorted(df_ext['_month'].unique())
        if len(all_months_ext) >= 2:
            cur_m  = all_months_ext[-1]
            prev_m = all_months_ext[-2]
            for mid_name in df_ext['ItemClassMName'].unique():
                sub = df_ext[df_ext['ItemClassMName'] == mid_name]
                cur_g  = sub[sub['_month']==cur_m].groupby('ItemClassSName')['CurAmt'].sum()
                prev_g = sub[sub['_month']==prev_m].groupby('ItemClassSName')['CurAmt'].sum()
                all_s  = set(cur_g.index) | set(prev_g.index)
                rows = []
                for s in all_s:
                    c = int(cur_g.get(s, 0))
                    p = int(prev_g.get(s, 0))
                    rate = round((c-p)/p*100, 1) if p > 0 else None
                    rows.append({'name': s, 'cur': c, 'prev': p, 'rate': rate})
                rows.sort(key=lambda x: x['cur'], reverse=True)
                mid_small_mom[mid_name] = rows

    return jsonify({
        'large':   grp_sum(df, 'ItemClassLName'),
        'mid':     grp_sum(df, 'ItemClassMName'),
        'small':   grp_sum(df, 'ItemClassSName'),
        'monthly_total':  monthly_total,
        'monthly_matrix': monthly_matrix,
        'monthly_labels': monthly_labels,
        'mom_change':     mom_change,
        'mid_small_mom':  mid_small_mom,
        'period': {'from': date_from, 'to': date_to},
    })

@app.route('/api/compare')
def get_compare():
    date_from = request.args.get('from', '')
    date_to   = request.args.get('to',   '')
    use_vat   = get_vat_flag(request)
    biz_unit  = request.args.get('biz',  '1')
    mode      = request.args.get('mode', 'yy')  # yy=전년동기, mm=전월, mm2=전전월

    def load_period(d_from, d_to):
        w, p = build_where(d_from, d_to, biz_unit)
        sql = f"SELECT InvoiceDate, DeptName, EmpName, CustName, CustNo, ItemName, ChannelName, STDQty AS Qty, CurAmt, TotAmt FROM {TABLE} {w}"
        df, err = query_df(sql, p or None)
        df = apply_vat(df, use_vat)
        if err:
            return pd.DataFrame()
        df['CurAmt'] = pd.to_numeric(df['CurAmt'], errors='coerce').fillna(0)
        df['Qty']    = pd.to_numeric(df['Qty'],    errors='coerce').fillna(0)
        return df

    df_this = load_period(date_from, date_to)

    try:
        from_dt = datetime.strptime(date_from, '%Y-%m-%d')
        to_dt   = datetime.strptime(date_to,   '%Y-%m-%d')

        if mode == 'yy':
            # 전년 동기: 1년 전 동일 기간
            try:    last_from = from_dt.replace(year=from_dt.year - 1)
            except: last_from = from_dt.replace(year=from_dt.year - 1, day=28)
            try:    last_to   = to_dt.replace(year=to_dt.year - 1)
            except: last_to   = to_dt.replace(year=to_dt.year - 1, day=28)

        elif mode == 'mm':
            # 전월: 현재 선택 달의 바로 전달 1일~말일
            first_of_this = from_dt.replace(day=1)
            last_to   = first_of_this - timedelta(days=1)          # 전달 말일
            last_from = last_to.replace(day=1)                     # 전달 1일

        elif mode == 'mm2':
            # 전전월: 전달의 또 전달 1일~말일
            first_of_this  = from_dt.replace(day=1)
            first_of_prev  = (first_of_this - timedelta(days=1)).replace(day=1)
            last_to   = first_of_prev - timedelta(days=1)          # 전전달 말일
            last_from = last_to.replace(day=1)                     # 전전달 1일
        else:
            try:    last_from = from_dt.replace(year=from_dt.year - 1)
            except: last_from = from_dt.replace(year=from_dt.year - 1, day=28)
            try:    last_to   = to_dt.replace(year=to_dt.year - 1)
            except: last_to   = to_dt.replace(year=to_dt.year - 1, day=28)

        df_last = load_period(last_from.strftime('%Y-%m-%d'), last_to.strftime('%Y-%m-%d'))
        last_from_str = last_from.strftime('%Y-%m-%d')
        last_to_str   = last_to.strftime('%Y-%m-%d')
    except Exception as e:
        return jsonify({"error": f"날짜 계산 오류: {e}"})

    def agg_summary(d):
        return {
            "total_sales":  int(d['CurAmt'].sum())       if 'CurAmt'   in d.columns else 0,
            "total_qty":    int(d['Qty'].sum())           if 'Qty'      in d.columns else 0,
            "customer_cnt": int(d['CustNo'].nunique())    if 'CustNo'   in d.columns else 0,
            "product_cnt":  int(d['ItemName'].nunique())  if 'ItemName' in d.columns else 0,
            "order_cnt":    len(d),
        }

    def agg_by(d, col, n=10):
        if col not in d.columns or 'CurAmt' not in d.columns:
            return {}
        return d.groupby(col)['CurAmt'].sum().sort_values(ascending=False).head(n).to_dict()

    def agg_by_staff(d, n=10):
        if 'EmpName' not in d.columns or 'DeptName' not in d.columns or 'CurAmt' not in d.columns:
            return {}
        grp = d.groupby(['DeptName','EmpName'])['CurAmt'].sum().sort_values(ascending=False).head(n)
        result = {}
        for (dept, emp), v in grp.items():
            key = f"{str(dept)}/{str(emp)}"
            result[key] = int(v)
        return result

    def agg_cust(d):
        if 'CustNo' not in d.columns or 'CustName' not in d.columns or 'CurAmt' not in d.columns:
            return {}
        grp = d.groupby('CustNo').agg(
            name=('CustName', 'first'),
            amt =('CurAmt',   'sum')
        ).reset_index()
        return {
            str(r['CustNo']): {'name': str(r['name']), 'amt': int(r['amt'])}
            for _, r in grp.iterrows()
        }

    def agg_daily(d):
        # 일차(1일, 2일...) 기준으로 집계 - 월이 달라도 같은 X축에 비교 가능
        if 'InvoiceDate' not in d.columns or 'CurAmt' not in d.columns:
            return {}
        result = {}
        for _, row in d.iterrows():
            try:
                day = int(str(row['InvoiceDate'])[8:10])  # DD 추출
                key = f"{day}일"
                result[key] = result.get(key, 0) + int(row['CurAmt'])
            except Exception:
                pass
        return result

    this_channel = agg_by(df_this, 'ChannelName')
    last_channel = agg_by(df_last, 'ChannelName')
    this_product = agg_by(df_this, 'ItemName')
    last_product = agg_by(df_last, 'ItemName')
    this_daily   = agg_daily(df_this)
    last_daily   = agg_daily(df_last)
    this_dept    = agg_by(df_this, 'DeptName', 20)
    last_dept    = agg_by(df_last, 'DeptName', 20)
    this_staff   = agg_by_staff(df_this, 10)
    last_staff   = agg_by_staff(df_last, 10)
    this_cust    = agg_cust(df_this)
    last_cust    = agg_cust(df_last)

    # 신규/이탈/급변동 거래처
    this_nos = set(this_cust.keys())
    last_nos = set(last_cust.keys())
    new_custs  = [{'no': k, 'name': this_cust[k]['name'], 'amt': this_cust[k]['amt']}
                  for k in (this_nos - last_nos)]
    new_custs.sort(key=lambda x: x['amt'], reverse=True)
    lost_custs = [{'no': k, 'name': last_cust[k]['name'], 'amt': last_cust[k]['amt']}
                  for k in (last_nos - this_nos)]
    lost_custs.sort(key=lambda x: x['amt'], reverse=True)
    # 급변동: 양쪽에 있는 거래처 중 증감률 큰 TOP5
    both_nos = this_nos & last_nos
    changes = []
    for k in both_nos:
        t = this_cust[k]['amt']
        l = last_cust[k]['amt']
        if l > 0:
            rate = (t - l) / l * 100
            changes.append({'no': k, 'name': this_cust[k]['name'],
                             'this': t, 'last': l, 'rate': round(rate, 1)})
    changes.sort(key=lambda x: abs(x['rate']), reverse=True)
    top_changes = changes  # 전체 반환

    channel_keys = sorted(
        set(list(this_channel.keys()) + list(last_channel.keys())),
        key=lambda x: this_channel.get(x, 0) + last_channel.get(x, 0), reverse=True
    )[:10]
    product_keys = list(this_product.keys())[:10]
    # 일차 기준 정렬 (1일, 2일, ..., 31일 순)
    all_day_keys = set(list(this_daily.keys()) + list(last_daily.keys()))
    daily_keys   = sorted(all_day_keys, key=lambda x: int(x.replace('일','')))

    dept_keys  = sorted(set(list(this_dept.keys()) + list(last_dept.keys())),
                       key=lambda x: this_dept.get(x,0)+last_dept.get(x,0), reverse=True)[:15]
    staff_keys = list(this_staff.keys())[:10]

    return jsonify({
        "period": {
            "this": {"from": date_from,     "to": date_to,     "rows": len(df_this)},
            "last": {"from": last_from_str, "to": last_to_str, "rows": len(df_last)},
        },
        "summary": {"this": agg_summary(df_this), "last": agg_summary(df_last)},
        "channel": {
            "labels":      channel_keys,
            "this_values": [int(this_channel.get(k, 0)) for k in channel_keys],
            "last_values": [int(last_channel.get(k, 0)) for k in channel_keys],
        },
        "product": {
            "labels":      product_keys,
            "this_values": [int(this_product.get(k, 0)) for k in product_keys],
            "last_values": [int(last_product.get(k, 0)) for k in product_keys],
        },
        "daily": {
            "labels":      daily_keys,
            "this_values": [int(this_daily.get(k, 0)) for k in daily_keys],
            "last_values": [int(last_daily.get(k, 0)) for k in daily_keys],
        },
        "dept": {
            "labels":      dept_keys,
            "this_values": [int(this_dept.get(k, 0)) for k in dept_keys],
            "last_values": [int(last_dept.get(k, 0)) for k in dept_keys],
        },
        "staff": {
            "labels":      staff_keys,
            "this_values": [int(this_staff.get(k, 0)) for k in staff_keys],
            "last_values": [int(last_staff.get(k, 0)) for k in staff_keys],
        },
        "customers": {
            "new":     new_custs[:20],
            "lost":    lost_custs[:20],
            "changes": top_changes,
        },
    })

@app.route('/api/yearly-trend')
def get_yearly_trend():
    # 연도별 월별 매출 집계 (사업단위 필터만 적용)
    # 기간 필터 없이 전체 연도 반환 → 과거대비비교 탭에서 사용
    biz_unit = request.args.get('biz', '1')
    use_vat  = get_vat_flag(request)
    bc, _ = biz_where(biz_unit)
    where = "WHERE " + bc[0]
    sql = (
        f"SELECT SUBSTRING(InvoiceDate,1,4) AS year, SUBSTRING(InvoiceDate,6,2) AS month, "
        f"SUM(CurAmt) AS sales, SUM(TotAmt) AS TotAmt FROM {TABLE} {where} "
        f"GROUP BY SUBSTRING(InvoiceDate,1,4), SUBSTRING(InvoiceDate,6,2) ORDER BY year, month"
    )
    df, err = query_df(sql)
    df = apply_vat(df, use_vat)
    if err:
        return jsonify({"error": err})

    df['sales'] = pd.to_numeric(df['sales'], errors='coerce').fillna(0).astype(int)
    df['year']  = df['year'].astype(str).str.strip()
    df['month'] = df['month'].astype(str).str.strip()

    years  = sorted(df['year'].unique())
    months = [str(m).zfill(2) for m in range(1, 13)]

    result = {}
    for y in years:
        sub = df[df['year'] == y]
        existing_months = set(sub['month'].tolist())
        result[y] = {m: int(sub[sub['month']==m]['sales'].sum()) if m in existing_months else None for m in months}

    return jsonify({'years': years, 'months': months, 'data': result})

@app.route('/api/classify/detail')
def get_classify_detail():
    # 소분류 클릭 시 해당 소분류의 품목별 매출 상세 반환
    # 파라미터: small=소분류명 (URL 인코딩 필요)
    date_from  = request.args.get('from', '')
    date_to    = request.args.get('to',   '')
    biz_unit   = request.args.get('biz',  '1')
    small_name = request.args.get('small', '')
    use_vat    = get_vat_flag(request)

    def load_items(d_from, d_to):
        # BizUnit 먼저 → 인덱스 순서 (BizUnit, InvoiceDate) 와 일치
        bc, bp = biz_where(biz_unit)
        dc, dp = date_where(d_from, d_to)
        clauses = bc + dc
        params  = bp + dp
        if small_name:
            clauses.append("ItemClassSName = ?")
            params.append(small_name)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        sql = f"SELECT ItemName, SUM(CurAmt) AS sales, SUM(TotAmt) AS TotAmt, SUM(STDQty) AS qty FROM {TABLE} {where} GROUP BY ItemName ORDER BY SUM(CurAmt) DESC"
        df, err = query_df(sql, params or None)
        df = apply_vat(df, use_vat)
        if err or df is None: return {}
        df['sales'] = pd.to_numeric(df['sales'], errors='coerce').fillna(0).astype(int)
        df['qty']   = pd.to_numeric(df['qty'],   errors='coerce').fillna(0).astype(int)
        return {r['ItemName']: {'sales': int(r['sales']), 'qty': int(r['qty'])} for _, r in df.iterrows()}

    cur_data = load_items(date_from, date_to)

    # 전월 계산
    prev_data = {}
    try:
        if date_to:
            first_of_cur = datetime.strptime(date_to[:7]+'-01', '%Y-%m-%d')
            prev_last = first_of_cur - timedelta(days=1)
            prev_first = prev_last.replace(day=1)
            prev_data = load_items(prev_first.strftime('%Y-%m-%d'), prev_last.strftime('%Y-%m-%d'))
    except Exception:
        pass

    all_names = list(cur_data.keys()) or list(prev_data.keys())
    total = sum(v['sales'] for v in cur_data.values()) or 1

    return jsonify({
        'items': [
            {
                'name':       name,
                'sales':      cur_data.get(name, {}).get('sales', 0),
                'prev_sales': prev_data.get(name, {}).get('sales', 0),
                'qty':        cur_data.get(name, {}).get('qty', 0),
                'pct':        round(cur_data.get(name, {}).get('sales', 0) / total * 100, 1)
            }
            for name in all_names
        ],
        'small_name': small_name,
    })


@app.route('/api/plan')
def get_plan():
    date_from = request.args.get('from', '')
    date_to   = request.args.get('to',   '')
    biz_unit  = request.args.get('biz',  '1')
    use_vat   = get_vat_flag(request)

    try:
        b = int(biz_unit) if str(biz_unit) in ('1','4','5','6') else 1
    except Exception:
        b = 1

    # 연도 범위 (월별 차트용 연도 전체)
    try:
        year_from = int(date_from[:4]) if date_from else datetime.now().year
        year_to   = int(date_to[:4])   if date_to   else datetime.now().year
        ym_year_from = str(year_from) + '01'
        ym_year_to   = str(year_to)   + '12'
        act_from = f"{year_from}-01-01"
        act_to   = f"{year_to}-12-31"
    except Exception:
        year_from = year_to = datetime.now().year
        ym_year_from = str(year_from) + '01'
        ym_year_to   = str(year_to)   + '12'
        act_from = date_from
        act_to   = date_to

    # 선택 기간 기준 PlanYM 범위
    try:
        ym_from = date_from[:7].replace('-','') if date_from else ym_year_from
        ym_to   = date_to[:7].replace('-','')   if date_to   else ym_year_to
    except Exception:
        ym_from = ym_year_from
        ym_to   = ym_year_to

    # 계획 데이터 - 두 벌: 선택기간용 + 연도전체(월별차트용)
    def make_plan_df(ym_f, ym_t):
        clauses = [f"BizUnit = {b}"]
        if ym_f: clauses.append(f"PlanYM >= '{ym_f}'")
        if ym_t: clauses.append(f"PlanYM <= '{ym_t}'")
        w = "WHERE " + " AND ".join(clauses)
        sql = (
            f"SELECT PlanYM, DeptName, EmpName, CustName, ItemName, ChannelName, "
            f"Lv1Name, Lv2Name, Lv3Name, "
            f"ItemClassLName, ItemClassMName, ItemClassSName, SUM(PlanDomAmt) AS planamt "
            f"FROM {PLAN_TABLE} {w} "
            f"GROUP BY PlanYM, DeptName, EmpName, CustName, ItemName, ChannelName, "
            f"Lv1Name, Lv2Name, Lv3Name, "
            f"ItemClassLName, ItemClassMName, ItemClassSName"
        )
        df, err = query_df(sql)
        return df, err

    # 선택 기간 계획 (차트/표/달성률용) - 계획 없어도 실적은 표시
    df_plan, err = make_plan_df(ym_from, ym_to)
    if err:
        return jsonify({"error": "계획 조회 오류: " + err})
    if df_plan is None:
        df_plan = pd.DataFrame()  # 계획 없으면 빈 DataFrame으로 처리

    # 연도 전체 계획 (월별 차트용)
    df_plan_year, _ = make_plan_df(ym_year_from, ym_year_to)
    if df_plan_year is None or len(df_plan_year) == 0:
        df_plan_year = df_plan.copy() if len(df_plan) else pd.DataFrame()

    # ── 실적 데이터 최적화: 넓은 범위 1번 조회 후 Python에서 필터링
    # 범위: 작년 1월(추이용) ~ 올해 12월 한 번에 가져오기
    wide_from = f"{year_from - 1}-01-01"
    wide_to   = f"{year_to}-12-31"
    wide_where, wide_params = build_where(wide_from, wide_to, biz_unit)
    sql_act_wide = (
        f"SELECT SUBSTRING(InvoiceDate,1,4)+SUBSTRING(InvoiceDate,6,2) AS PlanYM, "
        f"InvoiceDate, DeptName, EmpName, CustName, ItemName, ChannelName, "
        f"Lv1Name, Lv2Name, Lv3Name, "
        f"ItemClassLName, ItemClassMName, ItemClassSName, "
        f"SUM(CurAmt) AS actual, SUM(TotAmt) AS TotAmt "
        f"FROM {TABLE} {wide_where} "
        f"GROUP BY SUBSTRING(InvoiceDate,1,4)+SUBSTRING(InvoiceDate,6,2), "
        f"InvoiceDate, DeptName, EmpName, CustName, ItemName, ChannelName, "
        f"Lv1Name, Lv2Name, Lv3Name, "
        f"ItemClassLName, ItemClassMName, ItemClassSName"
    )
    df_act_wide, err = query_df(sql_act_wide, wide_params or None)
    if err:
        return jsonify({"error": "실적 조회 오류: " + err})
    if df_act_wide is None:
        df_act_wide = pd.DataFrame()
    else:
        df_act_wide = apply_vat(df_act_wide, use_vat)
        df_act_wide['actual'] = pd.to_numeric(df_act_wide['actual'], errors='coerce').fillna(0)
        for col in ['DeptName','EmpName','CustName','ItemName','ChannelName','PlanYM','InvoiceDate',
                    'ItemClassLName','ItemClassMName','ItemClassSName']:
            if col in df_act_wide.columns:
                df_act_wide[col] = df_act_wide[col].fillna('미지정').astype(str).str.strip()
        # Lv 컬럼은 빈 문자열로 처리 (미지정으로 채우면 build_lv_table에서 skip됨)
        for col in ['Lv1Name','Lv2Name','Lv3Name']:
            if col in df_act_wide.columns:
                df_act_wide[col] = df_act_wide[col].fillna('').astype(str).str.strip()

    # Python에서 각 범위 필터링
    def filter_act(df, d_from, d_to):
        if df is None or len(df) == 0: return pd.DataFrame()
        return df[(df['InvoiceDate'] >= d_from) & (df['InvoiceDate'] <= d_to)].copy()

    # 선택기간 실적
    df_act = filter_act(df_act_wide, date_from, date_to) if date_from and date_to else df_act_wide.copy()

    # 마지막 월 실적 (부서별 상세표용)
    try:
        last_ym = ym_to
        last_y = int(last_ym[:4]); last_m = int(last_ym[4:])
        last_day = calendar.monthrange(last_y, last_m)[1]
        last_month_from = f"{last_y}-{str(last_m).zfill(2)}-01"
        last_month_to   = f"{last_y}-{str(last_m).zfill(2)}-{str(last_day).zfill(2)}"
    except Exception:
        last_month_from = date_from
        last_month_to   = date_to
    df_act_last = filter_act(df_act_wide, last_month_from, last_month_to)

    # 연도 전체 실적 (월별 차트용)
    df_act_year_wide = filter_act(df_act_wide, act_from, act_to)

    # 수치 정리 (계획만 - 실적은 wide 조회 시 처리됨)
    if len(df_plan) and 'planamt' in df_plan.columns:
        df_plan['planamt'] = pd.to_numeric(df_plan['planamt'], errors='coerce').fillna(0)
    for col in ['DeptName','EmpName','CustName','ItemName','ChannelName',
                'Lv1Name','Lv2Name','Lv3Name',
                'ItemClassLName','ItemClassMName','ItemClassSName','PlanYM']:
        if len(df_plan) and col in df_plan.columns:
            df_plan[col] = df_plan[col].fillna('').astype(str).str.strip()
    # df_act 컬럼 정리
    if len(df_act):
        df_act['actual'] = pd.to_numeric(df_act['actual'], errors='coerce').fillna(0)
        for col in ['DeptName','EmpName','CustName','ItemName',
                    'ItemClassLName','ItemClassMName','ItemClassSName','PlanYM']:
            if col in df_act.columns:
                df_act[col] = df_act[col].fillna('미지정').astype(str).str.strip()
        for col in ['Lv1Name','Lv2Name','Lv3Name']:
            if col in df_act.columns:
                df_act[col] = df_act[col].fillna('').astype(str).str.strip()


    def rate(p, a):
        return round(a / p * 100, 1) if p > 0 else None

    def safe_rate(p, a):
        r = rate(p, a)
        return None if (r is None or (isinstance(r, float) and math.isnan(r))) else r

    def build_comparison(group_cols):
        # 계획 데이터가 있을 때만 groupby
        if len(df_plan) and 'planamt' in df_plan.columns:
            p = df_plan.groupby(group_cols)['planamt'].sum().reset_index()
        else:
            p = pd.DataFrame(columns=group_cols + ['planamt'])

        if len(df_act):
            a = df_act.groupby(group_cols)['actual'].sum().reset_index()
            merged = pd.merge(p, a, on=group_cols, how='outer').fillna(0)
        else:
            merged = p.copy()
            merged['actual'] = 0

        merged['planamt'] = merged['planamt'].astype(float)
        merged['actual']  = merged['actual'].astype(float)
        merged['gap']     = (merged['actual'] - merged['planamt']).astype(int)
        merged = merged.sort_values('actual', ascending=False)  # 계획 없으면 실적 기준 정렬
        result = []
        for _, r in merged.iterrows():
            result.append({
                'name':   str(r[group_cols[0]]) if len(group_cols)==1 else '/'.join(str(r[c]) for c in group_cols),
                'plan':   int(r['planamt']),
                'actual': int(r['actual']),
                'rate':   safe_rate(r['planamt'], r['actual']),
                'gap':    int(r['gap']),
            })
        return result

    # 전월 실적 → df_act_wide에서 필터링
    prev_act = {}
    prev_ym_str = ''
    try:
        if date_to:
            dt_to = datetime.strptime(date_to[:7]+'-01', '%Y-%m-%d')
            prev_last  = dt_to - timedelta(days=1)
            prev_first = prev_last.replace(day=1)
            prev_ym_str = prev_first.strftime('%Y%m')
            df_prev = filter_act(df_act_wide, prev_first.strftime('%Y-%m-%d'), prev_last.strftime('%Y-%m-%d'))
            if len(df_prev):
                pg = df_prev.groupby('DeptName')['actual'].sum()
                prev_act = {k: int(v) for k, v in pg.items()}
    except Exception:
        pass

    # 전년동월 실적 → df_act_wide에서 필터링
    yoy_act = {}
    yoy_ym_str = ''
    try:
        if date_to:
            dt_to = datetime.strptime(date_to[:7]+'-01', '%Y-%m-%d')
            try:    yoy_first = dt_to.replace(year=dt_to.year-1)
            except: yoy_first = dt_to.replace(year=dt_to.year-1, day=28)
            last_day = calendar.monthrange(yoy_first.year, yoy_first.month)[1]
            yoy_last = yoy_first.replace(day=last_day)
            yoy_ym_str = yoy_first.strftime('%Y%m')
            df_yoy = filter_act(df_act_wide, yoy_first.strftime('%Y-%m-%d'), yoy_last.strftime('%Y-%m-%d'))
            if len(df_yoy):
                yg = df_yoy.groupby('DeptName')['actual'].sum()
                yoy_act = {k: int(v) for k, v in yg.items()}
    except Exception:
        pass

    # 월별 차트용 연도전체 실적 → df_act_wide에서 필터링
    df_act_year = df_act_year_wide.copy() if len(df_act_year_wide) else pd.DataFrame()

    # 월별 집계 (1~12월 전체 표시)
    monthly = []
    p_m = df_plan_year.groupby('PlanYM')['planamt'].sum() if len(df_plan_year) and 'PlanYM' in df_plan_year.columns else pd.Series(dtype=float)
    a_m = df_act_year.groupby('PlanYM')['actual'].sum() if df_act_year is not None and len(df_act_year) else pd.Series(dtype=float)
    # 조회 연도 범위 전체 월 생성
    all_ym = []
    for y in range(year_from, year_to + 1):
        for m in range(1, 13):
            all_ym.append(str(y) + str(m).zfill(2))
    for ym in all_ym:
        p = float(p_m.get(ym, 0))
        a = float(a_m.get(ym, 0))
        monthly.append({'ym': ym, 'plan': int(p), 'actual': int(a), 'rate': safe_rate(p, a)})

    # 부서별 계획대비 상세 테이블 (전월/전년 포함, 부서만)
    # 부서별 상세표용 - 마지막 월 기준 계획/실적
    def build_last_month(group_cols):
        p_last, _ = make_plan_df(ym_to, ym_to)
        if p_last is None: p_last = pd.DataFrame()
        else:
            p_last['planamt'] = pd.to_numeric(p_last['planamt'], errors='coerce').fillna(0)
            for col in group_cols:
                if col in p_last.columns:
                    p_last[col] = p_last[col].fillna('미지정').astype(str).str.strip()
        pg = p_last.groupby(group_cols)['planamt'].sum().reset_index() if len(p_last) else pd.DataFrame(columns=group_cols+['planamt'])
        ag = df_act_last.groupby(group_cols)['actual'].sum().reset_index() if len(df_act_last) else pd.DataFrame(columns=group_cols+['actual'])
        if len(df_act_last) and group_cols[0] in df_act_last.columns:
            df_act_last[group_cols[0]] = df_act_last[group_cols[0]].fillna('미지정').astype(str).str.strip()
        merged = pd.merge(pg, ag, on=group_cols, how='outer').fillna(0)
        merged['planamt'] = merged['planamt'].astype(float)
        merged['actual']  = merged['actual'].astype(float)
        merged = merged.sort_values('planamt', ascending=False)
        result = []
        for _, r in merged.iterrows():
            result.append({
                'name':   str(r[group_cols[0]]),
                'plan':   int(r['planamt']),
                'actual': int(r['actual']),
                'rate':   safe_rate(r['planamt'], r['actual']),
                'gap':    int(r['actual'] - r['planamt']),
            })
        return result

    dept_table = []
    # dept_lv1_map: DeptName → Lv1Name 매핑 (df_plan 기준으로 미리 생성)
    dept_lv1_map = {}
    if len(df_plan) and 'DeptName' in df_plan.columns and 'Lv1Name' in df_plan.columns:
        for _, row in df_plan[['DeptName','Lv1Name']].drop_duplicates().iterrows():
            k = str(row['DeptName']).strip()
            v = str(row['Lv1Name']).strip()
            if k and k != '미지정':
                dept_lv1_map[k] = v

    dept_comp_last = sort_by_dept(build_last_month(['DeptName']))
    for row in dept_comp_last:
        dname = row['name']
        dept_prev = float(prev_act.get(dname, 0))
        dept_yoy  = float(yoy_act.get(dname, 0))
        dept_table.append({
            'dept':     dname,
            'lv2':      dept_lv1_map.get(dname, ''),  # 소속 본부
            'plan':     row['plan'],
            'actual':   row['actual'],
            'rate':     row['rate'],
            'gap':      row['gap'],
            'prev':     int(dept_prev),
            'prev_diff':int(row['actual'] - dept_prev),
            'prev_rate':safe_rate(dept_prev, row['actual']) if dept_prev > 0 else None,
            'yoy':      int(dept_yoy),
            'yoy_diff': int(row['actual'] - dept_yoy),
            'yoy_rate': safe_rate(dept_yoy, row['actual']) if dept_yoy > 0 else None,
        })

    # ── 본부(Lv1) / 부서(Lv2) / 팀(Lv3) 별 계획대비실적 테이블 생성
    def build_lv_table(lv_col, parent_col=None):
        """Lv1Name/Lv2Name/Lv3Name 기준 계획대비실적 집계
           parent_col: 상위 본부(Lv1Name) 매핑용 컬럼"""
        # 계획 - 선택기간 전체 합산 (ym_from~ym_to)
        if len(df_plan) and lv_col in df_plan.columns and 'planamt' in df_plan.columns:
            pg = df_plan.groupby(lv_col)['planamt'].sum()
        else:
            pg = pd.Series(dtype=float)

        # 실적 - 선택기간 전체 합산
        if len(df_act) and lv_col in df_act.columns:
            ag = df_act.groupby(lv_col)['actual'].sum()
        else:
            ag = pd.Series(dtype=float)

        # 전월 실적
        prev_lv = {}
        try:
            if len(df_prev) and lv_col in df_prev.columns:
                prev_lv = df_prev.groupby(lv_col)['actual'].sum().to_dict()
        except Exception:
            pass

        # 전년동월 실적
        yoy_lv = {}
        try:
            if len(df_yoy) and lv_col in df_yoy.columns:
                yoy_lv = df_yoy.groupby(lv_col)['actual'].sum().to_dict()
        except Exception:
            pass

        # 상위 본부(Lv1) 매핑 테이블 생성 - 계획/실적 모두 활용
        lv1_map = {}
        if parent_col and parent_col != lv_col:
            for df_tmp in [df_plan, df_act]:
                if len(df_tmp) and lv_col in df_tmp.columns and parent_col in df_tmp.columns:
                    for _, row in df_tmp[[lv_col, parent_col]].drop_duplicates().iterrows():
                        k = str(row[lv_col]).strip()
                        v = str(row[parent_col]).strip()
                        if k and k != '미지정' and k != '' and v and v != '':
                            lv1_map[k] = v

        all_names = sorted(set(list(pg.index) + list(ag.index)))
        result = []
        for name in all_names:
            if not name or name.strip() == '' or name == '미지정': continue
            p = float(pg.get(name, 0))
            a = float(ag.get(name, 0))
            pv = float(prev_lv.get(name, 0))
            yy = float(yoy_lv.get(name, 0))
            result.append({
                'dept':      name,
                'lv1':       lv1_map.get(name, ''),  # 소속 본부
                'plan':      int(p),
                'actual':    int(a),
                'rate':      safe_rate(p, a),
                'gap':       int(a - p),
                'prev':      int(pv),
                'prev_diff': int(a - pv),
                'prev_rate': safe_rate(pv, a) if pv > 0 else None,
                'yoy':       int(yy),
                'yoy_diff':  int(a - yy),
                'yoy_rate':  safe_rate(yy, a) if yy > 0 else None,
            })
        result.sort(key=lambda x: org_sort_map.get(x['dept'], 'zzzzzz'))
        return result

    # df_prev, df_yoy 재사용을 위해 미리 계산
    try:
        df_prev = filter_act(df_act_wide, prev_first.strftime('%Y-%m-%d'), prev_last.strftime('%Y-%m-%d')) \
                  if 'prev_first' in locals() else pd.DataFrame()
    except Exception:
        df_prev = pd.DataFrame()
    try:
        df_yoy = filter_act(df_act_wide, yoy_first.strftime('%Y-%m-%d'), yoy_last.strftime('%Y-%m-%d')) \
                 if 'yoy_first' in locals() else pd.DataFrame()
    except Exception:
        df_yoy = pd.DataFrame()

    # ── OrgCd 기준 정렬 맵 조회 (Lv3Name, Lv2Name, Lv1Name → OrgCd)
    org_sort_map = {}  # {이름: OrgCd} - OrgCd 알파벳 순으로 정렬하면 조직도 순서 유지
    try:
        sql_org = f"SELECT Lv1Name, Lv2Name, Lv3Name, OrgCd FROM {ORG_TABLE}"
        df_org, _ = query_df(sql_org)
        if df_org is not None and len(df_org):
            for _, row in df_org.iterrows():
                for col in ['Lv3Name', 'Lv2Name', 'Lv1Name']:
                    name = str(row[col]).strip() if row[col] else ''
                    orgcd = str(row['OrgCd']).strip() if row['OrgCd'] else ''
                    if name and orgcd and name not in org_sort_map:
                        org_sort_map[name] = orgcd
    except Exception:
        pass

    # Lv 컬럼 정리 → build_lv_table 호출 전에 반드시 실행
    for lv_col in ['Lv1Name','Lv2Name','Lv3Name']:
        for df_tmp in [df_plan, df_act, df_act_last, df_prev, df_yoy]:
            if len(df_tmp) and lv_col in df_tmp.columns:
                df_tmp[lv_col] = df_tmp[lv_col].fillna('').astype(str).str.strip()

    lv1_table = build_lv_table('Lv1Name')                            # 본부
    lv2_table = build_lv_table('Lv2Name', parent_col='Lv1Name')      # 부서 (본부 소속)
    lv3_table = build_lv_table('Lv3Name', parent_col='Lv1Name')      # 팀 (본부 소속)

    # 본부 목록 (콤보박스용)
    lv1_list = sorted([r['dept'] for r in lv1_table if r['dept']])

    # 중분류별 소분류 계획/실적 매핑
    mid_small = {}
    if 'ItemClassMName' in df_plan.columns and 'ItemClassSName' in df_plan.columns:
        # 계획 + 실적 합집합 기준으로 중분류 순회
        all_mids = set(df_plan['ItemClassMName'].unique())
        if len(df_act) and 'ItemClassMName' in df_act.columns:
            all_mids |= set(df_act['ItemClassMName'].unique())
        for mid in all_mids:
            sub_plan = df_plan[df_plan['ItemClassMName']==mid] if 'ItemClassMName' in df_plan.columns and mid in df_plan['ItemClassMName'].values else pd.DataFrame(columns=['ItemClassSName','planamt'])
            sub_act  = df_act[df_act['ItemClassMName']==mid]   if len(df_act) and 'ItemClassMName' in df_act.columns else pd.DataFrame(columns=['ItemClassSName','actual'])
            sg = sub_plan.groupby('ItemClassSName')['planamt'].sum() if len(sub_plan) else pd.Series(dtype=float)
            ag = sub_act.groupby('ItemClassSName')['actual'].sum()   if len(sub_act) else pd.Series(dtype=float)
            all_s = sorted(set(list(sg.index)+list(ag.index)))
            rows = []
            for s in all_s:
                p = float(sg.get(s,0)); a = float(ag.get(s,0))
                rows.append({'name':s,'plan':int(p),'actual':int(a),'rate':safe_rate(p,a),'gap':int(a-p)})
            rows.sort(key=lambda x: x['actual'], reverse=True)
            mid_small[mid] = rows

    # 부서별/소분류별 월별 추이 → df_act_wide에서 필터링 (추가 쿼리 없음)
    dept_trend = {}
    small_trend = {}
    try:
        trend_to = date_to if date_to else f"{year_to}-12-31"
        df_trend = filter_act(df_act_wide, wide_from, trend_to)

        if len(df_trend):
            df_trend['ym'] = df_trend['PlanYM'].astype(str).str.strip()
            all_ym_trend = sorted(df_trend['ym'].unique())

            # 전체 합계
            dept_trend['__total__'] = [
                {'ym': ym, 'actual': int(df_trend[df_trend['ym']==ym]['actual'].sum())}
                for ym in all_ym_trend
            ]
            # 팀별 (DeptName)
            for dname in df_trend['DeptName'].unique():
                sub = df_trend[df_trend['DeptName']==dname]
                dept_trend[dname] = [
                    {'ym': ym, 'actual': int(sub[sub['ym']==ym]['actual'].sum())}
                    for ym in all_ym_trend
                ]
            # 본부별 (Lv1Name)
            if 'Lv1Name' in df_trend.columns:
                for lv1name in df_trend['Lv1Name'].dropna().unique():
                    if not str(lv1name).strip(): continue
                    sub = df_trend[df_trend['Lv1Name']==lv1name]
                    dept_trend[f'__lv1__{lv1name}'] = [
                        {'ym': ym, 'actual': int(sub[sub['ym']==ym]['actual'].sum())}
                        for ym in all_ym_trend
                    ]
            # 부서별 (Lv2Name)
            if 'Lv2Name' in df_trend.columns:
                for lv2name in df_trend['Lv2Name'].dropna().unique():
                    if not str(lv2name).strip(): continue
                    sub = df_trend[df_trend['Lv2Name']==lv2name]
                    dept_trend[f'__lv2__{lv2name}'] = [
                        {'ym': ym, 'actual': int(sub[sub['ym']==ym]['actual'].sum())}
                        for ym in all_ym_trend
                    ]
            # 소분류별
            if 'ItemClassSName' in df_trend.columns:
                for sname in df_trend['ItemClassSName'].unique():
                    sub = df_trend[df_trend['ItemClassSName']==sname]
                    small_trend[sname] = [
                        {'ym': ym, 'actual': int(sub[sub['ym']==ym]['actual'].sum())}
                        for ym in all_ym_trend
                    ]

        # 계획도 추가 (작년 1월 ~ 올해 전체)
        plan_trend = {}
        trend_plan_from = str(year_from - 1) + '01'
        trend_plan_to   = str(year_to) + '12'
        plan_t_clauses = [f"BizUnit = {b}",
                          f"PlanYM >= '{trend_plan_from}'",
                          f"PlanYM <= '{trend_plan_to}'"]
        plan_t_where = "WHERE " + " AND ".join(plan_t_clauses)
        sql_pt = (
            f"SELECT PlanYM, DeptName, Lv1Name, Lv2Name, ItemClassSName, SUM(PlanDomAmt) AS planamt "
            f"FROM {PLAN_TABLE} {plan_t_where} "
            f"GROUP BY PlanYM, DeptName, Lv1Name, Lv2Name, ItemClassSName"
        )
        df_pt, _ = query_df(sql_pt)
        p_dept = df_pt if df_pt is not None else pd.DataFrame()
        if len(p_dept):
            p_dept['DeptName'] = p_dept['DeptName'].fillna('미지정').astype(str).str.strip()
            all_plan_ym = sorted(p_dept['PlanYM'].unique())
            plan_trend['__total__'] = [
                {'ym': ym, 'plan': int(p_dept[p_dept['PlanYM']==ym]['planamt'].sum())}
                for ym in all_plan_ym
            ]
            # 팀별 (DeptName)
            for dname in p_dept['DeptName'].unique():
                sub = p_dept[p_dept['DeptName']==dname]
                plan_trend[dname] = [
                    {'ym': ym, 'plan': int(sub[sub['PlanYM']==ym]['planamt'].sum())}
                    for ym in all_plan_ym
                ]
            # 본부별 (Lv1Name)
            if 'Lv1Name' in p_dept.columns:
                for lv1name in p_dept['Lv1Name'].dropna().unique():
                    if not str(lv1name).strip(): continue
                    sub = p_dept[p_dept['Lv1Name']==lv1name]
                    plan_trend[f'__lv1__{lv1name}'] = [
                        {'ym': ym, 'plan': int(sub[sub['PlanYM']==ym]['planamt'].sum())}
                        for ym in all_plan_ym
                    ]
            # 부서별 (Lv2Name)
            if 'Lv2Name' in p_dept.columns:
                for lv2name in p_dept['Lv2Name'].dropna().unique():
                    if not str(lv2name).strip(): continue
                    sub = p_dept[p_dept['Lv2Name']==lv2name]
                    plan_trend[f'__lv2__{lv2name}'] = [
                        {'ym': ym, 'plan': int(sub[sub['PlanYM']==ym]['planamt'].sum())}
                        for ym in all_plan_ym
                    ]
    except Exception as e:
        dept_trend = {}
        plan_trend = {}

    # 소분류별 월별 추이
    small_plan_trend = {}
    try:
        if df_pt is not None and len(df_pt) and 'ItemClassSName' in df_pt.columns:
            df_pt['ItemClassSName'] = df_pt['ItemClassSName'].fillna('미지정').astype(str).str.strip()
            all_pt_ym = sorted(df_pt['PlanYM'].unique())
            for sname in df_pt['ItemClassSName'].unique():
                sub = df_pt[df_pt['ItemClassSName']==sname]
                small_plan_trend[sname] = [
                    {'ym': ym, 'plan': int(sub[sub['PlanYM']==ym]['planamt'].sum())}
                    for ym in all_pt_ym
                ]
    except Exception:
        pass

    return jsonify({
        'monthly':    monthly,
        'dept_table': dept_table,
        'lv1_table':  lv1_table,   # 본부별
        'lv2_table':  lv2_table,   # 부서별
        'lv3_table':  lv3_table,   # 팀별
        'lv1_list':   lv1_list,    # 본부 목록 (콤보박스용)
        'cur_ym':     ym_to,  # 마지막 월
        'prev_ym':    prev_ym_str,
        'yoy_ym':     yoy_ym_str,
        'mid_small':  mid_small,
        'small_trend': small_trend,
        'small_plan_trend': small_plan_trend,
        'dept_trend': dept_trend,
        'plan_trend': plan_trend,
        'dept':       sort_by_dept(build_comparison(['DeptName'])),
        'emp':       build_comparison(['EmpName']),
        'item':      build_comparison(['ItemName']),
        'large':     build_comparison(['ItemClassLName']),
        'mid':       build_comparison(['ItemClassMName']),
        'small':     build_comparison(['ItemClassSName']),
        'cust':      build_comparison(['CustName']),
        'channel':   build_comparison(['ChannelName']),
    })


# ──────────────────────────────────────────────
# /api/news  : 네이버 뉴스 RSS 기반 주류업계 동향
# 파라미터  : kw (키워드, 기본값='주류업계')
# ──────────────────────────────────────────────
@app.route('/api/news')
def get_news():
    kw_map = {
        '전체':    '주류업계 OR 소주 OR 맥주 OR 막걸리',
        '주류업계': '주류업계',
        '소주':    '소주',
        '맥주':    '맥주',
        '막걸리':  '막걸리',
        '위스키':  '위스키',
        '주세법':  '주세법 OR 주류규제',
        '배상면주가': '배상면주가',
        '느린마을':  '느린마을 막걸리',
    }
    raw_kw = request.args.get('kw', '전체')
    keyword = kw_map.get(raw_kw, raw_kw)

    try:
        # 구글 뉴스 RSS (안정적, 한국어 지원)
        encoded_kw = urllib.parse.quote(keyword)
        url = f"https://news.google.com/rss/search?q={encoded_kw}&hl=ko&gl=KR&ceid=KR:ko"
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            xml_data = resp.read()


        root = ET.fromstring(xml_data)
        channel = root.find('channel')
        items = []
        items_xml = channel.findall('item') if channel is not None else []
        for item in items_xml[:20]:
            def txt(tag):
                el = item.find(tag)
                return html.unescape(re.sub(r'<[^>]+>', '', el.text or '')) if el is not None and el.text else ''

            title  = txt('title')
            desc   = txt('description')
            pub    = txt('pubDate')

            # 구글 뉴스 RSS link 추출 (text 또는 tail)
            link = ''
            link_el = item.find('link')
            if link_el is not None:
                link = (link_el.text or '').strip() or (link_el.tail or '').strip()
            if not link:
                link = txt('guid')

            # source 태그 (구글 뉴스 제공)
            src_el = item.find('source')
            if src_el is not None:
                source = (src_el.text or '').strip()
                if not source:
                    source = src_el.get('url', '')
                    try:
                        source = urllib.parse.urlparse(source).hostname or ''
                        source = source.replace('www.', '').split('.')[0]
                    except Exception:
                        pass
            else:
                try:
                    source = urllib.parse.urlparse(link).hostname or ''
                    source = source.replace('www.', '').split('.')[0]
                except Exception:
                    source = ''

            # 날짜 파싱
            try:
                dt = parsedate_to_datetime(pub)
                date_str = dt.strftime('%Y.%m.%d %H:%M')
            except Exception:
                date_str = pub[:16] if pub else ''

            if title:  # link 없어도 title 있으면 표시
                items.append({
                    'title':  title,
                    'link':   link,
                    'desc':   desc[:200] if desc else '',
                    'date':   date_str,
                    'source': source,
                })

        return jsonify({'items': items, 'keyword': keyword})


    except Exception as e:
        return jsonify({'error': f'뉴스 조회 실패: {str(e)}', 'items': []})


if __name__ == '__main__':
    # ── 서버 실행 진입점
    # python app.py 또는 run.bat 실행 시 여기서 시작
    # host='0.0.0.0' → 내부망 전 IP에서 접속 가능
    import socket

    def get_local_ip():
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            return "61.33.23.171"

    print("=" * 55)
    print("  BSM Dashboard  |  DB Edition")
    print("=" * 55)

    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {TABLE}")
        cnt = cursor.fetchone()[0]
        release_connection(conn)  # 풀에 반납 → 첫 API 호출 시 재사용
        print(f"  DB     : {DB_CONFIG['server']}")
        print(f"  Total  : {cnt:,} rows")
        print(f"  Status : Connected OK  (풀 워밍업 완료)")
    except Exception as e:
        print(f"  DB     : {DB_CONFIG['server']}")
        print(f"  Status : FAILED - {e}")

    local_ip = get_local_ip()
    print()
    print(f"  Local  : http://localhost:5000")
    print(f"  Network: http://{local_ip}:5000")
    print("=" * 55)

    # threaded=True: 요청마다 별도 스레드로 처리 → 동시 접속 시 대기 없음
    app.run(debug=False, port=5000, host='0.0.0.0', threaded=True)
