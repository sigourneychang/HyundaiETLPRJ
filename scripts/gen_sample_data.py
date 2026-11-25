#!/usr/bin/env python3
"""자동차 도메인 샘플 데이터를 원본(src) 로컬 클러스터 안에서 바로 만든다 (로컬 테스트 환경 전용).

  DB 20개 x 50개 = 1,000개 객체 (ORC / Parquet / 텍스트, external / managed, 파티션, ACID, 뷰, 빈 테이블).
  데이터는 Hive SQL 로 클러스터 안에서 생성해 ORC·Parquet 로 바로 쓴다 (파일 업로드 없음).
  값은 컬럼마다 시드를 고정한 rand(seed) 로 만들므로 다시 만들어도 같은 데이터가 나온다.
  마지막에 HDFS 사용량을 재서 --target-mb (기본 1024MB) 가 되도록 telematics.trip_log_hist 를 늘린다.
  모델명·VIN 제조사 코드 등은 모두 가상의 값이며 실존 브랜드나 개인정보를 쓰지 않는다.

  python3 scripts/gen_sample_data.py              # 생성 (원본이 비어 있어야 함)
  python3 scripts/gen_sample_data.py --plan       # 테이블 구성만 출력
"""
import argparse
import re
import subprocess
import time
import zlib
from concurrent.futures import ThreadPoolExecutor

NN, HS2 = 'src-namenode', 'src-hiveserver2'
GEN_DB = 'zz_gen'          # 행 번호(0..9999) 테이블을 두는 임시 DB
BASE_TS = 1672531200       # 2023-01-01 00:00:00 UTC

# ----------------------------------------------------------------------------- 값 (Hive SQL 식)
MODELS = [('SD100', 'Aster'), ('SD200', 'Breeze'), ('SU300', 'Cobalt'), ('SU400', 'Dune'), ('HB110', 'Ember'),
          ('EV500', 'Fjord'), ('EV600', 'Glide'), ('TR700', 'Harbor'), ('VN800', 'Ion'), ('CP900', 'Juno')]
TRIMS = ['BASE', 'PLUS', 'PREMIUM', 'SPORT', 'LUXURY']
PLANTS = ['P01', 'P02', 'P03', 'P04', 'P05', 'P06']
COLORS = ['WHITE', 'BLACK', 'SILVER', 'GRAY', 'RED', 'BLUE', 'GREEN']
ENGINES = ['G1.6T', 'G2.0', 'G2.5T', 'D2.2', 'HEV1.6', 'EV160KW', 'EV225KW']
FUELS = ['GASOLINE', 'DIESEL', 'HYBRID', 'EV', 'LPG']
REGIONS = ['SEOUL', 'GYEONGGI', 'BUSAN', 'DAEGU', 'GWANGJU', 'DAEJEON', 'ULSAN', 'JEJU']
PARTS = ['BRAKE PAD', 'OIL FILTER', 'AIR FILTER', 'WIPER BLADE', 'BATTERY 12V', 'SPARK PLUG', 'TIRE 18IN',
         'HEADLAMP', 'RADIATOR', 'ALTERNATOR', 'STARTER MOTOR', 'CLUTCH', 'CABIN FILTER', 'SHOCK ABSORBER',
         'CV JOINT', 'WATER PUMP', 'TIMING BELT', 'FUEL INJECTOR', 'ABS SENSOR', 'DOOR MODULE']
SEVERITY = ['LOW', 'MID', 'HIGH', 'CRITICAL']
STATUS = ['OPEN', 'IN_PROGRESS', 'CLOSED', 'CANCELLED']
CHANNELS = ['ONLINE', 'DEALER', 'FLEET', 'EXPORT']
VINCH = 'ABCDEFGHJKLMNPRSTUVWXYZ0123456789'
MONTHS = ['%d-%02d' % (y, m) for y in (2025, 2026) for m in range(1, 13)][:22]   # 2025-01 .. 2026-10

def H(salt, mod):
    """0..mod-1 난수. 컬럼마다 시드가 다른 rand(seed) 라 컬럼끼리 독립이고, 같은 입력이면 같은 값이 다시 나옴.
    (hash(id, ...) 는 31*id + c 꼴이라 값이 행 번호를 따라 규칙적으로 변하고 컬럼끼리 상관이 생겨 쓰지 않음)"""
    return "cast(floor(rand(%d) * %d) as int)" % (zlib.crc32(salt.encode('utf-8')) & 0x7fffffff, mod)


def pick(values, salt):
    return "array(%s)[%s]" % (', '.join("'%s'" % v for v in values), H(salt, len(values)))


def rint(lo, hi, salt):
    return "cast(%s + %d as int)" % (H(salt, hi - lo + 1), lo)


def rdbl(lo, hi, nd, salt):
    return "round(%s / 1000000.0 * %s + %s, %d)" % (H(salt, 1000000), hi - lo, lo, nd)


def rid(prefix, salt, width=9):
    return "concat('%s', lpad(cast(%s as string), %d, '0'))" % (prefix, H(salt, 10 ** width - 1), width)


def rdate(salt):
    return "cast(date_add('2023-01-01', %s) as string)" % H(salt, 1396)


def rts(salt):
    return "from_unixtime(%d + %s)" % (BASE_TS, H(salt, 120000000))


# 컬럼 키 -> (Hive 타입, salt 를 받아 SQL 식을 돌려주는 함수)
COL = {
    'vin': ('string', lambda s: "concat('ZZA', upper(substr(md5(concat(cast(id as string), '%s')), 1, 14)))" % s),
    'model_code': ('string', lambda s: pick([m[0] for m in MODELS], s)),
    'model_name': ('string', lambda s: pick([m[1] for m in MODELS], s)),
    'trim_level': ('string', lambda s: pick(TRIMS, s)),
    'plant_code': ('string', lambda s: pick(PLANTS, s)),
    'line_no': ('int', lambda s: rint(1, 12, s)),
    'prod_date': ('string', rdate),
    'color': ('string', lambda s: pick(COLORS, s)),
    'engine_type': ('string', lambda s: pick(ENGINES, s)),
    'fuel_type': ('string', lambda s: pick(FUELS, s)),
    'region': ('string', lambda s: pick(REGIONS, s)),
    'dealer_id': ('string', lambda s: rid('D', s, 4)),
    'customer_no': ('string', lambda s: rid('C', s, 8)),
    'price_krw': ('bigint', lambda s: "cast((%s + 180) * 100000 as bigint)" % H(s, 721)),
    'qty': ('int', lambda s: rint(1, 50, s)),
    'part_no': ('string', lambda s: rid('PN-', s, 6)),
    'part_name': ('string', lambda s: pick(PARTS, s)),
    'supplier_id': ('string', lambda s: rid('S', s, 3)),
    'defect_code': ('string', lambda s: rid('DF', s, 3)),
    'severity': ('string', lambda s: pick(SEVERITY, s)),
    'status': ('string', lambda s: pick(STATUS, s)),
    'channel': ('string', lambda s: pick(CHANNELS, s)),
    'mileage_km': ('int', lambda s: rint(0, 250000, s)),
    'speed_kmh': ('double', lambda s: rdbl(0, 180, 1, s)),
    'rpm': ('int', lambda s: rint(700, 6500, s)),
    'soc_pct': ('double', lambda s: rdbl(0, 100, 1, s)),
    'cell_temp_c': ('double', lambda s: rdbl(-5, 45, 1, s)),
    'voltage_v': ('double', lambda s: rdbl(330, 800, 2, s)),
    'current_a': ('double', lambda s: rdbl(-150, 250, 1, s)),
    'energy_kwh': ('double', lambda s: rdbl(0, 2000, 2, s)),
    'lat': ('double', lambda s: rdbl(33.2, 38.5, 5, s)),
    'lon': ('double', lambda s: rdbl(126.1, 129.5, 5, s)),
    'co2_gkm': ('double', lambda s: rdbl(0, 220, 1, s)),
    'nox_mgkm': ('double', lambda s: rdbl(0, 80, 1, s)),
    'amount_krw': ('bigint', lambda s: "cast((%s + 1) * 10000 as bigint)" % H(s, 5000)),
    'rate_pct': ('double', lambda s: rdbl(0, 9, 2, s)),
    'term_month': ('int', lambda s: "array(12, 24, 36, 48, 60)[%s]" % H(s, 5)),
    'score': ('double', lambda s: rdbl(0, 100, 2, s)),
    'duration_min': ('int', lambda s: rint(1, 600, s)),
    'event_ts': ('string', rts),
    'event_date': ('string', rdate),
    # 이력 / 집계 / 오류 전용
    'stat_date': ('string', rdate),
    'stat_month': ('string', lambda s: pick(MONTHS, s)),
    'cnt': ('bigint', lambda s: "cast(%s + 1 as bigint)" % H(s, 5000)),
    'sum_amount': ('bigint', lambda s: "cast(%s as bigint)" % H(s, 1000000000)),
    'avg_value': ('double', lambda s: rdbl(0, 1000, 3, s)),
    'changed_at': ('string', rts),
    'change_type': ('string', lambda s: pick(['I', 'U', 'D'], s)),
    'err_code': ('string', lambda s: rid('E', s, 3)),
    'err_msg': ('string', lambda s: pick(['MISSING VIN', 'INVALID DATE', 'UNKNOWN DEALER', 'DUPLICATE KEY',
                                          'OUT OF RANGE', 'BAD ENCODING'], s)),
    'err_ts': ('string', rts),
}
for _k, _p in (('order_id', 'OR'), ('wo_id', 'WO'), ('lot_id', 'LT'), ('inspection_id', 'IN'), ('contract_id', 'CT'),
               ('quote_id', 'QT'), ('ticket_id', 'TK'), ('claim_id', 'CL'), ('recall_id', 'RC'), ('campaign_id', 'CP'),
               ('shipment_id', 'SH'), ('trip_id', 'TR'), ('test_id', 'TS'), ('pack_id', 'PK'), ('loan_id', 'LN'),
               ('lead_id', 'LD'), ('booking_id', 'BK'), ('meter_id', 'MT'), ('equip_id', 'EQ'), ('project_id', 'PJ'),
               ('po_id', 'PO'), ('asn_id', 'AS'), ('visit_id', 'VS'), ('cert_id', 'CE'), ('event_id', 'EV')):
    COL[_k] = ('string', (lambda p: lambda s: rid(p, s))(_p))

# ----------------------------------------------------------------------------- DB / 엔티티
V = ['vin', 'model_code', 'trim_level']
DBS = {
    'vehicle_master': [('vehicle', V + ['model_name', 'color', 'engine_type', 'fuel_type', 'plant_code', 'prod_date']),
                       ('model_spec', ['model_code', 'model_name', 'engine_type', 'fuel_type', 'price_krw']),
                       ('option_pkg', ['model_code', 'trim_level', 'part_no', 'price_krw']),
                       ('color_code', ['model_code', 'color', 'price_krw']),
                       ('vin_assign', V + ['plant_code', 'line_no', 'prod_date']),
                       ('model_year', ['model_code', 'event_date', 'status']),
                       ('engine_spec', ['engine_type', 'fuel_type', 'co2_gkm']),
                       ('trim_spec', ['model_code', 'trim_level', 'price_krw', 'status']),
                       ('market_spec', ['model_code', 'region', 'price_krw', 'status']),
                       ('vehicle_status', V + ['status', 'region', 'event_ts'])],
    'production': [('work_order', ['wo_id'] + V + ['plant_code', 'line_no', 'prod_date', 'status']),
                   ('line_event', ['event_id', 'plant_code', 'line_no', 'event_ts', 'duration_min', 'status']),
                   ('body_shop', ['vin', 'plant_code', 'line_no', 'event_ts', 'score']),
                   ('paint_shop', ['vin', 'color', 'plant_code', 'event_ts', 'score']),
                   ('assembly', ['vin', 'plant_code', 'line_no', 'part_no', 'event_ts']),
                   ('lot_trace', ['lot_id', 'part_no', 'supplier_id', 'vin', 'event_ts']),
                   ('downtime', ['event_id', 'plant_code', 'line_no', 'duration_min', 'event_date']),
                   ('shift_output', ['plant_code', 'line_no', 'event_date', 'qty', 'model_code']),
                   ('rework', ['vin', 'defect_code', 'plant_code', 'duration_min', 'event_ts']),
                   ('eol_test', ['vin', 'model_code', 'score', 'status', 'event_ts'])],
    'quality': [('inspection', ['inspection_id', 'vin', 'plant_code', 'defect_code', 'severity', 'event_ts']),
                ('defect', ['defect_code', 'part_no', 'severity', 'status', 'event_date']),
                ('audit', ['inspection_id', 'plant_code', 'model_code', 'score', 'event_date']),
                ('field_issue', ['ticket_id', 'vin', 'defect_code', 'mileage_km', 'region', 'event_date']),
                ('supplier_ppm', ['supplier_id', 'part_no', 'qty', 'score', 'event_date']),
                ('test_result', ['test_id', 'vin', 'score', 'status', 'event_ts']),
                ('gauge_cal', ['equip_id', 'plant_code', 'score', 'event_date']),
                ('ncr', ['ticket_id', 'part_no', 'supplier_id', 'severity', 'status']),
                ('spc_sample', ['plant_code', 'line_no', 'score', 'event_ts']),
                ('quality_cost', ['plant_code', 'model_code', 'amount_krw', 'event_date'])],
    'sales': [('contract', ['contract_id'] + V + ['dealer_id', 'customer_no', 'price_krw', 'channel', 'region', 'event_date']),
              ('quote', ['quote_id', 'model_code', 'trim_level', 'dealer_id', 'price_krw', 'event_date']),
              ('order_book', ['order_id', 'model_code', 'color', 'dealer_id', 'status', 'event_date']),
              ('delivery', ['contract_id', 'vin', 'dealer_id', 'region', 'event_date']),
              ('discount', ['contract_id', 'amount_krw', 'channel', 'event_date']),
              ('trade_in', ['contract_id', 'model_code', 'mileage_km', 'amount_krw']),
              ('fleet_deal', ['contract_id', 'customer_no', 'qty', 'amount_krw', 'region']),
              ('export_order', ['order_id', 'model_code', 'region', 'qty', 'event_date']),
              ('sales_target', ['dealer_id', 'model_code', 'qty', 'event_date']),
              ('incentive', ['dealer_id', 'amount_krw', 'event_date', 'status'])],
    'dealer': [('dealer', ['dealer_id', 'region', 'status', 'event_date']),
               ('showroom_visit', ['visit_id', 'dealer_id', 'customer_no', 'model_code', 'event_ts']),
               ('stock', ['dealer_id', 'vin', 'model_code', 'color', 'status']),
               ('staff', ['dealer_id', 'event_date', 'qty', 'status']),
               ('dealer_kpi', ['dealer_id', 'score', 'event_date', 'region']),
               ('demo_car', ['dealer_id', 'vin', 'mileage_km', 'status']),
               ('dealer_claim', ['claim_id', 'dealer_id', 'amount_krw', 'status']),
               ('training', ['dealer_id', 'duration_min', 'score', 'event_date']),
               ('display_car', ['dealer_id', 'vin', 'model_code', 'event_date']),
               ('dealer_contract', ['dealer_id', 'region', 'amount_krw', 'event_date'])],
    'customer': [('customer', ['customer_no', 'region', 'channel', 'event_date']),
                 ('ownership', ['customer_no', 'vin', 'model_code', 'event_date']),
                 ('consent', ['customer_no', 'channel', 'status', 'event_ts']),
                 ('contact_hist', ['customer_no', 'channel', 'event_ts', 'duration_min']),
                 ('membership', ['customer_no', 'score', 'status', 'event_date']),
                 ('survey', ['customer_no', 'model_code', 'score', 'event_date']),
                 ('complaint', ['ticket_id', 'customer_no', 'vin', 'severity', 'status']),
                 ('app_login', ['customer_no', 'event_ts', 'channel']),
                 ('referral', ['customer_no', 'dealer_id', 'event_date']),
                 ('segment', ['customer_no', 'region', 'score'])],
    'after_service': [('repair_order', ['ticket_id', 'vin', 'dealer_id', 'mileage_km', 'amount_krw', 'event_date']),
                      ('repair_part', ['ticket_id', 'part_no', 'part_name', 'qty', 'amount_krw']),
                      ('labor', ['ticket_id', 'duration_min', 'amount_krw', 'status']),
                      ('booking', ['booking_id', 'customer_no', 'dealer_id', 'event_ts']),
                      ('roadside', ['ticket_id', 'vin', 'lat', 'lon', 'event_ts']),
                      ('maintenance_plan', ['vin', 'mileage_km', 'part_name', 'event_date']),
                      ('tech_bulletin', ['defect_code', 'model_code', 'status', 'event_date']),
                      ('diag_code', ['vin', 'defect_code', 'mileage_km', 'event_ts']),
                      ('loaner', ['ticket_id', 'vin', 'duration_min', 'status']),
                      ('csi', ['ticket_id', 'dealer_id', 'score', 'event_date'])],
    'warranty': [('claim', ['claim_id', 'vin', 'part_no', 'defect_code', 'mileage_km', 'amount_krw', 'event_date']),
                 ('coverage', ['vin', 'model_code', 'mileage_km', 'event_date', 'status']),
                 ('claim_part', ['claim_id', 'part_no', 'qty', 'amount_krw']),
                 ('supplier_recovery', ['claim_id', 'supplier_id', 'amount_krw', 'status']),
                 ('extended', ['vin', 'customer_no', 'amount_krw', 'term_month']),
                 ('reject', ['claim_id', 'defect_code', 'status', 'event_date']),
                 ('goodwill', ['claim_id', 'amount_krw', 'event_date']),
                 ('claim_audit', ['claim_id', 'score', 'status']),
                 ('cost_forecast', ['model_code', 'amount_krw', 'event_date']),
                 ('policy', ['model_code', 'mileage_km', 'term_month', 'status'])],
    'recall': [('recall', ['recall_id', 'model_code', 'defect_code', 'severity', 'event_date', 'status']),
               ('target_vehicle', ['recall_id', 'vin', 'region', 'status']),
               ('notice', ['recall_id', 'customer_no', 'channel', 'event_ts']),
               ('remedy', ['recall_id', 'vin', 'dealer_id', 'event_date']),
               ('authority_report', ['recall_id', 'status', 'event_date']),
               ('campaign', ['campaign_id', 'recall_id', 'qty', 'status']),
               ('part_supply', ['recall_id', 'part_no', 'qty', 'event_date']),
               ('completion', ['recall_id', 'region', 'score', 'event_date']),
               ('ota_fix', ['recall_id', 'vin', 'status', 'event_ts']),
               ('root_cause', ['recall_id', 'defect_code', 'supplier_id', 'status'])],
    'parts': [('part', ['part_no', 'part_name', 'supplier_id', 'price_krw', 'status']),
              ('bom', ['model_code', 'part_no', 'qty', 'status']),
              ('inventory', ['part_no', 'plant_code', 'qty', 'event_date']),
              ('price_hist', ['part_no', 'price_krw', 'event_date']),
              ('ecn', ['part_no', 'status', 'event_date']),
              ('kit', ['part_no', 'model_code', 'qty']),
              ('aftermarket', ['part_no', 'dealer_id', 'qty', 'amount_krw']),
              ('substitution', ['part_no', 'status', 'event_date']),
              ('packaging', ['part_no', 'qty', 'status']),
              ('obsolete', ['part_no', 'event_date', 'status'])],
    'supply_chain': [('purchase_order', ['po_id', 'supplier_id', 'part_no', 'qty', 'amount_krw', 'event_date']),
                     ('asn', ['asn_id', 'po_id', 'supplier_id', 'qty', 'event_ts']),
                     ('supplier', ['supplier_id', 'region', 'score', 'status']),
                     ('receiving', ['asn_id', 'plant_code', 'qty', 'event_ts']),
                     ('forecast', ['part_no', 'plant_code', 'qty', 'event_date']),
                     ('shortage', ['part_no', 'plant_code', 'qty', 'severity']),
                     ('invoice', ['po_id', 'amount_krw', 'status', 'event_date']),
                     ('lead_time', ['supplier_id', 'part_no', 'duration_min']),
                     ('risk', ['supplier_id', 'score', 'severity', 'event_date']),
                     ('contract_price', ['supplier_id', 'part_no', 'price_krw', 'event_date'])],
    'logistics': [('shipment', ['shipment_id', 'vin', 'plant_code', 'region', 'event_ts', 'status']),
                  ('carrier', ['shipment_id', 'amount_krw', 'duration_min']),
                  ('yard', ['vin', 'plant_code', 'event_ts', 'status']),
                  ('vessel', ['shipment_id', 'region', 'qty', 'event_date']),
                  ('damage', ['shipment_id', 'vin', 'severity', 'amount_krw']),
                  ('route', ['shipment_id', 'lat', 'lon', 'event_ts']),
                  ('eta', ['shipment_id', 'event_ts', 'duration_min']),
                  ('port_stock', ['region', 'model_code', 'qty', 'event_date']),
                  ('truck', ['shipment_id', 'qty', 'mileage_km', 'event_date']),
                  ('pod', ['shipment_id', 'dealer_id', 'event_ts'])],
    'telematics': [('trip_log', ['trip_id', 'vin', 'event_ts', 'speed_kmh', 'rpm', 'lat', 'lon', 'mileage_km', 'soc_pct']),
                   ('dtc', ['vin', 'defect_code', 'severity', 'event_ts']),
                   ('ota', ['vin', 'status', 'event_ts']),
                   ('driving_score', ['vin', 'score', 'event_date']),
                   ('geofence', ['vin', 'lat', 'lon', 'event_ts']),
                   ('remote_cmd', ['vin', 'status', 'event_ts', 'channel']),
                   ('eco_drive', ['vin', 'co2_gkm', 'score', 'event_date']),
                   ('crash_event', ['vin', 'speed_kmh', 'severity', 'lat', 'lon', 'event_ts']),
                   ('subscription', ['vin', 'customer_no', 'status', 'term_month']),
                   ('can_signal', ['vin', 'rpm', 'speed_kmh', 'event_ts'])],
    'ev_battery': [('cell_monitor', ['pack_id', 'vin', 'soc_pct', 'cell_temp_c', 'voltage_v', 'current_a', 'event_ts']),
                   ('pack', ['pack_id', 'vin', 'supplier_id', 'energy_kwh', 'prod_date']),
                   ('charge_session', ['vin', 'energy_kwh', 'duration_min', 'region', 'event_ts']),
                   ('soh', ['pack_id', 'score', 'event_date']),
                   ('thermal_alert', ['pack_id', 'cell_temp_c', 'severity', 'event_ts']),
                   ('charger', ['equip_id', 'region', 'status', 'event_date']),
                   ('bms_sw', ['pack_id', 'status', 'event_date']),
                   ('second_life', ['pack_id', 'score', 'status']),
                   ('recycle', ['pack_id', 'energy_kwh', 'event_date']),
                   ('warranty_soh', ['pack_id', 'vin', 'score', 'mileage_km'])],
    'emission': [('cert_test', ['cert_id', 'model_code', 'co2_gkm', 'nox_mgkm', 'event_date']),
                 ('rde_test', ['test_id', 'vin', 'co2_gkm', 'nox_mgkm', 'event_ts']),
                 ('fleet_co2', ['model_code', 'qty', 'co2_gkm', 'event_date']),
                 ('credit', ['region', 'amount_krw', 'event_date']),
                 ('approval', ['cert_id', 'region', 'status', 'event_date']),
                 ('fuel_economy', ['model_code', 'engine_type', 'score']),
                 ('obd_monitor', ['vin', 'defect_code', 'status', 'event_ts']),
                 ('evap_test', ['test_id', 'model_code', 'score', 'event_date']),
                 ('noise_test', ['test_id', 'model_code', 'score']),
                 ('regulation', ['region', 'status', 'event_date'])],
    'test_drive': [('booking', ['booking_id', 'customer_no', 'dealer_id', 'model_code', 'event_ts']),
                   ('drive_log', ['booking_id', 'vin', 'duration_min', 'mileage_km', 'event_ts']),
                   ('feedback', ['booking_id', 'score', 'status']),
                   ('conversion', ['booking_id', 'contract_id', 'event_date']),
                   ('route_plan', ['dealer_id', 'lat', 'lon', 'duration_min']),
                   ('fleet_car', ['vin', 'dealer_id', 'mileage_km', 'status']),
                   ('no_show', ['booking_id', 'event_date']),
                   ('event_drive', ['campaign_id', 'region', 'qty', 'event_date']),
                   ('insurance', ['booking_id', 'amount_krw', 'status']),
                   ('damage_report', ['booking_id', 'vin', 'severity', 'amount_krw'])],
    'finance': [('loan', ['loan_id', 'contract_id', 'customer_no', 'amount_krw', 'rate_pct', 'term_month', 'event_date']),
                ('lease', ['loan_id', 'vin', 'amount_krw', 'term_month', 'status']),
                ('payment', ['loan_id', 'amount_krw', 'event_date', 'status']),
                ('credit_score', ['customer_no', 'score', 'event_date']),
                ('delinquency', ['loan_id', 'amount_krw', 'duration_min', 'status']),
                ('residual', ['model_code', 'term_month', 'rate_pct']),
                ('insurance_link', ['loan_id', 'amount_krw', 'status']),
                ('promo_rate', ['model_code', 'rate_pct', 'event_date']),
                ('write_off', ['loan_id', 'amount_krw', 'event_date']),
                ('collection', ['loan_id', 'channel', 'status', 'event_ts'])],
    'marketing': [('campaign', ['campaign_id', 'model_code', 'channel', 'amount_krw', 'event_date']),
                  ('impression', ['campaign_id', 'channel', 'qty', 'event_ts']),
                  ('lead', ['lead_id', 'campaign_id', 'customer_no', 'model_code', 'event_ts']),
                  ('click', ['campaign_id', 'channel', 'event_ts']),
                  ('event_attend', ['campaign_id', 'customer_no', 'region']),
                  ('sns_mention', ['model_code', 'score', 'event_date']),
                  ('price_promo', ['model_code', 'amount_krw', 'event_date', 'status']),
                  ('brand_survey', ['region', 'score', 'event_date']),
                  ('media_spend', ['channel', 'amount_krw', 'event_date']),
                  ('attribution', ['lead_id', 'contract_id', 'channel', 'score'])],
    'plant_energy': [('meter_reading', ['meter_id', 'plant_code', 'energy_kwh', 'event_ts']),
                     ('equipment', ['equip_id', 'plant_code', 'line_no', 'status']),
                     ('alarm', ['equip_id', 'severity', 'event_ts', 'status']),
                     ('maintenance', ['equip_id', 'duration_min', 'amount_krw', 'event_date']),
                     ('water', ['plant_code', 'qty', 'event_date']),
                     ('waste', ['plant_code', 'qty', 'event_date']),
                     ('carbon', ['plant_code', 'co2_gkm', 'event_date']),
                     ('hvac', ['plant_code', 'cell_temp_c', 'event_ts']),
                     ('compressor', ['equip_id', 'energy_kwh', 'event_ts']),
                     ('solar', ['plant_code', 'energy_kwh', 'event_date'])],
    'rnd_test': [('test_plan', ['test_id', 'project_id', 'model_code', 'status', 'event_date']),
                 ('crash_test', ['test_id', 'model_code', 'speed_kmh', 'score', 'event_date']),
                 ('durability', ['test_id', 'vin', 'mileage_km', 'status']),
                 ('climate_test', ['test_id', 'cell_temp_c', 'score', 'event_ts']),
                 ('nvh', ['test_id', 'rpm', 'score']),
                 ('sim_run', ['project_id', 'duration_min', 'status', 'event_ts']),
                 ('prototype', ['vin', 'project_id', 'status', 'event_date']),
                 ('sensor_log', ['test_id', 'speed_kmh', 'rpm', 'event_ts']),
                 ('issue', ['ticket_id', 'project_id', 'severity', 'status']),
                 ('milestone', ['project_id', 'event_date', 'status'])],
}

# ----------------------------------------------------------------------------- 테이블 구성
def h(*parts):
    """실행할 때마다 같은 값 (파이썬 hash() 는 프로세스마다 달라짐)"""
    return zlib.crc32('|'.join(parts).encode('utf-8'))


def rng(lo, hi, *key):
    return lo + h(*key) % (hi - lo + 1)


BIG_HIST_ROWS = {'telematics': 6000000}   # DB 대표 이력 테이블 행 수 (그 외 DB는 DEFAULT)
DEFAULT_BIG_ROWS = 500000


def plan():
    """[(db, table, spec)]  spec: kind(managed/external/view) fmt(ORC/PARQUET/TEXT/ACID) cols part rows"""
    out = []
    for db, ents in DBS.items():
        for e, (ent, cols) in enumerate(ents):
            base = list(dict.fromkeys(cols))
            # 1) 기본 테이블 (managed)
            if e == 0:
                fmt, part = 'ACID', None
            elif e % 3 == 1:
                fmt, part = 'ORC', 'region'
            elif e % 3 == 2:
                fmt, part = 'PARQUET', None
            else:
                fmt, part = 'ORC', None
            bcols = [c for c in base if c != part]
            out.append((db, ent, dict(kind='managed', fmt=fmt, cols=bcols, part=part,
                                      rows=5000 if e == 0 else rng(5000, 50000, db, ent))))
            # 2) 이력: external, 월(dt) 파티션. ORC / Parquet, 일부 텍스트. 첫 엔티티가 DB의 큰 테이블
            hfmt = 'TEXT' if e in (3, 7) else ('ORC' if e % 2 == 0 else 'PARQUET')
            out.append((db, ent + '_hist', dict(kind='external', fmt=hfmt, cols=base + ['changed_at', 'change_type'],
                                                part='dt', rows=BIG_HIST_ROWS.get(db, DEFAULT_BIG_ROWS) if e == 0
                                                else rng(20000, 60000, db, ent, 'h'))))
            # 3) 일별 집계: managed Parquet / ORC
            out.append((db, ent + '_agg_daily', dict(kind='managed', fmt='PARQUET' if e % 2 == 0 else 'ORC',
                                                     cols=['stat_date', 'region', 'model_code', 'cnt', 'sum_amount',
                                                           'avg_value'], part=None, rows=rng(300, 3000, db, ent, 'd'))))
            # 4) 월별 집계: managed 텍스트 (마지막 두 엔티티는 뷰)
            if e in (8, 9):
                out.append((db, 'v_' + ent + '_summary', dict(kind='view', base=ent, by_model='model_code' in bcols)))
            else:
                out.append((db, ent + '_agg_monthly', dict(kind='managed', fmt='TEXT',
                                                           cols=['stat_month', 'model_code', 'cnt', 'sum_amount'],
                                                           part=None, rows=rng(50, 500, db, ent, 'm'))))
            # 5) 오류 적재분: external 텍스트 (세 엔티티는 빈 테이블)
            out.append((db, ent + '_err', dict(kind='external', fmt='TEXT', cols=base + ['err_code', 'err_msg', 'err_ts'],
                                               part=None, rows=0 if e in (5, 6, 7) else rng(10, 300, db, ent, 'e'))))
    return out


# ----------------------------------------------------------------------------- SQL
def ddl(db, tbl, s):
    fq = '`%s`.`%s`' % (db, tbl)
    cols = ',\n  '.join('`%s` %s' % (c, COL[c][0]) for c in s['cols'])
    part = '\nPARTITIONED BY (`%s` string)' % s['part'] if s['part'] else ''
    if s['fmt'] == 'ACID':
        return ("CREATE TABLE %s (\n  %s)\nCLUSTERED BY (`%s`) INTO 2 BUCKETS STORED AS ORC "
                "TBLPROPERTIES ('transactional'='true')" % (fq, cols, s['cols'][0]))
    store = {'ORC': 'STORED AS ORC', 'PARQUET': "STORED AS PARQUET",
             'TEXT': "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' STORED AS TEXTFILE"}[s['fmt']]
    props = "\nTBLPROPERTIES ('parquet.compression'='SNAPPY')" if s['fmt'] == 'PARQUET' else ''
    ext = 'EXTERNAL ' if s['kind'] == 'external' else ''
    loc = "\nLOCATION '/data/%s/%s'" % (db, tbl) if s['kind'] == 'external' else ''
    return 'CREATE %sTABLE %s (\n  %s)%s\n%s%s%s' % (ext, fq, cols, part, store, loc, props)


def id_source(n, offset=0):
    """id = offset .. offset+n-1, 행 번호 테이블(0..9999)에서 만듦 (n > 10000 이면 자기 자신과 cross join)"""
    if n <= 10000:
        return 'SELECT n + %d AS id FROM %s.nums WHERE n < %d' % (offset, GEN_DB, n)
    return ('SELECT a.n * 10000 + b.n + %d AS id FROM %s.nums a CROSS JOIN %s.nums b '
            'WHERE a.n < %d AND a.n * 10000 + b.n < %d' % (offset, GEN_DB, GEN_DB, -(-n // 10000), n))


def insert(db, tbl, s, rows=None, offset=0):
    fq = '`%s`.`%s`' % (db, tbl)
    sel = ',\n  '.join('%s AS `%s`' % (COL[c][1]('%s.%s.%s' % (db, tbl, c)), c) for c in s['cols'])
    if s['part'] == 'dt':
        sel += ",\n  %s AS `dt`" % COL['stat_month'][1]('%s.%s.dt' % (db, tbl))
    elif s['part']:
        sel += ',\n  %s AS `%s`' % (COL[s['part']][1]('%s.%s.%s' % (db, tbl, s['part'])), s['part'])
    verb = 'INSERT INTO' if s['fmt'] == 'ACID' or offset else 'INSERT OVERWRITE'
    return '%s TABLE %s%s\nSELECT\n  %s\nFROM (%s) g' % (verb, fq, ' PARTITION (`%s`)' % s['part'] if s['part'] else '',
                                                    sel, id_source(rows or s['rows'], offset))


SESSION = ['SET hive.exec.dynamic.partition=true', 'SET hive.exec.dynamic.partition.mode=nonstrict',
           'SET hive.exec.max.dynamic.partitions=10000', 'SET hive.exec.max.dynamic.partitions.pernode=10000',
           'SET hive.strict.checks.cartesian.product=false', 'SET hive.mapred.mode=nonstrict',
           'SET hive.auto.convert.join=true', 'SET parquet.compression=SNAPPY']


def db_sql(db, items):
    st = SESSION + ["CREATE DATABASE IF NOT EXISTS `%s` COMMENT 'sample automotive data'" % db]
    views = []
    for _, tbl, s in items:
        if s['kind'] == 'view':
            src = '`%s`.`%s`' % (db, s['base'])
            views.append("CREATE VIEW `%s`.`%s` AS SELECT %s FROM %s%s" % (
                db, tbl, 'model_code, count(*) AS cnt' if s['by_model'] else 'count(*) AS cnt', src,
                ' GROUP BY model_code' if s['by_model'] else ''))
            continue
        st.append(ddl(db, tbl, s))
        if s['rows']:
            st.append(insert(db, tbl, s))
        elif s['kind'] == 'external':   # 빈 external 테이블도 LOCATION 폴더는 있어야 함
            st.append("dfs -mkdir -p /data/%s/%s" % (db, tbl))
    return ';\n'.join(st + views) + ';\n'


def beeline(sql, check=True):
    p = subprocess.run(['docker', 'exec', '-i', HS2, '/opt/hive/bin/beeline', '-u', 'jdbc:hive2://localhost:10000/',
                        '--silent=true', '--outputformat=tsv2', '--showHeader=false', '-f', '/dev/stdin'],
                       input=sql, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    if check and p.returncode != 0:
        err = [l for l in p.stderr.splitlines() if 'Error' in l or 'FAILED' in l]
        raise SystemExit('beeline failed: %s' % ('\n'.join(err[-3:]) or p.stderr[-1500:]))
    return p.stdout


def hdfs_mb():
    out = subprocess.run(['docker', 'exec', NN, 'hdfs', 'dfs', '-du', '-s', '/data', '/user/hive/warehouse'],
                         stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, universal_newlines=True).stdout
    return sum(int(l.split()[0]) for l in out.splitlines() if l.split() and l.split()[0].isdigit()) / 1048576


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--parallel', type=int, default=6, help='DBs generated at the same time')
    ap.add_argument('--target-mb', type=int, default=1024, help='HDFS total to reach (top up telematics.trip_log_hist)')
    ap.add_argument('--plan', action='store_true', help='print the table layout only')
    a = ap.parse_args()
    items = plan()
    if a.plan:
        from collections import Counter
        c = Counter((s['kind'], s.get('fmt', ''), s.get('part') or '') for _, _, s in items)
        print('%d DBs, %d objects, %d rows planned' % (len(DBS), len(items), sum(s.get('rows', 0) for _, _, s in items)))
        for k, v in sorted(c.items()):
            print('  %-9s %-8s part=%-7s %4d' % (k[0], k[1], k[2], v))
        return

    t0 = time.time()
    beeline('CREATE DATABASE IF NOT EXISTS %s;\nCREATE TABLE IF NOT EXISTS %s.nums (n int) STORED AS ORC;\n'
            "INSERT OVERWRITE TABLE %s.nums SELECT pos FROM (SELECT 1 AS x) t "
            "LATERAL VIEW posexplode(split(space(9999), ' ')) p AS pos, v;\n" % (GEN_DB, GEN_DB, GEN_DB))
    by_db = {}
    for it in items:
        by_db.setdefault(it[0], []).append(it)
    # 큰 DB(telematics)부터 시작해서 전체 시간이 덜 늘어지게
    order = sorted(by_db, key=lambda d: -sum(s.get('rows', 0) for _, _, s in by_db[d]))

    def one(db):
        s0 = time.time()
        beeline(db_sql(db, by_db[db]))
        print('  %-16s %3d objects  %5.0fs' % (db, len(by_db[db]), time.time() - s0), flush=True)
    print('generating %d objects in %d DBs (%d at a time) ...' % (len(items), len(by_db), a.parallel), flush=True)
    with ThreadPoolExecutor(a.parallel) as ex:
        list(ex.map(one, order))
    total = hdfs_mb()
    print('HDFS after generation: %.0f MB (%.0fs)' % (total, time.time() - t0), flush=True)

    # 목표 크기까지 telematics.trip_log_hist 를 늘림 (행당 크기를 재서 추가 행 수 계산)
    db, tbl = 'telematics', 'trip_log_hist'
    spec = next(s for d, t, s in items if d == db and t == tbl)
    if total < a.target_mb * 0.97:
        du = subprocess.run(['docker', 'exec', NN, 'hdfs', 'dfs', '-du', '-s', '/data/%s/%s' % (db, tbl)],
                            stdout=subprocess.PIPE, universal_newlines=True).stdout.split()
        per_row = int(du[0]) / spec['rows']
        extra = int((a.target_mb - total) * 1048576 / per_row)
        print('top up %s.%s: +%d rows (%.1f bytes/row)' % (db, tbl, extra, per_row), flush=True)
        beeline(';\n'.join(SESSION + [insert(db, tbl, spec, rows=extra, offset=spec['rows'])]) + ';\n')
        total = hdfs_mb()
    beeline('DROP DATABASE IF EXISTS %s CASCADE;\n' % GEN_DB)
    print('done: HDFS %.0f MB, %.0fs total' % (total, time.time() - t0), flush=True)


if __name__ == '__main__':
    main()
