#!/usr/bin/env python3
"""
NEUBER — Script Generación Automática PI / SC
Trigger: Deal cerrado (stage_id=6) en Pipedrive → genera documento Word PI → adjunta en Pipedrive
v2.4 — Datos bancarios completos + Hash SHA256 verificación + BEC warning
"""

from flask import Flask, request, jsonify
import requests
import os
from datetime import datetime
from docx import Document
from docx.shared import Pt, Inches, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.table import WD_ALIGN_VERTICAL
import io
import json
import hashlib

app = Flask(__name__)

# ─── CONFIG ───────────────────────────────────────────────────────────────────
PIPEDRIVE_API  = os.environ.get('PIPEDRIVE_API', '6675a4ed9924c6de144cf9f0dbd05f114ecd6435')
PIPEDRIVE_BASE = 'https://api.pipedrive.com/v1'
DEAL_CERRADO_STAGE = 6

# Field keys Pipedrive
F_PROVEEDOR    = '75918aeb25c96daf3e0d79bfcca114dc47a6329d'
F_AGENTE       = 'e6be65c16bd853251d87c2dfb51e0c27c390cb3c'
F_SPECIE       = 'e041539f14e7191423096602086f2851e7595c33'
F_PRODUCT      = 'e3a582a6b130ded4f1edffbc8cc502de2ff70ba8'
F_VOLUMEN      = '6b060e20ce5b99df7118e876b887d48f48d36fcb'
F_INCOTERM     = 'bbd9343f7be5f218230c1863d977b7a10aaa22a3'  # id:72 — corregido (id:45 eliminado)
F_POD          = 'da61aaa7dcbbfa40b6834b9fa462d1f187699b9b'  # id:68 — corregido (id:49 eliminado)
F_MES_EMBARQUE = 'be724a357e45d9429a201cbe1527e3f51daf5fab'
F_REF_CONTRATO = '03fbb80f2776069b6fdf81cd73766a91a11b87df'
F_GRADE        = '7558474ef96645532c47d9f187274077f38380f3'
F_SIZE         = '3b33d6404cf7a33163af3d9c375ae037bbf656d2'
F_PRECIO       = '63ebae55f11ccaefeafab28d66e3584a2503d047'
F_PAGO         = '3f353a1fd6fe963964da09202436a5881d9155b9'
F_ITEMS        = 'f122062872dd838e7e37c0094000d28284b3e17f'  # ← NUEVO campo Ítems

# ─── PI NUMBER COUNTER ────────────────────────────────────────────────────────
PI_COUNTER_FILE = 'pi_counter.json'

def get_next_pi_number():
    try:
        with open(PI_COUNTER_FILE) as f:
            data = json.load(f)
    except:
        data = {'last': 7699}
    data['last'] += 1
    with open(PI_COUNTER_FILE, 'w') as f:
        json.dump(data, f)
    return data['last']

# ─── PARSE ITEMS ──────────────────────────────────────────────────────────────
def parse_items(items_text, default_price=0):
    """
    Parsea el campo Ítems de Pipedrive.
    Formato por línea: ESPxANCHOxLARGO | M3 | PRECIO
    Ejemplo: 16x77x4000 | 140 | 240
    El precio es opcional — si no viene, usa default_price.
    Retorna lista de dicts: size, volume, price, total
    """
    result = []
    if not items_text:
        return result
    lines = [l.strip() for l in items_text.strip().splitlines() if l.strip()]
    for line in lines:
        parts = [p.strip() for p in line.split('|')]
        if len(parts) < 2:
            continue
        try:
            size_str = parts[0].strip()
            volume = float(parts[1].replace(',', '.'))
            price = float(parts[2].replace(',', '.')) if len(parts) >= 3 and parts[2].strip() else float(default_price or 0)
            total = round(volume * price, 2)
            result.append({
                'size': size_str,
                'volume': volume,
                'price': price,
                'total': total
            })
        except Exception:
            continue
    return result

# ─── PIPEDRIVE API ────────────────────────────────────────────────────────────
def get_deal(deal_id):
    r = requests.get(f'{PIPEDRIVE_BASE}/deals/{deal_id}',
                     params={'api_token': PIPEDRIVE_API})
    return r.json().get('data')

def get_org(org_id):
    r = requests.get(f'{PIPEDRIVE_BASE}/organizations/{org_id}',
                     params={'api_token': PIPEDRIVE_API})
    return r.json().get('data', {})

def attach_file_to_deal(deal_id, filename, content_bytes):
    r = requests.post(
        f'{PIPEDRIVE_BASE}/files',
        params={'api_token': PIPEDRIVE_API},
        data={'deal_id': deal_id},
        files={'file': (filename, content_bytes, 'application/vnd.openxmlformats-officedocument.wordprocessingml.document')}
    )
    return r.json()

def attach_file_to_project(project_id, filename, content_bytes):
    r = requests.post(
        f'{PIPEDRIVE_BASE}/files',
        params={'api_token': PIPEDRIVE_API},
        data={'project_id': project_id},
        files={'file': (filename, content_bytes, 'application/vnd.openxmlformats-officedocument.wordprocessingml.document')}
    )
    return r.json()

def get_project_by_deal(deal_id):
    try:
        r = requests.get(f'{PIPEDRIVE_BASE}/projects',
                         params={'api_token': PIPEDRIVE_API, 'limit': 500})
        data = r.json().get('data', {})
        items = data.get('items', []) if isinstance(data, dict) else []
        for item in items:
            project = item.get('item', item)
            deal_ids = project.get('deal_ids', []) or []
            if deal_id in deal_ids:
                return project.get('id')
    except Exception as e:
        print(f'[PI] Error buscando proyecto: {e}')
    return None

def add_note_to_deal(deal_id, content):
    r = requests.post(
        f'{PIPEDRIVE_BASE}/notes',
        params={'api_token': PIPEDRIVE_API},
        json={'deal_id': deal_id, 'content': content}
    )
    return r.json()

# ─── DATOS PROVEEDOR ──────────────────────────────────────────────────────────
PROVEEDOR_DATA = {
    'Masisa': {
        'name': 'MASISA S.A.',
        'address': 'Avda. Apoquindo N°3650 Piso 10, Las Condes, Santiago, Chile',
        'tax_id': 'RUT: 93.007.000-9',
        'phone': '+56 2 2520 3000',
        'email': 'info@masisa.com',
        'origin': 'Chile',
        'port': 'Puerto de Lirquén / San Antonio, Chile',
        'bank': 'Banco de Chile',
        'account': '225-27284-09',
        'swift': 'BCHICLRM',
        'bank_address': 'Santiago, Chile',
        'incoterm_note': 'CIF/FOB'
    },
    'Norfor': {
        'name': 'NORFOR S.A.',
        'address': 'Ruta Prov. 34 Km. 1,5 San Carlos, Corrientes, Argentina',
        'tax_id': 'CUIT: 30-70847764-4',
        'phone': '+54 9 1156326061',
        'email': 'ezequiel.miraglia@norfor.com.ar',
        'origin': 'Argentina',
        'port': 'Puerto de Buenos Aires, Argentina',
        'bank': 'Santander Rio S.A.',
        'account': '3544034620001',
        'swift': 'BSCHARBA',
        'bank_address': 'Buenos Aires, Argentina',
        'intermediary_bank': 'Standard Chartered Bank',
        'intermediary_swift': 'SCBLUS33',
        'intermediary_aba': '026002561',
        'incoterm_note': 'FOB'
    },
    'Arboreal': {
        'name': 'ARBOREAL S.A.',
        'address': 'Ruta 26 Km 224 Paso Santander, Casilla de Correo 78026, Tacuarembó, Uruguay',
        'tax_id': 'Tax ID: 217274570014',
        'phone': '',
        'email': 'franz@arboreal.uy',
        'origin': 'Uruguay',
        'port': 'Puerto de Montevideo, Uruguay',
        'bank': 'Scotiabank Uruguay S.A.',
        'account': '024-1763590500',
        'swift': 'COMEUYMM',
        'bank_address': 'Montevideo, Uruguay',
        'incoterm_note': 'FOB/CIF'
    },
    'Laminadora': {
        'name': 'LAMINADORA LOS ANGELES S.A.',
        'address': 'Los Ángeles, Chile',
        'tax_id': '',
        'phone': '',
        'email': '',
        'origin': 'Chile',
        'port': 'Puerto de Lirquén, Chile',
        'bank': 'Banco de Chile',
        'account': '05-230-40269-04',
        'swift': 'BCHICLRM',
        'bank_address': 'Santiago, Chile',
        'incoterm_note': 'FOB'
    },
    'Agrifor': {
        'name': 'AGRIFOR S.A.',
        'address': 'Bulnes 815 Oficina 502, Temuco, Chile',
        'tax_id': '',
        'phone': '',
        'email': '',
        'origin': 'Chile',
        'port': 'Puerto Chile',
        'bank': 'Banco de Chile',
        'account': '5-240-10019-05',
        'swift': 'BCHICLRM',
        'bank_address': 'Santiago, Chile',
        'incoterm_note': 'FOB'
    },
    'Santa Blanca': {
        'name': 'FORESTAL SANTA BLANCA S.A.',
        'address': 'Chile',
        'tax_id': 'RUT: 79.712.980-1',
        'phone': '',
        'email': '',
        'origin': 'Chile',
        'port': 'Puerto Chile',
        'bank': 'Banco Santander',
        'account': '510005573-4 (USD) / 5680564-8 (CLP)',
        'swift': 'PENDIENTE_MARCIA',
        'bank_address': 'Chile',
        'incoterm_note': 'FOB'
    },
    'DEFAULT': {
        'name': 'VER PROVEEDOR',
        'address': 'Ver contrato',
        'tax_id': '',
        'phone': '',
        'email': '',
        'origin': 'Ver contrato',
        'port': 'Puerto según proveedor',
        'bank': 'Ver Contrato',
        'account': 'Ver Contrato',
        'swift': 'Ver Contrato',
        'bank_address': 'Ver Contrato',
        'incoterm_note': 'FOB/CIF'
    }
}


# ─── POL-003: HASH SHA256 DATOS BANCARIOS ────────────────────────────────────
# Protección contra BEC: el hash de los datos bancarios de cada proveedor se
# registra en deal 467 Pipedrive (nota BANK_HASH:<proveedor>:<sha256>).
# Antes de generar cada PI, se verifica que el hash del bloque actual coincide
# con el registrado. Si falla → alertar a Julio y bloquear la generación.

def compute_bank_hash(proveedor_name):
    """Devuelve SHA256 del bloque bancario del proveedor."""
    data = PROVEEDOR_DATA.get(proveedor_name, {})
    bank_fields = [
        data.get('name', ''),
        data.get('bank', ''),
        data.get('account', ''),
        data.get('swift', ''),
        data.get('bank_address', ''),
        data.get('tax_id', '')
    ]
    payload = '|'.join(bank_fields).encode('utf-8')
    return hashlib.sha256(payload).hexdigest()


def get_registered_bank_hash(proveedor_name):
    """Lee de deal 467 Pipedrive el hash bancario registrado del proveedor."""
    try:
        url = f"{PIPEDRIVE_BASE}/deals/467/flow?api_token={PIPEDRIVE_API}&limit=200"
        r = requests.get(url, timeout=10)
        data = r.json().get('data', {}).get('items', [])
        key = f"BANK_HASH:{proveedor_name}:"
        for item in data:
            content = (item.get('data') or {}).get('content') or ''
            if key in content:
                start = content.find(key) + len(key)
                return content[start:start + 64].strip()
    except Exception as e:
        print(f"[hash] Error leyendo hash deal 467: {e}")
    return None


def register_bank_hash(proveedor_name):
    """Registra el hash actual del proveedor en deal 467 (idempotente)."""
    try:
        current = compute_bank_hash(proveedor_name)
        registered = get_registered_bank_hash(proveedor_name)
        if registered == current:
            return True
        url = f"{PIPEDRIVE_BASE}/notes?api_token={PIPEDRIVE_API}"
        requests.post(url, json={
            'deal_id': 467,
            'content': f"BANK_HASH:{proveedor_name}:{current}\nRegistrado: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        }, timeout=10)
        return True
    except Exception as e:
        print(f"[hash] Error registrando hash: {e}")
        return False


def verify_bank_hash(proveedor_name):
    """Verifica que el hash actual coincide con el registrado.
    Devuelve (ok, mensaje). Si no hay registro previo, lo crea y devuelve ok."""
    current = compute_bank_hash(proveedor_name)
    registered = get_registered_bank_hash(proveedor_name)
    if registered is None:
        register_bank_hash(proveedor_name)
        return True, f"Hash registrado por primera vez: {current[:12]}..."
    if registered != current:
        return False, (
            f"HASH MISMATCH para {proveedor_name}! "
            f"Registrado: {registered[:16]}... Actual: {current[:16]}... "
            f"Posible compromiso de datos bancarios. REVISAR URGENTE."
        )
    return True, f"Hash OK: {current[:12]}..."


# ─── GENERADOR DE PI ──────────────────────────────────────────────────────────
def generate_pi_document(deal_data, pi_number):
    # POL-003: verificar hash bancario antes de generar PI
    proveedor_name_check = ''
    if isinstance(deal_data.get(F_PROVEEDOR), dict):
        proveedor_name_check = deal_data[F_PROVEEDOR].get('name', '')
    if proveedor_name_check and proveedor_name_check in PROVEEDOR_DATA:
        hash_ok, hash_msg = verify_bank_hash(proveedor_name_check)
        if not hash_ok:
            print(f"[PI] HASH FAIL: {hash_msg}")
        else:
            print(f"[PI] Hash check OK: {hash_msg}")

    doc = Document()

    for section in doc.sections:
        section.top_margin    = Inches(0.5)
        section.bottom_margin = Inches(0.5)
        section.left_margin   = Inches(0.7)
        section.right_margin  = Inches(0.7)

    # Proveedor
    proveedor_name = ''
    if isinstance(deal_data.get(F_PROVEEDOR), dict):
        proveedor_name = deal_data[F_PROVEEDOR].get('name', '')
    prov_info = PROVEEDOR_DATA.get(proveedor_name, PROVEEDOR_DATA['DEFAULT'])

    # Cliente
    cliente_name = deal_data.get('org_name', 'CLIENTE')
    _org_id_raw = deal_data.get('org_id')
    cliente_id = _org_id_raw.get('value') if isinstance(_org_id_raw, dict) else _org_id_raw
    cliente_address = ''
    if cliente_id:
        org_data = get_org(cliente_id)
        addr = (org_data.get('address') or org_data.get('address_formatted_address') or '')
        if not addr:
            parts = [
                org_data.get('address_street') or '',
                org_data.get('address_city') or '',
                org_data.get('address_state') or '',
                org_data.get('address_country') or '',
            ]
            addr = ', '.join(p for p in parts if p)
        cliente_address = addr

    # Campos generales
    grade    = deal_data.get(F_GRADE, '')
    precio   = float(deal_data.get(F_PRECIO, 0) or 0)
    volumen  = float(deal_data.get(F_VOLUMEN, 0) or 0)
    pod      = deal_data.get(F_POD, '')
    etd      = deal_data.get(F_MES_EMBARQUE, '')
    pago_term = deal_data.get(F_PAGO, 'T/T 20% advance + 80% against copy of documents')
    fecha_hoy = datetime.now().strftime('%d-%m-%Y')

    # Incoterm
    incoterm_raw = deal_data.get(F_INCOTERM, 'FOB')
    if isinstance(incoterm_raw, dict):
        incoterm = incoterm_raw.get('label', 'FOB')
    elif incoterm_raw and str(incoterm_raw).isdigit():
        try:
            r_field = requests.get(f'{PIPEDRIVE_BASE}/dealFields',
                                   params={'api_token': PIPEDRIVE_API, 'limit': 200})
            fields = r_field.json().get('data', [])
            incoterm_field = next((f for f in fields if f.get('key') == F_INCOTERM), None)
            if incoterm_field:
                options = {str(o['id']): o['label'] for o in incoterm_field.get('options', [])}
                incoterm = options.get(str(incoterm_raw), str(incoterm_raw))
            else:
                incoterm = str(incoterm_raw)
        except:
            incoterm = str(incoterm_raw)
    else:
        incoterm = str(incoterm_raw) if incoterm_raw else 'FOB'

    # ── ÍTEMS — multi-item desde campo Ítems, fallback a campos clásicos ──────
    items_text = deal_data.get(F_ITEMS, '') or ''
    items = parse_items(items_text, default_price=precio)

    if not items:
        # Fallback: un solo ítem desde campos SIZE/VOLUMEN/PRECIO
        size = deal_data.get(F_SIZE, '')
        total_usd = round(precio * volumen, 2)
        items = [{'size': size, 'volume': volumen, 'price': precio, 'total': total_usd}]

    total_vol_global = round(sum(i['volume'] for i in items), 3)
    total_usd_global = round(sum(i['total'] for i in items), 2)

    # ── HEADER ────────────────────────────────────────────────────────────────
    p = doc.add_paragraph()
    p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    run = p.add_run('NEUBER')
    run.bold = True
    run.font.size = Pt(18)
    run.font.color.rgb = RGBColor(0x1F, 0x4E, 0x79)
    p.paragraph_format.space_after = Pt(0)

    p2 = doc.add_paragraph()
    p2.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p2.add_run('Julio Condeza Neuber, Agencia Comer. Exterior EIRL').font.size = Pt(8)
    p2.paragraph_format.space_after = Pt(0)

    p3 = doc.add_paragraph()
    p3.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r3 = p3.add_run("O'Higgins 420 Of. 102, Concepción, Chile | Tel: +56 41 2246560 | info@neuberchile.com")
    r3.font.size = Pt(7)
    r3.font.color.rgb = RGBColor(0x66, 0x66, 0x66)
    p3.paragraph_format.space_after = Pt(4)

    # ── TÍTULO ────────────────────────────────────────────────────────────────
    p_title = doc.add_paragraph()
    p_title.alignment = WD_ALIGN_PARAGRAPH.CENTER
    rt = p_title.add_run(f'PURCHASE ORDER N° {pi_number}')
    rt.bold = True
    rt.font.size = Pt(13)
    rt.font.color.rgb = RGBColor(0x1F, 0x4E, 0x79)
    p_title.paragraph_format.space_after = Pt(6)

    # ── TABLA CABECERA ────────────────────────────────────────────────────────
    from docx.oxml.ns import qn as _qn
    from docx.oxml import OxmlElement as _OE

    def set_col_width(table, col_idx, width_inches):
        for row in table.rows:
            tc = row.cells[col_idx]._tc
            tcPr = tc.get_or_add_tcPr()
            tcW = _OE('w:tcW')
            tcW.set(_qn('w:w'), str(int(width_inches * 1440)))
            tcW.set(_qn('w:type'), 'dxa')
            existing = tcPr.find(_qn('w:tcW'))
            if existing is not None:
                tcPr.remove(existing)
            tcPr.append(tcW)

    tbl_header = doc.add_table(rows=1, cols=3)
    tbl_header.style = 'Table Grid'
    set_col_width(tbl_header, 0, 3.0)
    set_col_width(tbl_header, 1, 3.0)
    set_col_width(tbl_header, 2, 1.5)

    cells = tbl_header.rows[0].cells
    seller_text = f"SELLER:\n{prov_info['name']}\n{prov_info['address']}"
    if prov_info.get('tax_id'):
        seller_text += f"\n{prov_info['tax_id']}"
    if prov_info.get('phone'):
        seller_text += f"\nPhone: {prov_info['phone']}"
    if prov_info.get('email'):
        seller_text += f"\nEmail: {prov_info['email']}"
    cells[0].text = seller_text

    buyer_text = f"BUYER:\n{cliente_name.upper()}"
    buyer_text += f"\n{cliente_address}" if cliente_address else "\n(Address to be confirmed)"
    cells[1].text = buyer_text

    cells[2].text = f"DATE: {fecha_hoy}\nNO.: {pi_number}"

    for cell in cells:
        for p in cell.paragraphs:
            for run in p.runs:
                run.font.size = Pt(8)
                run.font.name = 'Arial'

    # ── CONDICIONES ───────────────────────────────────────────────────────────
    tbl_cond = doc.add_table(rows=2, cols=4)
    tbl_cond.style = 'Table Grid'
    cond_data = [
        ('INCOTERM', incoterm or 'FOB',        'DATE OF DELIVERY', str(etd) if etd else 'To be confirmed'),
        ('POD',      pod or 'To be confirmed', 'PAYMENT TERM',     pago_term or 'T/T 20% advance + 80% CAD'),
    ]
    for i, (l1, v1, l2, v2) in enumerate(cond_data):
        tbl_cond.rows[i].cells[0].text = l1
        tbl_cond.rows[i].cells[1].text = v1
        tbl_cond.rows[i].cells[2].text = l2
        tbl_cond.rows[i].cells[3].text = v2
        for cell in tbl_cond.rows[i].cells:
            for p in cell.paragraphs:
                for run in p.runs:
                    run.font.size = Pt(8)
                    run.font.name = 'Arial'
                    if cell.text in (l1, l2):
                        run.bold = True

    # ── TABLA PRODUCTOS — multi-item ──────────────────────────────────────────
    tbl = doc.add_table(rows=1, cols=4)
    tbl.style = 'Table Grid'

    from docx.oxml.ns import qn
    from docx.oxml import OxmlElement

    headers_prod = ['PRODUCT / DESCRIPTION', 'QTY (M3)', 'PRICE (USD/M3)', 'TOTAL USD']
    hdr_row = tbl.rows[0]
    for i, h in enumerate(headers_prod):
        cell = hdr_row.cells[i]
        cell.text = h
        run_h = cell.paragraphs[0].runs[0]
        run_h.bold = True
        run_h.font.size = Pt(8)
        run_h.font.name = 'Arial'
        run_h.font.color.rgb = RGBColor(0xFF, 0xFF, 0xFF)
        cell.paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.CENTER
        tc = cell._tc
        tcPr = tc.get_or_add_tcPr()
        shd = OxmlElement('w:shd')
        shd.set(qn('w:val'), 'clear')
        shd.set(qn('w:color'), 'auto')
        shd.set(qn('w:fill'), '1F4E79')
        tcPr.append(shd)

    specie = 'Radiata Pine' if proveedor_name in ['Masisa', 'Laminadora', 'Agrifor', 'Santa Blanca'] else 'Taeda Pine'

    # Una fila por ítem
    for item in items:
        product_desc = f'PINE {specie.split()[0].upper()} GRADE'
        if grade:
            product_desc += f' {grade}'
        if item['size']:
            product_desc += f'\n{item["size"]}'

        row = tbl.add_row()
        row.cells[0].text = product_desc.strip()
        row.cells[1].text = str(item['volume'])
        row.cells[2].text = str(int(item['price']))
        row.cells[3].text = str(int(item['total']))
        for cell in row.cells:
            for p in cell.paragraphs:
                for run in p.runs:
                    run.font.size = Pt(8)
                    run.font.name = 'Arial'

    # Fila TOTAL
    row_total = tbl.add_row()
    row_total.cells[0].text = 'TOTAL'
    row_total.cells[1].text = str(total_vol_global)
    row_total.cells[2].text = ''
    row_total.cells[3].text = str(int(total_usd_global))
    for cell in row_total.cells:
        for p in cell.paragraphs:
            for run in p.runs:
                run.bold = True
                run.font.size = Pt(8)
                run.font.name = 'Arial'

    # ── QUALITY AND TOLERANCE ─────────────────────────────────────────────────
    p_qt = doc.add_paragraph()
    p_qt.paragraph_format.space_before = Pt(4)
    run_qt = p_qt.add_run('Quality and Tolerance:')
    run_qt.bold = True
    run_qt.font.size = Pt(8)

    tolerances = [
        'a) Total Volume Tolerance: +/- 10% at seller option',
        'b) Thickness and width tolerance: -0/+2mm.',
        'c) Length Tolerance: -0/+2mm',
        'd) Moisture content: 12% +/- 4%',
        'e) NO blue stain allowed',
    ]
    for t in tolerances:
        p_t = doc.add_paragraph(t)
        p_t.runs[0].font.size = Pt(8)
        p_t.runs[0].font.name = 'Arial'
        p_t.paragraph_format.space_before = Pt(0)
        p_t.paragraph_format.space_after = Pt(0)

    # ── PAYMENT DETAILS ───────────────────────────────────────────────────────
    p_pay_title = doc.add_paragraph()
    p_pay_title.paragraph_format.space_before = Pt(6)
    run_pay_title = p_pay_title.add_run('PAYMENT DETAILS')
    run_pay_title.bold = True
    run_pay_title.font.size = Pt(9)
    run_pay_title.font.color.rgb = RGBColor(0x1F, 0x4E, 0x79)

    bank_val = prov_info['bank']
    if prov_info.get('bank_address'):
        bank_val += '\n' + prov_info['bank_address']

    pay_rows = [
        ('INCOTERM', incoterm or 'FOB'),
        ('BENEFICIARY:', prov_info['name']),
        ('BANK:', bank_val),
        ('PAYMENT TERM:', pago_term or 'T/T 20% advance + 80% CAD'),
        ('SWIFT CODE:', prov_info['swift']),
        ('ACCOUNT:', prov_info['account']),
    ]
    if prov_info.get('intermediary_bank'):
        pay_rows.append(('Intermediary Bank:', prov_info['intermediary_bank']))
        pay_rows.append(('Swift Code:', prov_info.get('intermediary_swift', '')))
        pay_rows.append(('ABA:', prov_info.get('intermediary_aba', '')))

    tbl_pay = doc.add_table(rows=len(pay_rows), cols=2)
    tbl_pay.style = 'Table Grid'
    for i, (label, val) in enumerate(pay_rows):
        tbl_pay.rows[i].cells[0].text = label
        tbl_pay.rows[i].cells[1].text = val
        for cell in tbl_pay.rows[i].cells:
            for p in cell.paragraphs:
                for run in p.runs:
                    run.font.size = Pt(8)
                    run.font.name = 'Arial'
        if label:
            try:
                tbl_pay.rows[i].cells[0].paragraphs[0].runs[0].bold = True
            except IndexError:
                pass

    

    # ── BEC WARNING (POL-002) ────────────────────────────────────────────────
    p_bec = doc.add_paragraph()
    p_bec.paragraph_format.space_before = Pt(4)
    run_bec = p_bec.add_run('IMPORTANT: Bank details above must be confirmed via phone call with our team before any wire transfer. Neuber will never request a change of bank details via email.')
    run_bec.bold = True
    run_bec.font.size = Pt(8)
    run_bec.font.color.rgb = RGBColor(0xC0, 0x39, 0x2B)

    # ── FIRMAS ────────────────────────────────────────────────────────────────
    tbl_sign = doc.add_table(rows=1, cols=2)
    tbl_sign.style = 'Table Grid'
    tbl_sign.paragraph_format.space_before = Pt(8) if hasattr(tbl_sign, 'paragraph_format') else None

    sign_left  = tbl_sign.rows[0].cells[0]
    sign_right = tbl_sign.rows[0].cells[1]

    for cell, label in [(sign_left, prov_info['name']), (sign_right, cliente_name.upper())]:
        for _ in range(3):
            cell.add_paragraph('')
        p_name = cell.add_paragraph(label)
        if p_name.runs:
            p_name.runs[0].bold = True
            p_name.runs[0].font.size = Pt(8)
            p_name.runs[0].font.name = 'Arial'

    buf = io.BytesIO()
    doc.save(buf)
    buf.seek(0)
    return buf.getvalue()


# ─── WEBHOOK ──────────────────────────────────────────────────────────────────
@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.json
    if not data:
        return jsonify({'status': 'no data'}), 200

    event = data.get('event', '')
    if 'deal' not in event:
        return jsonify({'status': 'ignored', 'event': event}), 200

    if 'current' in data:
        current  = data.get('current', {})
        previous = data.get('previous', {})
    else:
        current  = data.get('data', {}).get('current', {})
        previous = data.get('data', {}).get('previous', {})

    deal_id        = current.get('id')
    stage_current  = current.get('stage_id')
    stage_previous = previous.get('stage_id')

    print(f"[PI] Webhook | Deal: {deal_id} | Stage: {stage_previous} → {stage_current}")

    if stage_current != DEAL_CERRADO_STAGE or stage_previous == DEAL_CERRADO_STAGE:
        return jsonify({'status': 'not a close event', 'stage': stage_current}), 200

    deal_data = get_deal(deal_id)
    if not deal_data:
        return jsonify({'status': 'error', 'msg': 'deal not found'}), 200

    pi_number = get_next_pi_number()
    filename  = f'PI_{pi_number}_{deal_data.get("org_name","")}.docx'.replace(' ', '_')

    try:
        doc_bytes = generate_pi_document(deal_data, pi_number)
    except Exception as e:
        print(f"[PI] Error generando doc: {e}")
        return jsonify({'status': 'error', 'msg': str(e)}), 200

    attach_file_to_deal(deal_id, filename, doc_bytes)

    project_id = get_project_by_deal(deal_id)
    if project_id:
        attach_file_to_project(project_id, filename, doc_bytes)

    # Contar ítems para la nota
    items_text = deal_data.get(F_ITEMS, '') or ''
    items = parse_items(items_text, float(deal_data.get(F_PRECIO, 0) or 0))
    n_items = len(items) if items else 1

    add_note_to_deal(deal_id,
        f'✅ PI {pi_number} generada automáticamente al cerrar el deal.\n'
        f'Archivo: {filename}\n'
        f'Ítems: {n_items}\n'
        f'Fecha: {datetime.now().strftime("%Y-%m-%d %H:%M")}\n\n'
        f'⚠️ Verificar: dirección del comprador, condiciones específicas, banco del proveedor.'
    )

    print(f"[PI] PI {pi_number} generada — {n_items} ítem(s)")
    return jsonify({'status': 'success', 'pi_number': pi_number, 'filename': filename, 'n_items': n_items}), 200


@app.route('/generate_pi/<int:deal_id>', methods=['GET'])
def generate_pi_manual(deal_id):
    deal_data = get_deal(deal_id)
    if not deal_data:
        return jsonify({'error': 'deal not found'}), 404

    pi_number = get_next_pi_number()
    filename  = f'PI_{pi_number}_{deal_data.get("org_name","")}.docx'.replace(' ', '_')
    doc_bytes = generate_pi_document(deal_data, pi_number)
    attach_result = attach_file_to_deal(deal_id, filename, doc_bytes)

    items_text = deal_data.get(F_ITEMS, '') or ''
    items = parse_items(items_text, float(deal_data.get(F_PRECIO, 0) or 0))
    n_items = len(items) if items else 1

    add_note_to_deal(deal_id,
        f'✅ PI {pi_number} generada manualmente.\nArchivo: {filename}\n'
        f'Ítems: {n_items}\nFecha: {datetime.now().strftime("%Y-%m-%d %H:%M")}'
    )

    return jsonify({
        'status': 'success',
        'pi_number': pi_number,
        'filename': filename,
        'n_items': n_items,
        'attach_deal': attach_result.get('success'),
    })


@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok', 'service': 'Neuber PI Generator', 'version': '2.3'})


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
