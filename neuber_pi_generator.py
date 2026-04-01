#!/usr/bin/env python3
"""
NEUBER — Script Generación Automática PI / SC
Trigger: Deal cerrado (stage_id=6) en Pipedrive → genera documento Word PI → adjunta en Pipedrive

Uso:
  1. Deploy en servidor (Railway, Render, etc.)
  2. Configurar webhook Pipedrive: deal.updated → este endpoint /webhook
  3. Cuando stage_id = 6 → genera PI automáticamente

Dependencias: pip install flask requests python-docx
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
F_INCOTERM     = '3ffd5d7de10babee6fb7ffbba9ece4ed6b94d67d'
F_POD          = '7fca81dc2aba466b084de4402a2ea44a777afe27'
F_MES_EMBARQUE = 'be724a357e45d9429a201cbe1527e3f51daf5fab'
F_REF_CONTRATO = '03fbb80f2776069b6fdf81cd73766a91a11b87df'
F_GRADE        = '7558474ef96645532c47d9f187274077f38380f3'
F_SIZE         = '3b33d6404cf7a33163af3d9c375ae037bbf656d2'
F_PRECIO       = '63ebae55f11ccaefeafab28d66e3584a2503d047'
F_PAGO         = '3f353a1fd6fe963964da09202436a5881d9155b9'

# ─── PI NUMBER COUNTER ────────────────────────────────────────────────────────
PI_COUNTER_FILE = 'pi_counter.json'

def get_next_pi_number():
    """Lee y avanza el contador de PI. Actualmente ~7685-7700."""
    try:
        with open(PI_COUNTER_FILE) as f:
            data = json.load(f)
    except:
        data = {'last': 7699}
    data['last'] += 1
    with open(PI_COUNTER_FILE, 'w') as f:
        json.dump(data, f)
    return data['last']

# ─── PIPEDRIVE API ────────────────────────────────────────────────────────────
def get_deal(deal_id):
    r = requests.get(f'{PIPEDRIVE_BASE}/deals/{deal_id}',
                     params={'api_token': PIPEDRIVE_API})
    return r.json().get('data')

def get_org(org_id):
    r = requests.get(f'{PIPEDRIVE_BASE}/organizations/{org_id}',
                     params={'api_token': PIPEDRIVE_API})
    return r.json().get('data', {})

def get_person(person_id):
    r = requests.get(f'{PIPEDRIVE_BASE}/persons/{person_id}',
                     params={'api_token': PIPEDRIVE_API})
    return r.json().get('data', {})

def attach_file_to_deal(deal_id, filename, content_bytes):
    """Adjunta archivo Word/PDF a un deal en Pipedrive."""
    r = requests.post(
        f'{PIPEDRIVE_BASE}/files',
        params={'api_token': PIPEDRIVE_API},
        data={'deal_id': deal_id},
        files={'file': (filename, content_bytes, 'application/vnd.openxmlformats-officedocument.wordprocessingml.document')}
    )
    return r.json()

def attach_file_to_project(project_id, filename, content_bytes):
    """Adjunta archivo Word a un proyecto en Pipedrive."""
    r = requests.post(
        f'{PIPEDRIVE_BASE}/files',
        params={'api_token': PIPEDRIVE_API},
        data={'project_id': project_id},
        files={'file': (filename, content_bytes, 'application/vnd.openxmlformats-officedocument.wordprocessingml.document')}
    )
    return r.json()

def get_project_by_deal(deal_id):
    """Busca el proyecto de Pipedrive asociado a un deal."""
    try:
        r = requests.get(f'{PIPEDRIVE_BASE}/projects',
                         params={'api_token': PIPEDRIVE_API, 'limit': 500})
        data = r.json().get('data', {})
        items = data.get('items', []) if isinstance(data, dict) else []
        for item in items:
            project = item.get('item', item)
            # Buscar por deal_ids asociados o por título similar
            deal_ids = project.get('deal_ids', []) or []
            if deal_id in deal_ids:
                return project.get('id')
    except Exception as e:
        print(f'[PI] Error buscando proyecto: {e}')
    return None

def add_note_to_deal(deal_id, content):
    """Agrega nota al deal."""
    r = requests.post(
        f'{PIPEDRIVE_BASE}/notes',
        params={'api_token': PIPEDRIVE_API},
        json={'deal_id': deal_id, 'content': content}
    )
    return r.json()

# ─── DATOS PROVEEDOR ──────────────────────────────────────────────────────────
# FIX #1 + #4 + #5: Seller = proveedor, Origin = solo país proveedor, Payment = cuenta proveedor
PROVEEDOR_DATA = {
    'Masisa': {
        'name': 'MASISA S.A.',
        'address': 'Av. El Golf 40 Piso 20, Las Condes, Santiago, Chile',
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
        'address': 'Montevideo, Uruguay',
        'tax_id': '',
        'phone': '',
        'email': '',
        'origin': 'Uruguay',
        'port': 'Puerto de Montevideo, Uruguay',
        'bank': 'Ver Lumber Sales Contract',
        'account': 'Ver Lumber Sales Contract',
        'swift': 'Ver Lumber Sales Contract',
        'bank_address': 'Montevideo, Uruguay',
        'incoterm_note': 'FOB/CIF'
    },
    'Laminadora': {
        'name': 'LAMINADORA LLASA',
        'address': 'Puerto de Lirquén, Chile',
        'tax_id': '',
        'phone': '',
        'email': '',
        'origin': 'Chile',
        'port': 'Puerto de Lirquén, Chile',
        'bank': 'Ver Sales Contract',
        'account': 'Ver Sales Contract',
        'swift': 'Ver Sales Contract',
        'bank_address': 'Chile',
        'incoterm_note': 'FOB'
    },
    'Agrifor': {
        'name': 'AGRIFOR S.A.',
        'address': 'Chile',
        'tax_id': '',
        'phone': '',
        'email': '',
        'origin': 'Chile',
        'port': 'Puerto Chile',
        'bank': 'Ver Sales Contract',
        'account': 'Ver Sales Contract',
        'swift': 'Ver Sales Contract',
        'bank_address': 'Chile',
        'incoterm_note': 'FOB'
    },
    'Santa Blanca': {
        'name': 'SANTA BLANCA S.A.',
        'address': 'Chile',
        'tax_id': '',
        'phone': '',
        'email': '',
        'origin': 'Chile',
        'port': 'Puerto Chile',
        'bank': 'Ver Sales Contract',
        'account': 'Ver Sales Contract',
        'swift': 'Ver Sales Contract',
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

# ─── GENERADOR DE PI ──────────────────────────────────────────────────────────
def generate_pi_document(deal_data, pi_number):
    """
    Genera documento Word de Proforma Invoice para Neuber.
    Retorna bytes del documento.
    """
    doc = Document()

    # Márgenes — optimizado para una hoja carta (FIX #7)
    for section in doc.sections:
        section.top_margin    = Inches(0.5)
        section.bottom_margin = Inches(0.5)
        section.left_margin   = Inches(0.7)
        section.right_margin  = Inches(0.7)

    # Extraer datos del deal
    proveedor_name = ''
    if isinstance(deal_data.get(F_PROVEEDOR), dict):
        proveedor_name = deal_data[F_PROVEEDOR].get('name', '')
    prov_info = PROVEEDOR_DATA.get(proveedor_name, PROVEEDOR_DATA['DEFAULT'])

    # Datos cliente
    cliente_name  = deal_data.get('org_name', 'CLIENTE')
    # org_id puede llegar como número o como dict {value: id, name: ...}
    _org_id_raw = deal_data.get('org_id')
    if isinstance(_org_id_raw, dict):
        cliente_id = _org_id_raw.get('value')
    else:
        cliente_id = _org_id_raw
    # Obtener dirección del cliente desde Pipedrive (FIX #2)
    cliente_address = ''
    if cliente_id:
        org_data = get_org(cliente_id)
        print(f'[PI] org_data keys: {list(org_data.keys()) if org_data else None}')
        # Pipedrive puede retornar la dirección en múltiples formatos
        addr = (org_data.get('address') or 
                org_data.get('address_formatted_address') or '')
        if not addr:
            parts = [
                org_data.get('address_street') or '',
                org_data.get('address_city') or '',
                org_data.get('address_state') or '',
                org_data.get('address_country') or '',
            ]
            addr = ', '.join(p for p in parts if p)
        print(f'[PI] cliente_address: {repr(addr)}')
        cliente_address = addr

    grade         = deal_data.get(F_GRADE, '')
    size          = deal_data.get(F_SIZE, '')
    precio        = deal_data.get(F_PRECIO, 0) or 0
    volumen       = deal_data.get(F_VOLUMEN, 0) or 0
    pod           = deal_data.get(F_POD, '')
    etd           = deal_data.get(F_MES_EMBARQUE, '')
    # Incoterm puede llegar como ID numérico (campo enum de Pipedrive)
    incoterm_raw = deal_data.get(F_INCOTERM, 'FOB')
    if isinstance(incoterm_raw, dict):
        incoterm = incoterm_raw.get('label', str(incoterm_raw.get('id', 'FOB')))
    elif incoterm_raw and str(incoterm_raw).isdigit():
        # Es un ID numérico — consultar opciones del campo
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
    pago_term     = deal_data.get(F_PAGO, 'T/T 20% advance + 80% against copy of documents')
    ref_contrato  = deal_data.get(F_REF_CONTRATO, '')  # FIX #3: se usa como Ref. Contract, NO se sobreescribe con PI#

    total_usd = round(float(precio) * float(volumen), 2) if precio and volumen else 0

    fecha_hoy = datetime.now().strftime('%d-%m-%Y')

    # ── HEADER — Logo/Nombre Neuber ───────────────────────────────────────────
    p = doc.add_paragraph()
    p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    run = p.add_run('NEUBER')
    run.bold = True
    run.font.size = Pt(18)
    run.font.color.rgb = RGBColor(0x1F, 0x4E, 0x79)
    p.paragraph_format.space_after = Pt(0)

    p2 = doc.add_paragraph()
    p2.alignment = WD_ALIGN_PARAGRAPH.CENTER
    run2 = p2.add_run('Julio Condeza Neuber, Agencia Comer. Exterior EIRL')
    run2.font.size = Pt(8)
    run2.font.color.rgb = RGBColor(0x44, 0x44, 0x44)
    p2.paragraph_format.space_after = Pt(0)

    p3 = doc.add_paragraph()
    p3.alignment = WD_ALIGN_PARAGRAPH.CENTER
    run3 = p3.add_run("O'Higgins 420 Of. 102, Concepción, Chile | Tel: +56 41 2246560 | info@neuberchile.com")
    run3.font.size = Pt(7)
    run3.font.color.rgb = RGBColor(0x66, 0x66, 0x66)
    p3.paragraph_format.space_after = Pt(4)

    # ── TÍTULO PI ─────────────────────────────────────────────────────────────
    p_title = doc.add_paragraph()
    p_title.alignment = WD_ALIGN_PARAGRAPH.CENTER
    rt = p_title.add_run(f'PURCHASE ORDER N° {pi_number}')
    rt.bold = True
    rt.font.size = Pt(13)
    rt.font.color.rgb = RGBColor(0x1F, 0x4E, 0x79)
    p_title.paragraph_format.space_after = Pt(6)

    # ── TABLA CABECERA: SELLER / BUYER / DATE ─────────────────────────────────
    # FIX #1: Seller = proveedor, NO Neuber
    tbl_header = doc.add_table(rows=1, cols=3)
    tbl_header.style = 'Table Grid'
    # Setear anchos via XML para garantizar que se apliquen
    from docx.oxml.ns import qn as _qn
    from docx.oxml import OxmlElement as _OE
    from docx.shared import Twips
    def set_col_width(table, col_idx, width_inches):
        col_cells = [row.cells[col_idx] for row in table.rows]
        for cell in col_cells:
            tc = cell._tc
            tcPr = tc.get_or_add_tcPr()
            tcW = _OE('w:tcW')
            tcW.set(_qn('w:w'), str(int(width_inches * 1440)))
            tcW.set(_qn('w:type'), 'dxa')
            existing = tcPr.find(_qn('w:tcW'))
            if existing is not None:
                tcPr.remove(existing)
            tcPr.append(tcW)
    set_col_width(tbl_header, 0, 3.0)
    set_col_width(tbl_header, 1, 3.0)
    set_col_width(tbl_header, 2, 1.5)

    cells = tbl_header.rows[0].cells

    # Seller = proveedor
    seller_text = f"SELLER:\n{prov_info['name']}\n{prov_info['address']}"
    if prov_info.get('tax_id'):
        seller_text += f"\n{prov_info['tax_id']}"
    if prov_info.get('phone'):
        seller_text += f"\nPhone: {prov_info['phone']}"
    if prov_info.get('email'):
        seller_text += f"\nEmail: {prov_info['email']}"
    cells[0].text = seller_text

    # Buyer = cliente con dirección real (FIX #2)
    buyer_text = f"BUYER:\n{cliente_name.upper()}"
    if cliente_address:
        buyer_text += f"\n{cliente_address}"
    else:
        buyer_text += "\n(Address to be confirmed)"
    cells[1].text = buyer_text

    # FIX #3: Ref. Contract = SC del contrato, NO se sobreescribe con PI#
    cells[2].text = f"DATE: {fecha_hoy}\nNO.: {pi_number}"

    for cell in cells:
        for p in cell.paragraphs:
            for run in p.runs:
                run.font.size = Pt(8)
                run.font.name = 'Arial'

    # ── SALE CONDITIONS ───────────────────────────────────────────────────────
    tbl_cond = doc.add_table(rows=4, cols=4)
    tbl_cond.style = 'Table Grid'

    # FIX #4: Origin = solo país del proveedor
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

    # ── TABLA PRODUCTOS ───────────────────────────────────────────────────────
    tbl = doc.add_table(rows=1, cols=4)
    tbl.style = 'Table Grid'

    headers_prod = ['PRODUCT / DESCRIPTION', 'QTY', 'PRICE', 'TOTAL USD']
    hdr_row = tbl.rows[0]
    for i, h in enumerate(headers_prod):
        cell = hdr_row.cells[i]
        cell.text = h
        cell.paragraphs[0].runs[0].bold = True
        cell.paragraphs[0].runs[0].font.size = Pt(8)
        cell.paragraphs[0].runs[0].font.name = 'Arial'
        cell.paragraphs[0].runs[0].font.color.rgb = RGBColor(0xFF, 0xFF, 0xFF)
        cell.paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.CENTER
        from docx.oxml.ns import qn
        from docx.oxml import OxmlElement
        tc = cell._tc
        tcPr = tc.get_or_add_tcPr()
        shd = OxmlElement('w:shd')
        shd.set(qn('w:val'), 'clear')
        shd.set(qn('w:color'), 'auto')
        shd.set(qn('w:fill'), '1F4E79')
        tcPr.append(shd)

    # Product row — descripción como en PI real: especie + grade + dimensions
    specie = 'Radiata Pine' if proveedor_name in ['Masisa', 'Laminadora', 'Agrifor', 'Santa Blanca'] else 'Taeda Pine'
    product_desc = f'PINE {specie.split()[0].upper()} GRADE\n'
    if grade:
        product_desc += f'{grade}'
    if size:
        product_desc += f' {size}'

    row = tbl.add_row()
    row.cells[0].text = product_desc.strip()
    row.cells[1].text = str(volumen) if volumen else ''
    row.cells[2].text = str(int(precio)) if precio else ''
    row.cells[3].text = str(int(total_usd)) if total_usd else ''
    for cell in row.cells:
        for p in cell.paragraphs:
            for run in p.runs:
                run.font.size = Pt(8)
                run.font.name = 'Arial'

    # Total row
    row_total = tbl.add_row()
    row_total.cells[0].text = ''
    row_total.cells[1].text = str(volumen)
    row_total.cells[1].paragraphs[0].runs[0].bold = True
    row_total.cells[2].text = ''
    row_total.cells[3].text = str(int(total_usd))
    row_total.cells[3].paragraphs[0].runs[0].bold = True

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

    # ── OTHER CONDITIONS ──────────────────────────────────────────────────────
    p_oth_title = doc.add_paragraph()
    p_oth_title.paragraph_format.space_before = Pt(4)
    run_oth = p_oth_title.add_run('OTHER CONDITIONS')
    run_oth.bold = True
    run_oth.font.size = Pt(8)

    # ── PAYMENT DETAILS ───────────────────────────────────────────────────────
    # FIX #5: Payment = cuenta del proveedor, NO de Neuber
    p_pay_title = doc.add_paragraph()
    p_pay_title.paragraph_format.space_before = Pt(6)
    run_pay_title = p_pay_title.add_run('PAYMENT DETAILS')
    run_pay_title.bold = True
    run_pay_title.font.size = Pt(9)
    run_pay_title.font.color.rgb = RGBColor(0x1F, 0x4E, 0x79)

    # Construir valor del banco con dirección debajo en la misma celda
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
    # Si hay banco intermediario (Norfor), agregar
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

    # ── FIRMAS — FIX #6: espacio para firma del proveedor Y del cliente ───────
    p_sign_title = doc.add_paragraph()
    p_sign_title.paragraph_format.space_before = Pt(8)
    run_sign = p_sign_title.add_run('OTHER CONDITIONS')
    run_sign.font.size = Pt(8)
    run_sign.font.color.rgb = RGBColor(0xFF, 0xFF, 0xFF)  # invisible — solo espaciado

    tbl_sign = doc.add_table(rows=1, cols=2)
    tbl_sign.style = 'Table Grid'
    tbl_sign.columns[0].width = Inches(3.5)
    tbl_sign.columns[1].width = Inches(3.5)

    from docx.oxml.ns import qn as qn2
    from docx.oxml import OxmlElement as OE

    sign_left  = tbl_sign.rows[0].cells[0]
    sign_right = tbl_sign.rows[0].cells[1]

    # Agregar espacio vertical antes de la firma
    for cell, label in [(sign_left, prov_info['name']), (sign_right, cliente_name.upper())]:
        for _ in range(3):
            cell.add_paragraph('')
        p_name = cell.add_paragraph(label)
        if p_name.runs:
            p_name.runs[0].bold = True
            p_name.runs[0].font.size = Pt(8)
            p_name.runs[0].font.name = 'Arial'

    # Guardar en memoria
    buf = io.BytesIO()
    doc.save(buf)
    buf.seek(0)
    return buf.getvalue()


# ─── WEBHOOK ENDPOINT ─────────────────────────────────────────────────────────
@app.route('/webhook', methods=['POST'])
def webhook():
    """
    Recibe webhook de Pipedrive cuando un deal se actualiza.
    Si stage_id = 6 (Deal Cerrado) → genera PI y adjunta.
    """
    data = request.json
    if not data:
        return jsonify({'status': 'no data'}), 200

    event = data.get('event', '')
    if 'deal' not in event:
        return jsonify({'status': 'ignored', 'event': event}), 200

    # Soporte Pipedrive v1 y v2
    # v1: {"event": "...", "data": {"current": {...}, "previous": {...}}}
    # v2: {"event": "...", "current": {...}, "previous": {...}}
    if 'current' in data:
        current  = data.get('current', {})
        previous = data.get('previous', {})
    else:
        current  = data.get('data', {}).get('current', {})
        previous = data.get('data', {}).get('previous', {})

    deal_id        = current.get('id')
    stage_current  = current.get('stage_id')
    stage_previous = previous.get('stage_id')

    print(f"[PI Generator] Webhook recibido. Event: {event} | Deal: {deal_id} | Stage: {stage_previous} → {stage_current}")

    # Solo actuar cuando se mueve A Deal Cerrado (no si ya estaba ahí)
    if stage_current != DEAL_CERRADO_STAGE or stage_previous == DEAL_CERRADO_STAGE:
        return jsonify({'status': 'not a close event', 'stage': stage_current}), 200

    print(f"[PI Generator] Deal {deal_id} cerrado — generando PI...")

    deal_data = get_deal(deal_id)
    if not deal_data:
        return jsonify({'status': 'error', 'msg': 'deal not found'}), 200

    pi_number = get_next_pi_number()
    filename  = f'PI_{pi_number}_{deal_data.get("org_name","")}.docx'.replace(' ', '_')

    try:
        doc_bytes = generate_pi_document(deal_data, pi_number)
    except Exception as e:
        print(f"[PI Generator] Error generando doc: {e}")
        return jsonify({'status': 'error', 'msg': str(e)}), 200

    # Adjuntar en deal
    attach_result = attach_file_to_deal(deal_id, filename, doc_bytes)

    # Adjuntar también en el proyecto asociado
    project_id = get_project_by_deal(deal_id)
    if project_id:
        attach_file_to_project(project_id, filename, doc_bytes)
        print(f'[PI Generator] PI adjuntada también en proyecto {project_id}')
    else:
        print(f'[PI Generator] No se encontró proyecto asociado al deal {deal_id}')

    # FIX #3: NO sobreescribir Ref. Contract con el número de PI
    # El campo Ref. Contract lo gestiona Neuber manualmente con el SC del proveedor
    # (removida la línea que sobreescribía F_REF_CONTRATO)

    # Agregar nota al deal
    add_note_to_deal(deal_id,
        f'✅ PI {pi_number} generada automáticamente al cerrar el deal.\n'
        f'Archivo: {filename}\n'
        f'Fecha: {datetime.now().strftime("%Y-%m-%d %H:%M")}\n\n'
        f'⚠️ Verificar: dirección del comprador, condiciones específicas, banco del proveedor.'
    )

    print(f"[PI Generator] PI {pi_number} generada y adjuntada al deal {deal_id}")
    return jsonify({
        'status': 'success',
        'pi_number': pi_number,
        'filename': filename,
        'deal_id': deal_id
    }), 200


@app.route('/generate_pi/<int:deal_id>', methods=['GET'])
def generate_pi_manual(deal_id):
    """Endpoint para generar PI manualmente para un deal específico."""
    deal_data = get_deal(deal_id)
    if not deal_data:
        return jsonify({'error': 'deal not found'}), 404

    pi_number = get_next_pi_number()
    filename  = f'PI_{pi_number}_{deal_data.get("org_name","")}.docx'.replace(' ', '_')

    doc_bytes    = generate_pi_document(deal_data, pi_number)
    attach_deal  = attach_file_to_deal(deal_id, filename, doc_bytes)

    add_note_to_deal(deal_id,
        f'✅ PI {pi_number} generada manualmente.\nArchivo: {filename}\n'
        f'Fecha: {datetime.now().strftime("%Y-%m-%d %H:%M")}'
    )

    return jsonify({
        'status': 'success',
        'pi_number': pi_number,
        'filename': filename,
        'attach_deal': attach_deal.get('success'),
    })


@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok', 'service': 'Neuber PI Generator'})


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
