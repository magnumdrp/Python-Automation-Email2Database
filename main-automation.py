#!/usr/bin/env python3
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from datetime import datetime
from pytz import timezone
from sqlalchemy.orm import declarative_base, sessionmaker
from PIL import Image
from email.header import decode_header
import imaplib
import email
import openai
import schedule
import time
import pdfplumber
import json
import pandas as pd
import os
import re

saopaulo_tz = timezone('America/Sao_Paulo')

IMAP_SERVER = "imap.gmail.com"
EMAIL_USER = "emailexample@example.com"
EMAIL_PASS = "example_app_password"

DB_URL = "mysql+pymysql://exampledbuser:examplepassword@examplehost/exampledbname"

KEYWORDS = [
    "C.Fiscal", "c.fiscal", "C.fiscal", "Cód.", "cód.", "Cód.",
    "Código", "código", "Código", "Descrição", "descrição", "Descrição",
    "Entrega", "entrega", "Entrega", "ICMS", "icms", "Icms", "ICMS(%)",
    "icms(%)", "Icms(%)", "IPI", "ipi", "Ipi", "IPI(%)", "ipi(%)", "Ipi(%)",
    "Item", "item", "Item", "Lista", "lista", "Lista", "Marca", "marca", "Marca",
    "Material", "material", "Material", "NCM", "ncm", "Ncm", "NCM/Orig", "ncm/orig",
    "Ncm/orig", "Parcial", "parcial", "Parcial", "Pr.", "pr.", "Pr.", "Prev.", "prev.",
    "Prev.", "Produto", "produto", "Produto", "Produto/serviço", "produto/serviço", "Produto/serviço",
    "PU", "pu", "Pu", "Qtde", "qtde", "Qtde", "Qtde.", "qtde.", "Qtde.", "Quantid.", "quantid.", 
    "Quantid.", "QDE", "qde", "Qde", "R$", "r$", "R$", "ST", "st", "St", "Sub.Trib.", "sub.trib.", 
    "Sub.trib.", "Total", "total", "Total", "UM", "um", "Um", "Und.", "und.", "Und.", "Un.", "un.",
    "Un.", "Unid.", "unid.", "Unid.", "Unitário", "unitário", "Unitário", "V.", "v.", "V.", "Val", 
    "val", "Val", "Valor", "valor", "Valor", "Vl.", "vl.", "Vl.", "VR", "vr", "Vr", "V.ST", "v.st", 
    "V.st", "V.TOTAL", "v.total", "V.total", "P.UNITÁRIO", "p.unitário", "P.unitário", "UNIT.", "unit.", 
    "Unit.", "Unitário", "unitário", "Unitário", "%ICMS", "%icms", "%Icms", "CST", "cst", "Cst", 
    "Desconto", "desconto", "Desconto"
]

chatgpt_responses = {}
openai.api_key = "openai-key-example"

field_mapping = {
    
    "STARK INDUSTRIA E COMERCIO LTDA": { 
        "produtos": {
            "Código": "code",
            "Descrição": "description",
            "Und.": "unit",
            "NCM": "ncm",
            "Qtde.": "quantity",
            "ICMS(%)": "icms_percent",
            "IPI(%)": "ipi_percent",
            "PU c/ IPI": "unit_price",
            "Total": "total_price"
        }
    },
    
     "FBC": { 
        "produtos": {
            "Item": "ordenation",
            "Descrição": "description",
            "UM": "unit",
            "NCM": "code",
            "Quantidade": "quantity",
            "Vlr. Unit.": "unit_price",
            "Vl. ICMS": "icms_value",
            "Al. Vl. IPI": "ipi_percent",
            "Vl. IPI": "ipi_value",
            "Vlr. Total": "total_price"
        }
    },
    
    "Clickdata": { 
        "produtos": {
            "Referência": "code",
            "NCM": "ncm",
            "Qtde": "quantity",
            "Moeda": "currency",
            "Valor Unitário": "unit_price",
            "Valor Total": "total_price",
            "Dias Úteis": "send_deadline"
        }
    },
    
     "BELLFONE": { 
        "produtos": { 
          "ITEM": "ordenation",
            "CÓDIGO": "code",
            "DESCRIÇÃO": "description",
            "MARCA": "manufacturer",
            "QDE": "quantity",
            "UN": "unit",
            "UNITÁRIO": "unit_price",
            "ENTREGA": "send_deadline"
        }
    },
        
    
     "Pauta Distribuição": {
        "produtos": {
            "CÓDIGO": "code",
            "DESCRIÇÃO": "description",
            "VALOR UNI": "unit_price"
        }
    },
        
    
    "Unentel": {
        "produtos": {
            "Item": "ordenation",
            "CD": "cd",
            "Fabricante": "manufacturer",
            "PN": "960-001401",
            "Descrição do Produto": "description",
            "Qtd": "quantity",
            "Moeda": "BRL",
            "Categoria": "category",
            "PIS/Cofins": "pis_confis_value",
            "ISS": "iss_percent",
            "ICMS": "icms_percent",
            "IPI": "ipi_percent",
            "Unitário": "unit_price",
            "Total": "total_value",
            "Disponibilidade": "send_deadline",
            "Class. Fiscal": "fiscalc"
        }
    },
        
    
    "WDC Networks": {
        "produtos": {
            "Item": "ordenation",
            "Fabricante": "manufacturer",
            "Código WDC": "code",
            "Descrição": "description",
            "Entrega": "send_deadline",
            "NCM": "ncm",
            "IPI": "ipi_value",
            "PIS / COFINS": "pis_confis_value",
            "ICMS / ISS": "icms_value",
            "ST": "st_value",
            "Quantidade": "quantity",
            "Valor Unitário": "unit_price",
            "Valor total": "total_price"
        }
    },
        
    "A Loja da limpeza": {
        'produtos': {
            'Qt.': 'quantity',
            'Produto/Serviço': 'description',
        }
    },
    "ifontech": {
        'produtos': {
            'Código': 'code',
            'Descrição': 'description',
            'Quantidade': 'quantity',
            'Unitário': 'unit_price',
            'Total': 'total_price'
        }
    },
    "ecoprint": {
        'produtos': {
            'Código': 'code',
            'Descrição': 'description',
            'Qtde': 'quantity',
            'Vlr Unit.': 'unit_price',
            'Vlr Total': 'total_price'
        }
    },
    "natural": {
        'produtos': {
            'Código-NCM': 'code',
            'Descrição': 'description',
            'Qtde': 'quantity',
            'R$ Unit.': 'unit_price',
            'Preço Total': 'total_price'
        }
    },
    "essenza": {
        'produtos': {
            'Descrição': 'description',
            'Nº Item SKU': 'code',
            'NCM': 'ncm',
            'Un': 'unit',
            'Qtd': 'quantity',
            'Preço': 'unit_price',
            'Total': 'total_price'
        }
    },
    "contabilista": {
        'produtos': {
            'Produto': 'ordenation',
            'Descrição': 'description',
            'NCM': 'ncm',
            'Quantidade': 'quantity',
            'UN': 'unit',
            'Valor Unitário Líquido': 'unit_price',
            'ICMS': 'icms_value',
            'ICMS %': 'icms_percent',
            'Vlr. Líq.': 'total_price'
        }
    },
    "nsteleinformatica": {
        'produtos': {
            'Cód. Ref.': 'code',
            'Descrição': 'description',
            'NCM': 'ncm',
            'ICMS': 'icms_value',
            'Marca': 'manufacturer',
            'Unid.': 'unit',
            'Quant.': 'quantity',
            'Unit.': 'unit_price',
            'Total': 'total_price'
        }
    },
    "dicomp": {
        'produtos': {
            'Cód.': 'code',
            'Descrição': 'description',
            'NCM': 'ncm',
            'CST': 'manufacturer',
            'Qtd.': 'quantity',
            'Vl. Líquido': 'unit_price',
            'Vl. T. Líquido': 'total_price',
            'IPI': 'ipi_percent',
            'Vl. ST': 'st_value'
        }
    },
    "superpro": {
        'produtos': {
            'Produto': 'description',
            'Quantidade': 'quantity',
            'Valor': 'unit_price'
        }
    },
    "dcadelta": {
        'produtos': {
            'Material': 'description',
            'Und.': 'unit',
            'NCM': 'ncm',
            'Qtde.': 'quantity',
            'ICMS(%)': 'icms_percent',
            'IPI(%)': 'ipi_percent',
            'PU c/ IPI': 'unit_price',
            'Total c/ IPI': 'total_price'
        }
    },
    "simples": {
        'produtos': {
            'Item': 'ordenation',
            'Código': 'code',
            'Descrição': 'description',
            'Qtde.': 'quantity',
            'Un.': 'unit',
            '% ICMS': 'icms_percent',
            'ST': 'st_value',
            'NCM': 'ncm',
            '% IPI': 'ipi_percent',
            'Valor Unit.': 'unit_price',
            'Valor Total': 'total_price'
        }
    },
    "distribuidora_blumenau": {
        'produtos': {
            'Produto': 'description',
            'QTD': 'quantity',
            'UM': 'unit',
            'Val Unit.': 'unit_price',
            'Val ST': 'st_value',
            'Val. Total': 'total_price',
            'Val ICMS': 'icms_value',
            '% ICMS': 'icms_percent'
        }
    },
    "star_cabos": {
        'produtos': {
            'Descrição': 'description',
            'Código /C.Fabr.': 'code',
            'Qtd.': 'quantity',
            'Unitário': 'unit_price',
            'Parcial': 'total_price'
        }
    },
    "adecom_materiais_eletricos": {
        'produtos': {
            'Item': 'ordenation',
            'Código': 'code',
            'Descrição': 'description',
            'Qtde': 'quantity',
            'Unid.': 'unit',
            'Unitário R$': 'unit_price',
            'IPI %': 'ipi_percent',
            'IPI R$': 'ipi_value',
            'ST R$': 'st_value',
            'R$ TOTAL C/IMPOSTO': 'total_price'
        }
    },
    "ads": { 
        'produtos': {
            'Item': 'ordenation',
            'Código': 'code',
            'Descrição do Item': 'description',
            'Quantid.': 'quantity',
            'Un': 'unit',
            'Desc.%': 'discount_percent',
            'Unit.R$': 'unit_price',
            'Total R$': 'total_price',
            'MVA %': 'mva',
            'IPI %': 'ipi_percent',
            'ICMS %': 'icms_percent',
            'CST': 'cst'
        }
    },
    "cabos_vieira_comercio": {
        'produtos': {
            'Código': 'code',
            'Qtd.': 'quantity',
            'Descrição do\nproduto/serviço': 'description',
            'Un': 'unit',
            'Preço un.': 'unit_price',
            'Preço total': 'total_price',
            'Desconto %': 'discount_percent'
        }
    },
    "ferramentas_gerais": {
        'produtos': {
            'It.': 'ordenation',
            'Código': 'code',
            'Quantid.': 'quantity',
            'Descrição': 'description',
            'NCM/Orig': 'ncm',
            'UN': 'unit',
            'Valor unitário R$': 'unit_price',
            'Valor total mercad. R$': 'total_price',
            'Valor ST R$': 'st_value',
            '% ST': 'st_percent',
            'Valor ICMS R$': 'icms_value',
            '% ICMS': 'icms_percent',
            'Valor IPI R$': 'ipi_value',
            '% IPI': 'ipi_percent',
            'Marca': 'manufacturer'
        }
    },
     "klint": {
        'produtos': {
            'ITEM': 'ordenation',
            'CÓDIGO': 'code',
            'C.Fiscal': 'fiscalc',
            'V.ST': 'st_value',
            'ST': 'st_percent',
            'QDE': 'quantity',
            'DESCRIÇÃO': 'description',
            'UN': 'unit',
            'P.UNITÁRIO': 'unit_price',
            'V.TOTAL': 'total_price',
            'ENTREGA': 'send_deadline',
            'ICMS(%)': 'icms_percent',
            'MARCA': 'manufacturer'
        }
    },
    "andra": {
        'produtos': {
            'It': 'ordenation',
            'Qtde': 'quantity',
            'Descrição dos Produtos': 'description',
            'Ncm': 'ncm',
            'Um': 'unit',
            'Pr. Unit.': 'unit_price',
            'Pr. Total': 'total_price',
            'Prev. Entrega': 'send_deadline',
            '% Icms': 'icms_percent'
        }
    }
}


def current_saopaulo_time():
    return datetime.now(saopaulo_tz)

Base = declarative_base()

class ProductTeste(Base):
                __tablename__ = 'back_quotations'
                
                id = Column(Integer, primary_key=True, autoincrement=True)
                sender = Column(String(255)) 
                quotationcode = Column(String(255)) 
                cnpj = Column(String(255)) 
                ordenation = Column(String(255))
                code = Column(String(255))
                fiscalc = Column(String(255))
                quantity = Column(String(255))
                description = Column(String(255))
                obs = Column(String(255))
                ncm = Column(String(255))
                mva = Column(String(255))
                cst = Column(String(255))
                unit = Column(String(255))
                unit_price = Column(String(255))
                total_price = Column(String(255))
                send_deadline = Column(String(255))
                st_value = Column(String(255))
                st_percent = Column(String(255))
                pis_cofins_value = Column(String(255))
                pis_cofins_percent = Column(String(255))
                icms_value = Column(String(255))
                icms_percent = Column(String(255))
                ipi_value = Column(String(255))
                ipi_percent = Column(String(255))
                iss_value = Column(String(255))
                iss_percent = Column(String(255))
                manufacturer = Column(String(255))
                discount_percent = Column(String(255)) 
                cd = Column(String(255)) 
                currency = Column(String(255))
                category = Column(String(255))
                cfop = Column(String(255))
                created_at = Column(DateTime, default=current_saopaulo_time)
                updated_at = Column(DateTime, default=current_saopaulo_time, onupdate=current_saopaulo_time)
                                


def save_attachment_or_embed_as_pdf(part, save_dir="/tmp"):
    content_type = part.get_content_type()
    if content_type.startswith("image/"):
        filename = part.get_filename() or "embed_image.jpg"
        safe_filename = re.sub(r'[^\w\-_\. ]', '_', filename)
        image_path = os.path.join(save_dir, safe_filename)
        
        with open(image_path, "wb") as f:
            f.write(part.get_payload(decode=True))
        
        pdf_path = os.path.join(save_dir, safe_filename.rsplit(".", 1)[0] + ".pdf")
        with Image.open(image_path) as img:
            img.convert("RGB").save(pdf_path, "PDF")
        
        os.remove(image_path)
        return pdf_path
    
    elif content_type == "application/pdf":
        filename = part.get_filename() or "attachment.pdf"
        safe_filename = re.sub(r'[^\w\-_\. ]', '_', filename)
        pdf_path = os.path.join(save_dir, safe_filename)
        
        with open(pdf_path, "wb") as f:
            f.write(part.get_payload(decode=True))
        return pdf_path

    return None


def download_pdf_attachments():
    mail = imaplib.IMAP4_SSL(IMAP_SERVER)
    mail.login(EMAIL_USER, EMAIL_PASS)
    mail.select("inbox")

    status, messages = mail.search(None, '(UNSEEN)')
    
    if messages[0] == b"":  
        mail.close()
        mail.logout()
        return [], "sem mensagens", "", "", ""
    
    downloaded_files = []
    sender = ""
    body = ""
    subject = ""
    quotationcode = None
    cnpj = None
    
    for num in messages[0].split():
        status, msg_data = mail.fetch(num, "(RFC822)")
        msg = email.message_from_bytes(msg_data[0][1])
        sender = msg.get("From")
        email_match = re.search(r'<(.+?)>', sender)  
        if email_match:
            sender = email_match.group(1)  

        subject_raw = msg.get("Subject")
        if subject_raw:
            decoded_parts = decode_header(subject_raw)
            decoded_subject = ""
            for part, encoding in decoded_parts:
                if isinstance(part, bytes):
                    decoded_subject += part.decode(encoding or 'utf-8')
                else:
                    decoded_subject += part
            subject = decoded_subject

            quotationcode_match = re.search(r'<(.+?)>', subject)
            if quotationcode_match:
                quotationcode = quotationcode_match.group(1)

            cnpj_match = re.search(r'\((.+?)\)', subject)
            if cnpj_match:
                cnpj = cnpj_match.group(1)

        print(f"Email recibido de: {sender}")
        print(f"Asunto: {subject}")
        print(f"Código: {quotationcode}")
        print(f"CNPJ: {cnpj}")
        
        for part in msg.walk():
            if part.get_content_type() == "text/plain":  
                part_body = part.get_payload(decode=True).decode(part.get_content_charset() or "utf-8")
                body += part_body  
                
        print(f"Cuerpo: {body}")

        for part in msg.walk():
            pdf_file = save_attachment_or_embed_as_pdf(part)
            if pdf_file:
                downloaded_files.append(pdf_file)

    mail.close()
    mail.logout()
    
    return downloaded_files, sender, body, quotationcode, cnpj



def extract_table_from_pdf(pdf_file, body):
    
        text_from_pdf = ""
        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                text_from_pdf += page.extract_text() + "\n"
        
        text_hash = hash(body + text_from_pdf)
        
        if text_hash in chatgpt_responses:
            print("Resposta previamente armazenada para este texto!")
            return chatgpt_responses[text_hash]

        retries = 1
        for attempt in range(retries):
            try:
                response = openai.ChatCompletion.create(
                    model="gpt-3.5-turbo",
                    messages=[
                        {
                            "role": "system", 
                            "content": (
                                "Extraia a tabela de produtos contida no texto extraido do PDF de orçamento, mantendo os nomes dos cabeçalhos na forma original. "
                                "utilize sempre o mesmo formato JSON: 'produtos': [{ '...': '...', '...': '...' }]. "
                                "Responda em português brasileiro (pt-BR) e garanta consistência."
                            )
                        },
                        {"role": "user", "content": body + text_from_pdf}
                    ],
                    max_tokens=4096,
                    temperature=0.2,
                )

                chatgpt_responses[text_hash] = response["choices"][0]["message"]["content"]
                print("ChatGPT Response")
                return chatgpt_responses[text_hash]
            except openai.error.RateLimitError as e:
                if attempt < retries - 1:
                    print(f"Rate limit exceeded. Retrying in 30 seconds... (Attempt {attempt+1})")
                    time.sleep(30)
                else:
                    print("Rate limit exceeded. Please check your OpenAI plan or try again later.")
                    return []
                   
def create_table():
    engine = create_engine(DB_URL)
    Base.metadata.create_all(engine)
    print("Tabela criada com sucesso.")

def insert_into_database(data, sender, quotationcode, cnpj):
    
    if sender not in field_mapping:
        print(f"Sender {sender} desconocido.")
        return
    
    engine = create_engine(DB_URL)
    Session = sessionmaker(bind=engine)
    json_data = data
    datapos = json.loads(json_data)
    
    with Session() as session:
        try:
            for row in datapos['produtos']:
                new_product = ProductTeste(
                    sender = sender,
                    quotationcode=quotationcode,
                    cnpj=cnpj
                )

                for json_field, db_field in field_mapping[sender]['produtos'].items():
                    value = row.get(json_field)
                    if value is not None:
                        setattr(new_product, db_field, value)
                
                session.add(new_product)
                
            session.commit()
            print("Dados inseridos com sucesso.")
        except Exception as e:
            session.rollback()
            print(f"Erro ao inserir dados: {e}")
                  
def process_email_quotes():
    downloaded_files, sender, body, quotationcode , cnpj = download_pdf_attachments()
    if sender == "sem mensagens":
        print("Nenhuma mensagem não lida. Nada a processar.")
        return
    else:
        for pdf_file in downloaded_files:
            tables = extract_table_from_pdf(pdf_file, body)
            create_table()
            print(tables)
            insert_into_database(tables, sender, quotationcode, cnpj)
            os.remove(pdf_file)

if __name__ == "__main__":
    schedule.every(10).seconds.do(process_email_quotes)
    while True:
        schedule.run_pending()
        time.sleep(1)
