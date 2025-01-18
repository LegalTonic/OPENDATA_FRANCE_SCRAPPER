import pandas as pd
import requests
import tarfile
import xml.etree.ElementTree as ET
import json
import os
import re
from io import BytesIO
import glob
from tqdm import tqdm
from pathlib import Path
import logging
from datetime import datetime
import shutil

# Set up logging
logging.basicConfig(
    filename=f'inca_processing_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
logging.getLogger().addHandler(console_handler)

def download_and_extract(url, filename, extract_dir):
    """Download and extract tar.gz file"""
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            temp_tar_path = os.path.join(extract_dir, filename)
            with open(temp_tar_path, 'wb') as f:
                f.write(response.content)
            
            with tarfile.open(temp_tar_path, 'r:gz') as tar:
                tar.extractall(path=extract_dir)
            
            os.remove(temp_tar_path)
            logging.info(f"Successfully downloaded and extracted {filename}")
            return True
        else:
            logging.error(f"Failed to download {filename}. Status code: {response.status_code}")
            return False
    except Exception as e:
        logging.error(f"Error processing {filename}: {str(e)}")
        return False

def clean_text(text):
    """Clean text by removing XML/HTML tags and normalizing whitespace"""
    if not text:
        return ""
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    text = text.replace('&amp;', '&')
    text = text.replace('&lt;', '<')
    text = text.replace('&gt;', '>')
    text = text.replace('&quot;', '"')
    text = text.replace('&apos;', "'")
    return text.strip()

def get_element_text(element, path):
    """Safely get text from XML element"""
    found = element.find(path)
    return clean_text(found.text) if found is not None and found.text else ''

def parse_xml_file(xml_path):
    """Parse XML file and return dictionary of relevant fields"""
    try:
        parser = ET.XMLParser(encoding="utf-8")
        tree = ET.parse(xml_path, parser=parser)
        root = tree.getroot()
        
        data = {
            'id': '',
            'ancien_id': '',
            'origine': '',
            'url': '',
            'nature': '',
            'titre': '',
            'date_decision': '',
            'juridiction': '',
            'numero': '',
            'solution': '',
            'numero_affaire': '',
            'publie_bulletin': '',
            'formation': '',
            'date_decision_attaquee': '',
            'juridiction_attaquee': '',
            'siege_appel': '',
            'juridiction_premiere_instance': '',
            'lieu_premiere_instance': '',
            'demandeur': '',
            'defendeur': '',
            'president': '',
            'avocat_general': '',
            'avocats': '',
            'rapporteur': '',
            'ecli': '',
            'contenu': '',
            'sommaire': ''
        }
        
        # META_COMMUN
        meta_commun = root.find('.//META_COMMUN')
        if meta_commun is not None:
            data.update({
                'id': get_element_text(meta_commun, 'ID'),
                'ancien_id': get_element_text(meta_commun, 'ANCIEN_ID'),
                'origine': get_element_text(meta_commun, 'ORIGINE'),
                'url': get_element_text(meta_commun, 'URL'),
                'nature': get_element_text(meta_commun, 'NATURE')
            })
        
        # META_SPEC
        meta_spec = root.find('.//META_SPEC')
        if meta_spec is not None:
            meta_juri = meta_spec.find('.//META_JURI')
            if meta_juri is not None:
                data.update({
                    'titre': get_element_text(meta_juri, 'TITRE'),
                    'date_decision': get_element_text(meta_juri, 'DATE_DEC'),
                    'juridiction': get_element_text(meta_juri, 'JURIDICTION'),
                    'numero': get_element_text(meta_juri, 'NUMERO'),
                    'solution': get_element_text(meta_juri, 'SOLUTION')
                })
            
            meta_juri_judi = meta_spec.find('.//META_JURI_JUDI')
            if meta_juri_judi is not None:
                publi_bull = meta_juri_judi.find('PUBLI_BULL')
                data.update({
                    'numero_affaire': get_element_text(meta_juri_judi, './/NUMERO_AFFAIRE'),
                    'publie_bulletin': publi_bull.get('publie') if publi_bull is not None else '',
                    'formation': get_element_text(meta_juri_judi, 'FORMATION'),
                    'date_decision_attaquee': get_element_text(meta_juri_judi, 'DATE_DEC_ATT'),
                    'juridiction_attaquee': get_element_text(meta_juri_judi, 'FORM_DEC_ATT'),
                    'siege_appel': get_element_text(meta_juri_judi, 'SIEGE_APPEL'),
                    'juridiction_premiere_instance': get_element_text(meta_juri_judi, 'JURI_PREM'),
                    'lieu_premiere_instance': get_element_text(meta_juri_judi, 'LIEU_PREM'),
                    'demandeur': get_element_text(meta_juri_judi, 'DEMANDEUR'),
                    'defendeur': get_element_text(meta_juri_judi, 'DEFENDEUR'),
                    'president': get_element_text(meta_juri_judi, 'PRESIDENT'),
                    'avocat_general': get_element_text(meta_juri_judi, 'AVOCAT_GL'),
                    'avocats': get_element_text(meta_juri_judi, 'AVOCATS'),
                    'rapporteur': get_element_text(meta_juri_judi, 'RAPPORTEUR'),
                    'ecli': get_element_text(meta_juri_judi, 'ECLI')
                })
        
        # TEXTE content
        contenu = root.find('.//CONTENU')
        if contenu is not None:
            data['contenu'] = clean_text(''.join(contenu.itertext()))
        
        # SOMMAIRE
        sommaire = root.find('.//SOMMAIRE')
        if sommaire is not None:
            data['sommaire'] = clean_text(''.join(sommaire.itertext()))
        
        return data
    except Exception as e:
        logging.error(f"Error parsing {xml_path}: {str(e)}")
        return None

def write_batch_to_jsonl(batch_data, output_file, mode='a'):
    """Write a batch of data to JSONL file"""
    try:
        with open(output_file, mode, encoding='utf-8') as f:
            for item in batch_data:
                f.write(json.dumps(item, ensure_ascii=False) + '\n')
        return True
    except Exception as e:
        logging.error(f"Error writing batch to file: {str(e)}")
        return False

def main():
    # Create directories
    base_dir = Path('inca_data')
    extract_dir = base_dir / 'extracted_files'
    
    # Clean up existing directories
    if os.path.exists(base_dir):
        shutil.rmtree(base_dir)
    
    os.makedirs(base_dir, exist_ok=True)
    os.makedirs(extract_dir, exist_ok=True)
    
    output_file = base_dir / 'inca_dataset.jsonl'
    batch_size = 1000
    
    # Read the CSV file
    try:
        with open('INCALISTE.csv', 'r') as f:
            content = f.read()
        filenames = [fname.strip().strip(',') for fname in content.split(',')]
        filenames = [fname for fname in filenames if fname]
        logging.info(f"Found {len(filenames)} files to process in INCALISTE.csv")
    except Exception as e:
        logging.error(f"Error reading INCALISTE.csv: {str(e)}")
        return
    
    base_url = 'https://echanges.dila.gouv.fr/OPENDATA/INCA/'
    
    # Download and extract files
    logging.info("Starting download and extraction process...")
    for filename in tqdm(filenames, desc="Downloading files"):
        url = base_url + filename
        download_and_extract(url, filename, extract_dir)
    
    # Process XML files
    logging.info("Searching for XML files...")
    xml_files = []
    for root, dirs, files in os.walk(extract_dir):
        for file in files:
            if file.endswith('.xml'):
                xml_files.append(os.path.join(root, file))
    
    logging.info(f"Found {len(xml_files)} XML files to process")
    
    if not xml_files:
        logging.error("No XML files found to process!")
        return
    
    # Clear output file if it exists
    open(output_file, 'w').close()
    
    # Process XML files
    batch_data = []
    total_processed = 0
    
    for xml_file in tqdm(xml_files, desc="Processing XML files"):
        data = parse_xml_file(xml_file)
        if data:
            batch_data.append(data)
            total_processed += 1
            
            if len(batch_data) >= batch_size:
                write_batch_to_jsonl(batch_data, output_file)
                batch_data = []
                logging.info(f"Processed {total_processed} documents")
    
    # Write remaining documents
    if batch_data:
        write_batch_to_jsonl(batch_data, output_file)
    
    logging.info(f"Process completed. Total documents processed: {total_processed}")
    logging.info(f"Dataset saved as '{output_file}'")
    
    # Create a sample DataFrame to verify data
    if total_processed > 0:
        try:
            with open(output_file, 'r', encoding='utf-8') as f:
                sample_data = [json.loads(next(f)) for _ in range(min(5, total_processed))]
            df_sample = pd.DataFrame(sample_data)
            logging.info("\nSample of processed data:")
            logging.info(df_sample[['titre', 'date_decision', 'juridiction', 'numero']].head())
        except Exception as e:
            logging.error(f"Error creating sample DataFrame: {str(e)}")

if __name__ == "__main__":
    main()