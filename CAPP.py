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

# Set up logging
logging.basicConfig(
    filename=f'capp_processing_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def download_and_extract(url, filename, extract_dir):
    """Download and extract tar.gz file"""
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            tar_content = BytesIO(response.content)
            with tarfile.open(fileobj=tar_content, mode='r:gz') as tar:
                tar.extractall(extract_dir)
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
            'formation': '',
            'siege_appel': '',
            'juridiction_premiere_instance': '',
            'lieu_premiere_instance': '',
            'president': '',
            'avocat_general': '',
            'avocats': '',
            'rapporteur': '',
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
        
        # META_JURI
        meta_juri = root.find('.//META_JURI')
        if meta_juri is not None:
            data.update({
                'titre': get_element_text(meta_juri, 'TITRE'),
                'date_decision': get_element_text(meta_juri, 'DATE_DEC'),
                'juridiction': get_element_text(meta_juri, 'JURIDICTION'),
                'numero': get_element_text(meta_juri, 'NUMERO'),
                'solution': get_element_text(meta_juri, 'SOLUTION')
            })
        
        # META_JURI_JUDI
        meta_juri_judi = root.find('.//META_JURI_JUDI')
        if meta_juri_judi is not None:
            numero_affaire = meta_juri_judi.find('.//NUMERO_AFFAIRE')
            data.update({
                'numero_affaire': numero_affaire.text if numero_affaire is not None else '',
                'formation': get_element_text(meta_juri_judi, 'FORMATION'),
                'siege_appel': get_element_text(meta_juri_judi, 'SIEGE_APPEL'),
                'juridiction_premiere_instance': get_element_text(meta_juri_judi, 'JURI_PREM'),
                'lieu_premiere_instance': get_element_text(meta_juri_judi, 'LIEU_PREM'),
                'president': get_element_text(meta_juri_judi, 'PRESIDENT'),
                'avocat_general': get_element_text(meta_juri_judi, 'AVOCAT_GL'),
                'avocats': get_element_text(meta_juri_judi, 'AVOCATS'),
                'rapporteur': get_element_text(meta_juri_judi, 'RAPPORTEUR')
            })
        
        # Content and Summary
        contenu = root.find('.//CONTENU')
        if contenu is not None:
            data['contenu'] = clean_text(''.join(contenu.itertext()))
            
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
    base_dir = Path('capp_data')
    extract_dir = base_dir / 'extracted_files'
    os.makedirs(base_dir, exist_ok=True)
    os.makedirs(extract_dir, exist_ok=True)
    
    output_file = base_dir / 'capp_dataset.jsonl'
    batch_size = 1000  # Number of documents to process before writing to file
    
    # Read the CSV file
    try:
        with open('CAPPLISTE.csv', 'r') as f:
            content = f.read()
        filenames = [fname.strip().strip(',') for fname in content.split(',')]
        filenames = [fname for fname in filenames if fname]
    except Exception as e:
        logging.error(f"Error reading CAPPLISTE.csv: {str(e)}")
        return
    
    base_url = 'https://echanges.dila.gouv.fr/OPENDATA/CAPP/'
    
    # Download and extract files
    logging.info("Starting download and extraction process...")
    for filename in tqdm(filenames, desc="Downloading files"):
        url = base_url + filename
        download_and_extract(url, filename, extract_dir)
    
    # Process XML files
    logging.info("Processing XML files...")
    batch_data = []
    total_processed = 0
    
    # Clear output file if it exists
    open(output_file, 'w').close()
    
    xml_files = glob.glob(str(extract_dir / '**' / '*.xml'), recursive=True)
    
    for xml_file in tqdm(xml_files, desc="Processing XML files"):
        data = parse_xml_file(xml_file)
        if data:
            batch_data.append(data)
            total_processed += 1
            
            # Write batch when it reaches batch_size
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
    try:
        with open(output_file, 'r', encoding='utf-8') as f:
            sample_data = [json.loads(next(f)) for _ in range(5)]
        df_sample = pd.DataFrame(sample_data)
        logging.info("\nSample of processed data:")
        logging.info(df_sample[['titre', 'date_decision', 'juridiction', 'numero']].head())
    except Exception as e:
        logging.error(f"Error creating sample DataFrame: {str(e)}")

if __name__ == "__main__":
    main()