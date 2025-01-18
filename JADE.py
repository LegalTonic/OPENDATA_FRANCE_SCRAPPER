import pandas as pd
import requests
import tarfile
import xml.etree.ElementTree as ET
import json
import os
from io import BytesIO
import glob
from tqdm import tqdm

def download_and_extract(url, filename):
    """Download and extract tar.gz file"""
    try:
        response = requests.get(url)
        if response.status_code == 200:
            tar_content = BytesIO(response.content)
            with tarfile.open(fileobj=tar_content, mode='r:gz') as tar:
                tar.extractall('extracted_files')
            print(f"Successfully downloaded and extracted {filename}")
        else:
            print(f"Failed to download {filename}. Status code: {response.status_code}")
    except Exception as e:
        print(f"Error processing {filename}: {str(e)}")


def clean_text(text):
    """Clean text by removing XML/HTML tags and normalizing whitespace"""
    # Remove XML/HTML tags
    text = re.sub(r'<[^>]+>', ' ', text)
    # Replace multiple spaces, newlines and tabs with a single space
    text = re.sub(r'\s+', ' ', text)
    # Replace special XML entities
    text = text.replace('&amp;', '&')
    text = text.replace('&lt;', '<')
    text = text.replace('&gt;', '>')
    text = text.replace('&quot;', '"')
    text = text.replace('&apos;', "'")
    # Final strip and normalization
    return text.strip()

def get_element_text(element, path):
    """Safely get text from XML element"""
    found = element.find(path)
    return found.text.strip() if found is not None and found.text else ''

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
            'formation': '',
            'type_recours': '',
            'publication_recueil': '',
            'president': '',
            'avocats': '',
            'rapporteur': '',
            'commissaire_gouvernement': '',
            'contenu': ''
        }
        
        # Extract metadata from META_COMMUN
        meta_commun = root.find('.//META_COMMUN')
        if meta_commun is not None:
            data['id'] = get_element_text(meta_commun, 'ID')
            data['ancien_id'] = get_element_text(meta_commun, 'ANCIEN_ID')
            data['origine'] = get_element_text(meta_commun, 'ORIGINE')
            data['url'] = get_element_text(meta_commun, 'URL')
            data['nature'] = get_element_text(meta_commun, 'NATURE')
        
        # Extract metadata from META_JURI
        meta_juri = root.find('.//META_JURI')
        if meta_juri is not None:
            data['titre'] = get_element_text(meta_juri, 'TITRE')
            data['date_decision'] = get_element_text(meta_juri, 'DATE_DEC')
            data['juridiction'] = get_element_text(meta_juri, 'JURIDICTION')
            data['numero'] = get_element_text(meta_juri, 'NUMERO')
        
        # Extract metadata from META_JURI_ADMIN
        meta_juri_admin = root.find('.//META_JURI_ADMIN')
        if meta_juri_admin is not None:
            data['formation'] = get_element_text(meta_juri_admin, 'FORMATION')
            data['type_recours'] = get_element_text(meta_juri_admin, 'TYPE_REC')
            data['publication_recueil'] = get_element_text(meta_juri_admin, 'PUBLI_RECUEIL')
            data['president'] = get_element_text(meta_juri_admin, 'PRESIDENT')
            data['avocats'] = get_element_text(meta_juri_admin, 'AVOCATS')
            data['rapporteur'] = get_element_text(meta_juri_admin, 'RAPPORTEUR')
            data['commissaire_gouvernement'] = get_element_text(meta_juri_admin, 'COMMISSAIRE_GVT')
        
        # Extract and clean content
        contenu_element = root.find('.//CONTENU')
        if contenu_element is not None:
            # Get the full text content including nested tags
            content = ''.join(contenu_element.itertext())
            # Clean the text
            data['contenu'] = clean_text(content)
        
        return data
    except Exception as e:
        print(f"Error parsing {xml_path}: {str(e)}")
        return None
    
def main():
    # Create directory for extracted files
    os.makedirs('extracted_files', exist_ok=True)
    
    # Read the CSV file and process it
    with open('JADELISTE.csv', 'r') as f:
        content = f.read()
    
    # Split the content by commas and clean up the filenames
    filenames = [fname.strip().strip(',') for fname in content.split(',')]
    filenames = [fname for fname in filenames if fname]  # Remove empty strings
    
    base_url = 'https://echanges.dila.gouv.fr/OPENDATA/JADE/'
    
    # Download and extract each tar.gz file
    print("Downloading and extracting files...")
    for filename in tqdm(filenames):
        url = base_url + filename
        download_and_extract(url, filename)
    
    # Process all XML files
    print("Processing XML files...")
    results = []
    
    # Adjust this path to point to your extracted XML files
    xml_files = glob.glob('extracted_files/**/*.xml', recursive=True)
    
    for xml_file in tqdm(xml_files):
        data = parse_xml_file(xml_file)
        if data:
            results.append(data)
    
    # Write to JSONL file
    print("Writing results to JSONL file...")
    with open('jade_dataset_clean.jsonl', 'w', encoding='utf-8') as f:
        for item in results:
            f.write(json.dumps(item, ensure_ascii=False) + '\n')
    
    # Create a sample DataFrame to verify data
    df_sample = pd.DataFrame(results[:5])
    print("\nSample of processed data:")
    print(df_sample[['titre', 'date_decision', 'juridiction', 'numero']].head())
    print("\nSample of content (first 200 characters):")
    for content in df_sample['contenu'].head():
        print(content[:200] + "...\n")
    
    print(f"Process completed. Dataset saved as 'jade_dataset_clean.jsonl'")
    print(f"Total number of documents processed: {len(results)}")

if __name__ == "__main__":
    main()