# Scripts

1. HathiTrust Webscraper `ht_webscraper.py`
   - Uses the annotation data to scrape hathitrust for volumes
   - Creates the `annotation_metadata_mapping.csv` file
2. HathiTrust Extracted Features Cleaner `ht_ef_cleaner.py`
   - Creates the volumes from the extracted features and combines them
   - Creates the `directory_annotation_metadata_mapping.csv` file
3. Process HathiTrust Data `process_ht.py`
   - Processes and cleans the hathitrust text data with spaCy
   - Creates the final file `directory_annotation_metadata_mapping_processed.csv`