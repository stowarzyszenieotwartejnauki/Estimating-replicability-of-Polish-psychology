import pandas as pd
import os
import random
import string
from typing import Dict, List, Tuple, Optional, Union, Any
from SONaa_Tools import NameMatcher, AuthorLookup, NameStandardizer
import re

class SONaa:
    """
    Society for Open Science.
    Manages authors, articles, and their associations.
    """
    
    def __init__(self):
        """Initialize the SONaa system with empty data structures."""
        self.authors = None
        self.articles = None
        self.files = None
        
        # Define required columns for articles
        self.ARTICLE_COLUMNS = [
            'Article_ID', 'doi', 'title', 'date', 'journal', 'authors',
            'pdf_url', 'landing_page_url', 'oa_url'
        ]
        
        # Initialize utilities
        self.standardizer = NameStandardizer()
        self._author_lookup = None
        self.name_matcher = None
        
        # Initialize progress bar utility
        self.tqdm = None
        try:
            from tqdm import tqdm
            self.tqdm = tqdm
        except ImportError:
            self.tqdm = lambda x, **kwargs: x
    
    def legacy_load_authors(self, filepath: str) -> None:
        """
        Load authors from a CSV file and standardize name format.
        
        Args:
            filepath: Path to the CSV file containing author data
        """        
        # Load the CSV file
        self.authors = pd.read_csv(filepath)
        
        # Store original AIDs to extract the 3-letter codes
        if 'Aid' in self.authors.columns:
            self.authors['original_Aid'] = self.authors['Aid']
        
        # Standardize fullname format
        if 'fullname' in self.authors.columns:
            # Format names properly
            self.authors['fullname'] = self.authors['fullname'].apply(self._format_name)
        
        # Update AIDs based on corrected fullnames
        if 'fullname' in self.authors.columns and 'original_Aid' in self.authors.columns:
            self.authors['Aid'] = self.authors.apply(self._create_new_aid, axis=1)
            # Remove temporary column
            self.authors.drop(columns=['original_Aid'], inplace=True)
        
        # Create a persistent standardized names column
        self.authors['std_fullname'] = self.authors['fullname'].apply(
            lambda x: self.standardizer.standardize(x))
        
        # Ensure alternative_names column exists
        if 'alternative_names' not in self.authors.columns:
            self.authors['alternative_names'] = None
        
        # Initialize lookup and matcher
        self._initialize_lookup_and_matcher()
        
        print(f"Loaded {len(self.authors)} authors with standardized name format and updated AIDs.")
    
    def _format_name(self, name: str) -> str:
        """
        Format a name with proper capitalization and handling of hyphenated names.
        
        Args:
            name: Name to format
            
        Returns:
            Formatted name
        """
        if not isinstance(name, str):
            return name
        
        # Use standardize_name for basic standardization (but don't lowercase)
        name = self.standardizer.standardize(name, to_lower=False)
        
        # Split name into words
        words = name.split()
        
        # Format each word: first letter uppercase, rest lowercase
        formatted_words = []
        for word in words:
            if not word:
                continue
                
            # Handle hyphenated words (like surnames)
            if '-' in word:
                parts = word.split('-')
                # Capitalize each part
                parts = [p[0].upper() + p[1:].lower() if p else '' for p in parts]
                formatted_words.append('-'.join(parts))
            else:
                # Regular word: first letter uppercase, rest lowercase
                formatted_words.append(word[0].upper() + word[1:].lower())
        
        # Join back together
        return ' '.join(formatted_words)
    
    def _create_new_aid(self, row: pd.Series) -> str:
        """
        Create a new Autorid from fullname and original Aid.
        
        Args:
            row: DataFrame row containing fullname and original_Aid
            
        Returns:
            New Aid in format fullname_XXX
        """
        fullname = row['fullname']
        original_aid = row['original_Aid']
        
        if not isinstance(fullname, str) or not isinstance(original_aid, str):
            return original_aid
        
        # Extract the 3-letter code from the original AID if it exists
        three_letter_code = None
        if '_' in original_aid:
            parts = original_aid.split('_')
            if len(parts[-1]) == 3:  # Check if last part is a 3-letter code
                three_letter_code = parts[-1]
        
        # If no valid 3-letter code found, generate a new one
        if not three_letter_code:
            three_letter_code = ''.join(random.choices(string.ascii_uppercase, k=3))
        
        # Create new AID: replace spaces with underscores and add 3-letter code
        new_aid = fullname.replace(' ', '_') + '_' + three_letter_code
        return new_aid
    
    def _initialize_lookup_and_matcher(self) -> None:
        """Initialize lookup tables and name matcher."""
        if self.authors is None:
            print("Cannot initialize lookup tables without author data")
            return
            
        # Create lookup tables
        self._author_lookup = AuthorLookup(self.standardizer)
        
        # Process each author
        for idx, row in self.authors.iterrows():
            self._author_lookup.add_author(
                row['Aid'],
                row['std_fullname'],
                row['fullname'],
                row.get('orcid'),
                idx
            )
            
            # Add alternative names
            alt_names = row.get('alternative_names')
            if isinstance(alt_names, list):
                for alt_name in alt_names:
                    if isinstance(alt_name, str):
                        self._author_lookup.add_alternative_name(row['Aid'], alt_name)
        
        # Initialize name matcher
        self.name_matcher = NameMatcher(self._author_lookup, self.authors, self.standardizer)
    
    def legacy_import_alternative_names(self, source: Union[str, Dict], is_csv: bool = True) -> int:
        """
        Import alternative names from a CSV file or dictionary.
        
        Args:
            source: Path to CSV file or dictionary mapping Aid to alternative names
            is_csv: Whether source is a CSV file (True) or dictionary (False)
            
        Returns:
            Number of alternative names added
        """
        if self.authors is None:
            print("No authors loaded. Please load authors first.")
            return 0
        
        # Ensure name matcher is initialized
        if self.name_matcher is None:
            self._initialize_lookup_and_matcher()
        
        # Ensure alternative_names column exists
        if 'alternative_names' not in self.authors.columns:
            self.authors['alternative_names'] = None
        
        # Initialize counter
        added_count = 0
        
        # Process CSV file
        if is_csv:
            try:
                alt_names_df = pd.read_csv(source)
                
                # Check required columns
                if 'Aid' not in alt_names_df.columns or 'alternative_name' not in alt_names_df.columns:
                    print("CSV must contain 'Aid' and 'alternative_name' columns.")
                    return 0
                
                # Process each row
                for _, row in alt_names_df.iterrows():
                    aid = row['Aid']
                    alt_name = row['alternative_name']
                    
                    # Find author with this Aid
                    author_idx = self.authors.index[self.authors['Aid'] == aid].tolist()
                    
                    if author_idx:
                        idx = author_idx[0]
                        if self.name_matcher.add_alternative_name(idx, alt_name):
                            added_count += 1
                    else:
                        print(f"Warning: No author found with Aid {aid}")
                
            except Exception as e:
                print(f"Error loading alternative names from CSV: {e}")
                return 0
        
        # Process dictionary
        else:
            try:
                for aid, alt_name in source.items():
                    # Find author with this Aid
                    author_idx = self.authors.index[self.authors['Aid'] == aid].tolist()
                    
                    if author_idx:
                        idx = author_idx[0]
                        
                        # Handle case where alt_name is a list
                        if isinstance(alt_name, list):
                            for name in alt_name:
                                if self.name_matcher.add_alternative_name(idx, name):
                                    added_count += 1
                        # Handle case where alt_name is a string
                        else:
                            if self.name_matcher.add_alternative_name(idx, alt_name):
                                added_count += 1
                    else:
                        print(f"Warning: No author found with Aid {aid}")
            
            except Exception as e:
                print(f"Error loading alternative names from dictionary: {e}")
                return 0
        
        print(f"Added {added_count} alternative names to authors.")
        return added_count
    
    def legacy_load_orcid_articles(self, filepath: str) -> None:
        """
        Load articles from ORCID data with improved author matching.
        
        Args:
            filepath: Path to the CSV file containing ORCID article data
        """
        # Initialize a progress iterator
        progress = self.tqdm
        
        # Initialize name matching cache for optimization
        name_cache = {}
        
        # Load the CSV file
        df = pd.read_csv(filepath)
        
        # Remove 'Unnamed' columns
        unnamed_cols = [col for col in df.columns if 'Unnamed' in col]
        df = df.drop(columns=unnamed_cols)
        
        # Clean ORCID and DOI formats using vectorized operations
        df['orcid'] = df['orcid'].str.replace('https://orcid.org/', '', regex=False)
        df['doi'] = df['doi'].str.replace('https://doi.org/', '', regex=False)
        
        # Process to handle duplicate articles
        article_dict = {}
        
        print(f"Processing {len(df)} ORCID articles")
        
        for _, row in progress(df.iterrows(), desc="Processing ORCID articles"):
            article_id = row['Article_ID']
            author_name = row['name']
            author_orcid = row['orcid']
            
            if article_id not in article_dict:
                # Create a new entry for this article with all required fields
                article_data = {col: None for col in self.ARTICLE_COLUMNS}
                # Update with available data from row
                for col in row.index:
                    if col in article_data:
                        article_data[col] = row[col]
                
                # Initialize authors list
                article_data['authors'] = []
                article_dict[article_id] = article_data
            
            # Add author info as tuple (name, orcid) to avoid duplicates
            author_info = (author_name, author_orcid)
            if author_info not in article_dict[article_id]['authors']:
                article_dict[article_id]['authors'].append(author_info)
        
        # Convert to DataFrame
        self.articles = pd.DataFrame(list(article_dict.values()))
        
        # Ensure all required columns exist
        for col in self.ARTICLE_COLUMNS:
            if col not in self.articles.columns:
                self.articles[col] = None
        
        # Map authors to Aid if authors table is loaded
        if self.authors is not None and self.name_matcher is not None:
            print("Mapping authors to Aid...")
            
            for i, row in progress(self.articles.iterrows(), desc="Mapping authors"):
                author_infos = row['authors']
                author_aids = []
                
                for name, orcid in author_infos:
                    # Use name_matcher to find the author
                    author_aid, _, _ = self.name_matcher.match_name(name, orcid, name_cache)
                    
                    if author_aid is not None:
                        # Found a match in authors table
                        author_aids.append(author_aid)
                    else:
                        # No match found, fallback to name
                        author_aids.append(name)
                
                # Remove duplicates while preserving order
                seen = set()
                unique_author_aids = []
                for aid in author_aids:
                    if aid not in seen:
                        seen.add(aid)
                        unique_author_aids.append(aid)
                
                self.articles.at[i, 'authors'] = unique_author_aids
        
        print(f"Processed {len(self.articles)} ORCID articles with improved author matching.")
    
    def compare_name(self, name: str, orcid: Optional[str] = None, 
                     name_cache: Optional[Dict] = None) -> Tuple[Optional[str], Optional[str], bool]:
        """
        Compare a name (and optional ORCID) against authors to find a match.
        
        Args:
            name: Author name to check
            orcid: Author's ORCID if available
            name_cache: Cache of previous match results
            
        Returns:
            Tuple of (aid, db_name, is_alternative)
        """
        if self.authors is None or len(self.authors) == 0:
            return None, None, False
        
        # Ensure name matcher is initialized
        if self.name_matcher is None:
            self._initialize_lookup_and_matcher()
        
        # Use matcher to find the name
        return self.name_matcher.match_name(name, orcid, name_cache)

    def update_files(self, directory: str) -> None:
        """
        Update file associations for articles using tracking information.
        
        Args:
            directory: Path to directory containing article files
        """
        # Get all files in directory and subdirectories
        all_files = []
        for root, _, files in os.walk(directory):
            for file in files:
                all_files.append(os.path.join(root, file))
        
        print(f"Found {len(all_files)} files in {directory}")
        
        # Create files list for processing
        self.files = pd.DataFrame({'path': all_files})
        self.files['filename'] = self.files['path'].apply(lambda x: os.path.splitext(os.path.basename(x))[0])
        
        # Check if articles are loaded
        if self.articles is None:
            print("No articles loaded. Cannot update file associations.")
            return
            
        # Check if file_name column exists, if not create it
        if 'file_name' not in self.articles.columns:
            self.articles['file_name'] = None
        
        # Create dictionary mapping article IDs to file paths
        article_files_dict = {}
        
        # Create a lookup dictionary for faster matching
        file_lookup = {filename: path for filename, path in 
                    zip(self.files['filename'], self.files['path'])}
        
        # Track which files are matched to articles
        matched_files = set()
        
        # Process articles more efficiently
        articles_without_files = []
        
        for i, article in self.articles.iterrows():
            article_id = article['Article_ID']
            file_name = article['file_name']
            article_files = []
            
            # First check if there's an existing file_name
            if pd.notna(file_name) and file_name in file_lookup:
                # Found a file using existing file_name
                article_files.append(file_lookup[file_name])
                matched_files.add(file_name)
                
            # Check for current Article_ID
            if article_id in file_lookup:
                article_files.append(file_lookup[article_id])
                matched_files.add(article_id)
                
            # Also check all IDs in the tracking column
            if 'tracking' in self.articles.columns:
                tracking = article['tracking']
                if isinstance(tracking, list):
                    for tracked_id in tracking:
                        if tracked_id in file_lookup and tracked_id not in matched_files:
                            article_files.append(file_lookup[tracked_id])
                            matched_files.add(tracked_id)
            
            # Update article_files_dict
            if article_files:
                article_files_dict[article_id] = article_files
                
                # Update file_name to first found file's filename
                first_file_name = os.path.splitext(os.path.basename(article_files[0]))[0]
                self.articles.at[i, 'file_name'] = first_file_name
            else:
                # No file found for this article
                articles_without_files.append(article.to_dict())
        
        # Find files without corresponding articles
        files_without_articles = []
        for filename, path in file_lookup.items():
            if filename not in matched_files:
                files_without_articles.append({
                    'filename': filename,
                    'path': path
                })
        
        # Report findings
        print(f"Found files for {len(article_files_dict)} articles.")
        print(f"{len(articles_without_files)} articles have no matching files.")
        print(f"{len(files_without_articles)} files have no corresponding articles.")
        
        # Save missing articles to CSV
        if articles_without_files:
            missing_df = pd.DataFrame(articles_without_files)
            missing_csv_path = os.path.join(directory, 'articles_without_files.csv')
            missing_df.to_csv(missing_csv_path, index=False)
            print(f"List of articles without files saved to {missing_csv_path}")
        
        # Save unmatched files to CSV
        if files_without_articles:
            unmatched_df = pd.DataFrame(files_without_articles)
            unmatched_csv_path = os.path.join(directory, 'files_without_articles.csv')
            unmatched_df.to_csv(unmatched_csv_path, index=False)
            print(f"List of files without corresponding articles saved to {unmatched_csv_path}")

    def legacy_load_pbn_articles(self, filepath: str) -> None:
        """
        Load articles from PBN data and add them to the self.articles dataset.
        Uses direct ID matching to identify authors, avoiding name-based matching.
        Optimized for performance.
        
        Args:
            filepath: Path to the CSV file containing PBN article data
        """
        # Load the CSV file
        df = pd.read_csv(filepath)
        
        # Filter to only include articles
        df = df[df['type'] == 'ARTICLE']
        
        print(f"Processing {len(df)} PBN articles")
        
        # Create a lookup for matching author IDs to AIDs (to avoid repeated lookups)
        author_id_to_aid = {}
        if self.authors is not None:
            # Build a mapping of id -> Aid for faster lookups
            id_aid_map = self.authors[['id', 'Aid']].dropna(subset=['id']).set_index('id')['Aid'].to_dict()
            author_id_to_aid = id_aid_map
        
        # Build lookup dictionaries for faster article existence checking
        article_id_dict = {}
        doi_dict = {}
        if self.articles is not None and len(self.articles) > 0:
            # Create dictionary for Article_ID lookup
            for idx, article_id in enumerate(self.articles['Article_ID']):
                article_id_dict[article_id] = idx
                
            # Create dictionary for DOI lookup
            for idx, doi in enumerate(self.articles['doi']):
                if pd.notna(doi) and doi != '':
                    clean_doi = doi.replace('https://doi.org/', '')
                    doi_dict[clean_doi] = idx
                    doi_dict['https://doi.org/' + clean_doi] = idx
        
        # Track statistics
        added_count = 0
        updated_count = 0
        
        # Process each article - keep the original row-by-row approach as requested
        for _, row in self.tqdm(df.iterrows(), desc="Processing PBN articles"):
            doi_transformed = row.get('doi_transformed')
            if pd.isna(doi_transformed):
                continue  # Skip entries without a transformed DOI
            
            # Get author ID from PBN data
            author_id = row['our_id']
            
            # Find the corresponding Aid using the lookup dictionary (much faster than DataFrame filtering)
            aid = author_id_to_aid.get(author_id, author_id)
            
            # Prepare new article data with all required columns
            article_data = {col: None for col in self.ARTICLE_COLUMNS}
            article_data.update({
                'Article_ID': doi_transformed,
                'title': row['title'],
                'doi': row['doi'],
                'date': row['year'],
                'journal': row['journal'],
                'authors': [aid]
            })
            
            # Check if we have articles loaded and if this article exists
            if self.articles is not None:
                # Look for the article by Article_ID and DOI using dictionaries instead of DataFrame filtering
                existing_idx = None
                
                # Check by Article_ID first (fast dictionary lookup)
                if doi_transformed in article_id_dict:
                    existing_idx = article_id_dict[doi_transformed]
                # If not found, check by DOI
                elif article_data['doi'] and article_data['doi'] in doi_dict:
                    existing_idx = doi_dict[article_data['doi']]
                # If still not found, try clean version of DOI
                elif article_data['doi']:
                    clean_doi = article_data['doi'].replace('https://doi.org/', '')
                    if clean_doi in doi_dict:
                        existing_idx = doi_dict[clean_doi]
                
                if existing_idx is not None:
                    # Article exists, check if this author is already in the list
                    current_authors = self.articles.at[existing_idx, 'authors']
                    if not isinstance(current_authors, list):
                        current_authors = []
                    
                    # Check if this author is already in the list (exact match only)
                    if aid not in current_authors:
                        # Add the author to the list
                        current_authors.append(aid)
                        self.articles.at[existing_idx, 'authors'] = current_authors
                        updated_count += 1
                    continue
                
                # Article doesn't exist, append to existing DataFrame
                self.articles = pd.concat([self.articles, pd.DataFrame([article_data])], ignore_index=True)
                
                # Update our lookup dictionaries with the new article
                new_idx = len(self.articles) - 1
                article_id_dict[doi_transformed] = new_idx
                if article_data['doi'] and pd.notna(article_data['doi']):
                    clean_doi = article_data['doi'].replace('https://doi.org/', '')
                    doi_dict[clean_doi] = new_idx
                    doi_dict['https://doi.org/' + clean_doi] = new_idx
                
                added_count += 1
            else:
                # No articles table yet, create one with this article
                self.articles = pd.DataFrame([article_data])
                
                # Initialize lookup dictionaries
                article_id_dict[doi_transformed] = 0
                if article_data['doi'] and pd.notna(article_data['doi']):
                    clean_doi = article_data['doi'].replace('https://doi.org/', '')
                    doi_dict[clean_doi] = 0
                    doi_dict['https://doi.org/' + clean_doi] = 0
                    
                added_count += 1
        
        # Ensure all required columns exist
        for col in self.ARTICLE_COLUMNS:
            if col not in self.articles.columns:
                self.articles[col] = None
                
        print(f"Added {added_count} new PBN articles and updated {updated_count} existing articles. Total articles: {len(self.articles)}")

    def legacy_load_openalex_articles(self, filepath: str) -> None:
        """
        Load articles from OpenAlex data:
        1. Enhance existing articles with same DOI (current functionality)
        2. Add new articles if their authors exist in the database
        3. Save remaining articles to CSV
        
        Args:
            filepath: Path to the CSV file containing OpenAlex article data
        """
        # Load the CSV file
        df = pd.read_csv(filepath)
        
        # Ensure all required columns exist in the articles DataFrame
        if self.articles is not None:
            for col in self.ARTICLE_COLUMNS:
                if col not in self.articles.columns:
                    self.articles[col] = None
            
            # Make sure OpenAlex specific columns exist in the articles DataFrame
            for col in ['pdf_url', 'landing_page_url', 'oa_url']:
                if col not in self.articles.columns:
                    self.articles[col] = None
        
            # Make sure old_dois column exists
            if 'old_dois' not in self.articles.columns:
                self.articles['old_dois'] = None
                # Initialize old_dois with current DOIs (if they exist)
                for idx in self.articles.index:
                    curr_doi = self.articles.at[idx, 'doi']
                    if curr_doi and pd.notna(curr_doi) and curr_doi != 'empty':
                        clean_doi = str(curr_doi).replace('https://doi.org/', '').lower()
                        self.articles.at[idx, 'old_dois'] = [clean_doi]
                    else:
                        self.articles.at[idx, 'old_dois'] = []
        
        # Initialize name matching cache for optimization
        name_cache = {}
        
        # Step 1: Clean and standardize DOI format in both datasets (case-insensitive)
        df['clean_doi'] = df['doi'].apply(
            lambda x: str(x).replace('https://doi.org/', '').lower() if pd.notna(x) else ''
        )
        
        articles_copy = None
        existing_dois = set()
        all_old_dois = []
        
        if self.articles is not None and len(self.articles) > 0:
            articles_copy = self.articles.copy()
            articles_copy['clean_doi'] = articles_copy['doi'].apply(
                lambda x: str(x).replace('https://doi.org/', '').lower() if pd.notna(x) else ''
            )
            
            # Get existing DOIs
            existing_dois = set(articles_copy['clean_doi'].dropna())
            
            # Collect all old_dois from articles
            for idx, row in articles_copy.iterrows():
                old_dois = row['old_dois']
                if isinstance(old_dois, list) and old_dois:
                    all_old_dois.extend(old_dois)
        
        # Step 2: Identify and enhance articles with the same DOI (case-insensitive)
        print("Step 1: Identifying articles with the same DOI (case-insensitive)")
        openalex_dois = set(df['clean_doi'].dropna())
        
        # Now also check for matches with old_dois
        old_doi_matches = set()
        
        # Find OpenAlex DOIs that match any old DOI
        for old_doi in all_old_dois:
            if old_doi in openalex_dois:
                old_doi_matches.add(old_doi)
        
        # Combine current DOI matches and old DOI matches
        current_doi_matches = openalex_dois.intersection(existing_dois)
        duplicate_dois = {doi for doi in current_doi_matches.union(old_doi_matches) if doi}  # Exclude empty DOIs
        
        print(f"Found {len(duplicate_dois)} articles with matching DOIs in OpenAlex (including {len(old_doi_matches)} matches from historical DOIs).")
        
        # Track which rows have been processed
        processed_rows = set()
        
        # Step 2A: Process each duplicate DOI
        if self.articles is not None and len(self.articles) > 0:
            for doi in self.tqdm(duplicate_dois, desc="Processing matching DOIs"):
                # Get OpenAlex data for this DOI
                openalex_rows = df[df['clean_doi'] == doi]
                
                # Extract unique authors from OpenAlex
                openalex_authors_all = []
                for _, row in openalex_rows.iterrows():
                    processed_rows.add(row.name)  # Mark this row as processed
                    if pd.notna(row['authors']) and isinstance(row['authors'], str):
                        authors = [name.strip() for name in row['authors'].split(',')]
                        openalex_authors_all.extend(authors)
                
                # Extract ORCIDs from OpenAlex
                author_orcids = {}
                if openalex_rows.shape[0] > 0:
                    for _, row in openalex_rows.iterrows():
                        if pd.notna(row['author_orcids']) and isinstance(row['author_orcids'], str):
                            # Split by semicolons to get each author-ORCID pair
                            pairs = row['author_orcids'].split(';')
                            for pair in pairs:
                                pair = pair.strip()
                                if ': https://orcid.org/' in pair:
                                    try:
                                        name, orcid = pair.split(': https://orcid.org/', 1)
                                        author_orcids[name.strip()] = orcid.strip()
                                    except ValueError:
                                        print(f"Warning: Could not parse ORCID pair: {pair}")
                                        continue
                
                # Map OpenAlex authors to AIDs where possible
                openalex_authors_mapped = []
                for author in openalex_authors_all:
                    author_orcid = author_orcids.get(author)
                    author_aid, _, _ = self.compare_name(author, author_orcid, name_cache)
                    
                    # Use the Aid if found, otherwise original name
                    openalex_authors_mapped.append((author_aid, author) if author_aid is not None else (author, None))
                
                # Find matching articles - now including those with this DOI in their old_dois
                matching_indices = []
                
                # First, add articles with matching current DOI
                current_doi_indices = articles_copy[articles_copy['clean_doi'] == doi].index.tolist()
                matching_indices.extend(current_doi_indices)
                
                # Then, add articles with this DOI in their old_dois
                for idx, row in articles_copy.iterrows():
                    old_dois = row['old_dois']
                    if isinstance(old_dois, list) and doi in old_dois:
                        if idx not in matching_indices:
                            matching_indices.append(idx)
                
                if matching_indices and openalex_rows.shape[0] > 0:
                    for idx in matching_indices:
                        openalex_row = openalex_rows.iloc[0]
                        
                        # Update OpenAlex specific fields
                        for col in ['pdf_url', 'landing_page_url', 'oa_url']:
                            if col in openalex_row and pd.notna(openalex_row[col]):
                                self.articles.at[idx, col] = openalex_row[col]
                        
                        # Update authors - add any missing authors
                        current_authors = self.articles.at[idx, 'authors']
                        if not isinstance(current_authors, list):
                            current_authors = []
                        
                        # Find authors in OpenAlex but not in article - only add if they're in self.authors
                        for auth_aid, _ in openalex_authors_mapped:
                            if isinstance(auth_aid, (int, str)) and auth_aid not in current_authors:
                                # Only include if this is an Aid from self.authors
                                if self.authors is not None and auth_aid in self.authors['Aid'].values:
                                    current_authors.append(auth_aid)
                        
                        self.articles.at[idx, 'authors'] = current_authors
                        
                        # Update old_dois if the current DOI is not already there
                        old_dois = self.articles.at[idx, 'old_dois']
                        if not isinstance(old_dois, list):
                            old_dois = []
                        
                        if doi not in old_dois:
                            old_dois.append(doi)
                            self.articles.at[idx, 'old_dois'] = old_dois
        
        # Step 3: Filter remaining articles to find those with authors in our database
        print("\nStep 2: Processing remaining articles to find those with authors in our database")
        
        # Get rows not yet processed
        remaining_df = df.iloc[[i for i in range(len(df)) if i not in processed_rows]]
        print(f"Remaining articles to check: {len(remaining_df)}")
        
        # Create sets of articles to add and to save
        articles_to_add = []
        articles_to_save = []
        
        # Process each remaining article
        for _, row in self.tqdm(remaining_df.iterrows(), desc="Checking remaining articles"):
            # Extract authors and ORCIDs
            author_names = []
            if pd.notna(row['authors']) and isinstance(row['authors'], str):
                author_names = [name.strip() for name in row['authors'].split(',')]
            
            # Extract author ORCIDs
            author_orcids = {}
            if pd.notna(row['author_orcids']) and isinstance(row['author_orcids'], str):
                pairs = row['author_orcids'].split(';')
                for pair in pairs:
                    pair = pair.strip()
                    if ': https://orcid.org/' in pair:
                        try:
                            name, orcid = pair.split(': https://orcid.org/', 1)
                            author_orcids[name.strip()] = orcid.strip()
                        except ValueError:
                            continue
            
            # Check if any author is in our database
            matched_authors = []
            for author in author_names:
                author_orcid = author_orcids.get(author)
                author_aid, _, _ = self.compare_name(author, author_orcid, name_cache)
                
                if author_aid is not None:
                    # Author is in our database
                    matched_authors.append(author_aid)
            
            # If we found at least one author, add this article
            if len(matched_authors) > 0:
                # Create a new article record
                article_data = {col: None for col in self.ARTICLE_COLUMNS}
                
                # Generate Article_ID
                doi = row['doi'] if pd.notna(row['doi']) else None
                title = row['title'] if pd.notna(row['title']) else None
                article_id = self.create_article_id(doi=doi, title=title)
                
                # Add basic fields
                article_data.update({
                    'Article_ID': article_id,
                    'doi': doi,
                    'title': title,
                    'date': row.get('publication_year'),
                    'journal': row.get('journal'),
                    'authors': matched_authors,
                    'pdf_url': row.get('pdf_url'),
                    'landing_page_url': row.get('landing_page_url'),
                    'oa_url': row.get('oa_url')
                })
                
                # Set up old_dois
                if doi and pd.notna(doi):
                    clean_doi = str(doi).replace('https://doi.org/', '').lower()
                    article_data['old_dois'] = [clean_doi]
                else:
                    article_data['old_dois'] = []
                
                articles_to_add.append(article_data)
            else:
                # No author match, save for later
                articles_to_save.append(row.to_dict())
        
        # Step 4: Add articles with matched authors
        print(f"\nAdding {len(articles_to_add)} new articles with matched authors")
        if articles_to_add:
            if self.articles is not None:
                self.articles = pd.concat([self.articles, pd.DataFrame(articles_to_add)], ignore_index=True)
            else:
                self.articles = pd.DataFrame(articles_to_add)
        
        # Step 5: Save remaining articles to CSV
        if articles_to_save:
            output_path = os.path.splitext(filepath)[0] + '_unmatched.csv'
            pd.DataFrame(articles_to_save).to_csv(output_path, index=False)
            print(f"Saved {len(articles_to_save)} unmatched articles to {output_path}")
        
        print(f"\nProcessing complete. Total articles in database: {len(self.articles) if self.articles is not None else 0}")

    def create_article_id(self, article=None, doi=None, title=None):
        """
        Create an article ID from an article record, DOI, or title.
        
        Args:
            article (dict or pd.Series, optional): Article record containing 'doi' and/or 'title'
            doi (str, optional): DOI string in any format
            title (str, optional): Article title
            
        Returns:
            str: An article ID based on DOI (if available) or first 20 chars of title + 3 random chars
        """
        import random
        import string
        import re
        
        # Extract DOI and title from article if provided
        if article is not None:
            if doi is None and 'doi' in article:
                doi = article['doi']
            if title is None and 'title' in article:
                title = article['title']
        
        # Use DOI if available and not the string "empty"
        if doi and pd.notna(doi) and doi != "empty":
            # Extract the DOI part after "org/" if it exists
            if isinstance(doi, str) and "org/" in doi:
                doi_part = doi.split("org/")[-1]
            else:
                doi_part = str(doi)
                
            # Convert to lowercase and replace characters
            doi_part = doi_part.lower()
            doi_part = doi_part.replace('.', '_')
            doi_part = doi_part.replace('/', '&')
            
            return f"{doi_part}"
        
        # Use title if DOI is not available or is "empty"
        elif title and pd.notna(title):
            # Convert title to lowercase and remove punctuation
            title_clean = str(title).lower()
            title_clean = re.sub(r'[^\w\s]', '', title_clean)  # Remove punctuation
            title_clean = re.sub(r'\s+', '_', title_clean)     # Replace spaces with underscores
            
            # Take first 20 characters (or less if title is shorter)
            title_prefix = title_clean[:20]
            
            # Generate 3 random characters
            random_suffix = ''.join(random.choices(string.ascii_uppercase, k=3))
            
            return f"title_{title_prefix}_{random_suffix}"
        
        # Return a default ID if neither DOI nor title is available
        else:
            import uuid
            return f"unknown_{str(uuid.uuid4())[:8]}"
    
    def identify_doi_duplicates(self):
        """
        Identify articles with the same DOI.
        
        Returns:
            list: Groups of duplicate articles by DOI
        """
        if self.articles is None or len(self.articles) == 0:
            return []
            
        # Create clean DOIs for comparison (lowercase, no prefix)
        self.articles['clean_doi'] = self.articles['doi'].apply(
            lambda x: str(x).replace('https://doi.org/', '').lower() if pd.notna(x) and x != 'empty' else ''
        )
        
        # Group by clean DOI
        doi_groups = {}
        for idx, row in self.articles.iterrows():
            clean_doi = row['clean_doi'] 
            if isinstance(clean_doi, str) and clean_doi.strip():  # Check if it's a non-empty string
                if clean_doi not in doi_groups:
                    doi_groups[clean_doi] = []
                doi_groups[clean_doi].append(idx)
        
        # Return groups with multiple articles
        return [(doi, indices) for doi, indices in doi_groups.items() if len(indices) > 1]

    def identify_similar_titles(self, max_pairs=20, word_n_gram_size=4, min_shared_ngrams=1):
        """
        Identify articles with similar titles using only word-based n-grams.
        No Levenshtein distance calculation - much faster.
        
        Args:
            max_pairs: Maximum number of pairs to return
            word_n_gram_size: Size of word n-grams for matching (4-5 words recommended)
            min_shared_ngrams: Minimum number of word n-grams titles must share
                
        Returns:
            list: Pairs of articles that share word n-grams
        """
        if self.articles is None or len(self.articles) == 0:
            return []
        
        import re
        
        # Ensure we have standardized titles
        if 'std_title' not in self.articles.columns:
            self.articles['std_title'] = self.articles['title'].apply(
                lambda x: str(x).lower().translate(str.maketrans('', '', string.punctuation)) if pd.notna(x) else ''
            )
        
        # Get valid titles and indices
        valid_titles = []
        for idx, row in self.articles.iterrows():
            title = row['std_title']
            if isinstance(title, str) and title.strip() and len(title) >= 5:
                valid_titles.append((idx, title))
        
        print(f"Processing {len(valid_titles)} valid titles for similarity")
        
        # ---- Create word-based n-grams ----
        title_word_ngrams = {}
        
        # Function to extract word n-grams from text
        def get_word_ngrams(text, n):
            words = re.findall(r'\b\w+\b', text)
            if len(words) < n:
                return set([" ".join(words)])  # If fewer words than n, use the whole title
            return set([" ".join(words[i:i+n]) for i in range(len(words)-n+1)])
        
        # Add tqdm progress tracking for title processing
        with self.tqdm(total=len(valid_titles), desc="Processing titles") as pbar:
            for idx, title in valid_titles:
                # Create word-based n-grams
                word_ngrams = get_word_ngrams(title, word_n_gram_size)
                title_word_ngrams[idx] = word_ngrams
                pbar.update(1)
        
        # ---- Find candidate pairs using word n-grams ----
        word_ngram_to_titles = {}
        
        # Map word n-grams to titles that contain them
        for idx, ngrams in title_word_ngrams.items():
            for ngram in ngrams:
                if ngram not in word_ngram_to_titles:
                    word_ngram_to_titles[ngram] = []
                word_ngram_to_titles[ngram].append(idx)
        
        # Remove extremely common word n-grams (those in many titles)
        MAX_TITLES_PER_WORD_NGRAM = 30
        filtered_word_ngram_to_titles = {
            ngram: titles for ngram, titles in word_ngram_to_titles.items() 
            if len(titles) <= MAX_TITLES_PER_WORD_NGRAM
        }
        
        # Find and count how many n-grams each pair shares
        shared_ngram_counts = {}
        
        for titles in filtered_word_ngram_to_titles.values():
            if len(titles) > 1:
                for i in range(len(titles)):
                    for j in range(i+1, len(titles)):
                        idx1, idx2 = min(titles[i], titles[j]), max(titles[i], titles[j])
                        pair = (idx1, idx2)
                        
                        if pair not in shared_ngram_counts:
                            shared_ngram_counts[pair] = 0
                        shared_ngram_counts[pair] += 1
        
        # Filter to pairs that share at least min_shared_ngrams
        similar_pairs = [(count, idx1, idx2) for (idx1, idx2), count in shared_ngram_counts.items() 
                        if count >= min_shared_ngrams]
        
        # Sort by number of shared n-grams (descending)
        similar_pairs.sort(reverse=True)
        
        print(f"Found {len(similar_pairs)} title pairs that share at least {min_shared_ngrams} word n-gram(s)")
        
        # Return top pairs
        return similar_pairs[:max_pairs]

    def ensure_years_only(self):
        """
        Ensure all dates are saved as year only.
        
        Converts various date formats to year (integer) format.
        """
        if self.articles is None or len(self.articles) == 0:
            return
        
        if 'date' not in self.articles.columns:
            return
        
        # Regular expression to extract years
        year_pattern = re.compile(r'\b(19|20)\d{2}\b')
        
        # Process each date
        for idx, date in enumerate(self.articles['date']):
            if pd.isna(date):
                continue
                
            # If already an integer, make sure it's a valid year
            if isinstance(date, int):
                if 1900 <= date <= 2100:
                    continue
                else:
                    self.articles.at[idx, 'date'] = None
                    continue
                    
            # If it's a string, try to extract year
            if isinstance(date, str):
                # If it's already just a year as string
                if date.isdigit() and len(date) == 4 and 1900 <= int(date) <= 2100:
                    self.articles.at[idx, 'date'] = int(date)
                    continue
                    
                # Try to extract year using regex
                year_match = year_pattern.search(date)
                if year_match:
                    self.articles.at[idx, 'date'] = int(year_match.group())
                else:
                    self.articles.at[idx, 'date'] = None
                    
            # Other types or formats
            else:
                try:
                    # Try to extract year from various date formats
                    date_str = str(date)
                    year_match = year_pattern.search(date_str)
                    if year_match:
                        self.articles.at[idx, 'date'] = int(year_match.group())
                    else:
                        self.articles.at[idx, 'date'] = None
                except:
                    self.articles.at[idx, 'date'] = None

    def merge_articles(self, article_ids):
        """
        Merge multiple articles, preserving all unique information.
        
        Args:
            article_ids: List of Article_IDs to merge
                
        Returns:
            tuple: (bool, main_id) - Success flag and the ID of the main article after merging
        """
        if self.articles is None or len(self.articles) == 0:
            return False, None
            
        if not article_ids or len(article_ids) <= 1:
            return False, article_ids[0] if article_ids else None
            
        # Find the indices corresponding to the article IDs
        id_to_idx = {}
        for idx, row in self.articles.iterrows():
            if row['Article_ID'] in article_ids:
                id_to_idx[row['Article_ID']] = idx
        
        # Check if we found all the articles
        if len(id_to_idx) != len(article_ids):
            missing_ids = set(article_ids) - set(id_to_idx.keys())
            print(f"Warning: Could not find indices for {missing_ids} article IDs")
            return False, None
            
        # Determine main article (prefer one with DOI)
        main_id = None
        main_idx = None
        
        # First try to find an article with DOI
        for article_id, idx in id_to_idx.items():
            doi = self.articles.loc[idx]['doi']
            if doi is not None and not (isinstance(doi, float) and pd.isna(doi)) and doi != '' and doi != 'empty':
                main_id = article_id
                main_idx = idx
                break
                
        # If no article with DOI, use the first one
        if main_id is None:
            main_id = article_ids[0]
            main_idx = id_to_idx[main_id]
        
        # Ensure tracking column exists
        if 'tracking' not in self.articles.columns:
            self.articles['tracking'] = None
            
        # Ensure old_dois column exists
        if 'old_dois' not in self.articles.columns:
            self.articles['old_dois'] = None
            
        # Initialize tracking for main article
        main_article = self.articles.iloc[main_idx]
        main_tracking_val = main_article['tracking']
        if main_tracking_val is None or (isinstance(main_tracking_val, float) and pd.isna(main_tracking_val)):
            main_tracking = [main_article['Article_ID']]
        elif isinstance(main_tracking_val, list):
            main_tracking = main_tracking_val
        else:
            # Try to convert from string representation
            try:
                main_tracking = eval(str(main_tracking_val))
                if not isinstance(main_tracking, list):
                    main_tracking = [main_article['Article_ID']]
            except:
                main_tracking = [main_article['Article_ID']]
        
        # Initialize old_dois for main article
        main_old_dois_val = main_article['old_dois']
        if main_old_dois_val is None or (isinstance(main_old_dois_val, float) and pd.isna(main_old_dois_val)):
            main_old_dois = []
            # Add current DOI to old_dois if available
            main_doi = main_article['doi']
            if main_doi and pd.notna(main_doi) and main_doi != 'empty':
                clean_doi = str(main_doi).replace('https://doi.org/', '').lower()
                main_old_dois.append(clean_doi)
        elif isinstance(main_old_dois_val, list):
            main_old_dois = main_old_dois_val
        else:
            # Try to convert from string representation
            try:
                main_old_dois = eval(str(main_old_dois_val))
                if not isinstance(main_old_dois, list):
                    main_old_dois = []
            except:
                main_old_dois = []
        
        # Initialize authors list
        main_authors = main_article['authors']
        if not isinstance(main_authors, list):
            main_authors = []
        
        # Track merged articles to return
        merged_articles = []
        
        # Merge each other article into the main article
        for article_id, idx in id_to_idx.items():
            if article_id == main_id:
                continue
                
            other_article = self.articles.iloc[idx]
            
            # Update tracking for other article
            other_tracking_val = other_article['tracking']
            if other_tracking_val is None or (isinstance(other_tracking_val, float) and pd.isna(other_tracking_val)):
                other_tracking = [other_article['Article_ID']]
            elif isinstance(other_tracking_val, list):
                other_tracking = other_tracking_val
            else:
                # Try to convert from string representation
                try:
                    other_tracking = eval(str(other_tracking_val))
                    if not isinstance(other_tracking, list):
                        other_tracking = [other_article['Article_ID']]
                except:
                    other_tracking = [other_article['Article_ID']]
            
            # Update old_dois for other article
            other_old_dois_val = other_article['old_dois']
            if other_old_dois_val is None or (isinstance(other_old_dois_val, float) and pd.isna(other_old_dois_val)):
                other_old_dois = []
                # Add current DOI to old_dois if available
                other_doi = other_article['doi']
                if other_doi and pd.notna(other_doi) and other_doi != 'empty':
                    clean_doi = str(other_doi).replace('https://doi.org/', '').lower()
                    other_old_dois.append(clean_doi)
            elif isinstance(other_old_dois_val, list):
                other_old_dois = other_old_dois_val
            else:
                # Try to convert from string representation
                try:
                    other_old_dois = eval(str(other_old_dois_val))
                    if not isinstance(other_old_dois, list):
                        other_old_dois = []
                except:
                    other_old_dois = []
            
            # Merge tracking lists (remove duplicates)
            main_tracking = list(set(main_tracking + other_tracking))
            
            # Merge old_dois lists (remove duplicates)
            main_old_dois = list(set(main_old_dois + other_old_dois))
            
            # Merge authors
            other_authors = other_article['authors']
            if not isinstance(other_authors, list):
                other_authors = []
                
            # Add unique authors to main_authors
            for author in other_authors:
                if author not in main_authors:
                    main_authors.append(author)
            
            # Handle DOI - preserve if main doesn't have one but other does
            main_doi = main_article['doi']
            other_doi = other_article['doi']
            
            if (main_doi is None or (isinstance(main_doi, float) and pd.isna(main_doi)) or main_doi == '' or main_doi == 'empty') and \
            other_doi is not None and not (isinstance(other_doi, float) and pd.isna(other_doi)) and other_doi != '' and other_doi != 'empty':
                self.articles.at[main_idx, 'doi'] = other_doi
                
                # Add new current DOI to old_dois
                clean_doi = str(other_doi).replace('https://doi.org/', '').lower()
                if clean_doi not in main_old_dois:
                    main_old_dois.append(clean_doi)
                    
                # Update main_article to reflect new DOI
                main_article = self.articles.iloc[main_idx]
            # Handle dates
            main_date = main_article['date']
            other_date = other_article['date']
            
            # If main date is missing but other date exists, use other date (converted to year)
            if (main_date is None or (isinstance(main_date, float) and pd.isna(main_date))) and \
            other_date is not None and not (isinstance(other_date, float) and pd.isna(other_date)):
                # Extract year from other_date
                try:
                    # If it's already a year (integer or 4-digit string)
                    if isinstance(other_date, int) or (isinstance(other_date, str) and other_date.isdigit() and len(other_date) == 4):
                        self.articles.at[main_idx, 'date'] = int(other_date)
                    else:
                        # Try to extract year from string date
                        year_match = re.search(r'\b(19|20)\d{2}\b', str(other_date))
                        if year_match:
                            self.articles.at[main_idx, 'date'] = int(year_match.group())
                except:
                    # If anything fails, just keep the original date
                    pass
                
                # Update main_article to reflect new date
                main_article = self.articles.iloc[main_idx]
            
            # Handle URLs - preserve if one exists and the other doesn't
            for url_col in ['pdf_url', 'landing_page_url', 'oa_url']:
                main_url = main_article[url_col]
                other_url = other_article[url_col]
                
                if (main_url is None or (isinstance(main_url, float) and pd.isna(main_url))) and \
                other_url is not None and not (isinstance(other_url, float) and pd.isna(other_url)):
                    self.articles.at[main_idx, url_col] = other_url
            
            merged_articles.append(article_id)
        
        # Update tracking and authors for main article
        self.articles.at[main_idx, 'tracking'] = main_tracking
        self.articles.at[main_idx, 'authors'] = main_authors
        self.articles.at[main_idx, 'old_dois'] = main_old_dois
        
        # Generate new Article_ID based on updated information
        updated_article = self.articles.iloc[main_idx]
        new_article_id = self.create_article_id(article=updated_article)
        self.articles.at[main_idx, 'Article_ID'] = new_article_id
        
        return True, new_article_id
         
    def clean(self):
        """
        Identify and remove duplicate articles based on:
        1. Same DOI
        2. Same title
        3. Similar titles
        
        This method tracks both old Article_IDs and old DOIs to help with future merging.
        """
        if self.articles is None or len(self.articles) == 0:
            print("No articles to clean.")
            return
            
        # Ensure all dates are in year-only format
        self.ensure_years_only()
        
        # Make sure we have a tracking column
        if 'tracking' not in self.articles.columns:
            self.articles['tracking'] = None
            # Initialize tracking with current Article_IDs
            for idx in self.articles.index:
                self.articles.at[idx, 'tracking'] = [self.articles.at[idx, 'Article_ID']]
        
        # Add old_dois column if it doesn't exist
        if 'old_dois' not in self.articles.columns:
            self.articles['old_dois'] = None
            # Initialize old_dois with current DOIs (if they exist)
            for idx in self.articles.index:
                curr_doi = self.articles.at[idx, 'doi']
                if curr_doi and pd.notna(curr_doi) and curr_doi != 'empty':
                    clean_doi = str(curr_doi).replace('https://doi.org/', '').lower()
                    self.articles.at[idx, 'old_dois'] = [clean_doi]
                else:
                    self.articles.at[idx, 'old_dois'] = []
        
        # Step 1: Identify and merge articles with the same DOI
        print("\nStep 1: Identifying articles with the same DOI")
        doi_duplicates = self.identify_doi_duplicates()
        print(f"Found {len(doi_duplicates)} groups of articles with the same DOI.")
        
        # Track which articles should be deleted after DOI merging
        articles_to_delete = set()
        doi_merged = []
        
        for doi, indices in doi_duplicates:
            # Get article IDs for this group
            article_ids = [self.articles.loc[idx]['Article_ID'] for idx in indices]
            
            # Before merging, collect all DOIs from this group
            all_dois = []
            for idx in indices:
                curr_doi = self.articles.loc[idx]['doi']
                if curr_doi and pd.notna(curr_doi) and curr_doi != 'empty':
                    clean_doi = str(curr_doi).replace('https://doi.org/', '').lower()
                    all_dois.append(clean_doi)
                
                # Also collect old_dois
                old_dois = self.articles.loc[idx]['old_dois']
                if isinstance(old_dois, list) and old_dois:
                    all_dois.extend(old_dois)
            
            # Remove duplicates
            all_dois = list(set(all_dois))
            
            # Merge them
            success, main_id = self.merge_articles(article_ids)
            
            if success:
                # Find all indices except the one containing main_id
                main_idx = None
                for idx in indices:
                    if self.articles.loc[idx]['Article_ID'] == main_id:
                        main_idx = idx
                        break
                        
                if main_idx is not None:
                    main_title = self.articles.loc[main_idx]['title']
                    
                    # Update old_dois for the main article
                    self.articles.at[main_idx, 'old_dois'] = all_dois
                    
                    # Mark other articles for deletion
                    for idx in indices:
                        if idx != main_idx:
                            other_title = self.articles.loc[idx]['title']
                            articles_to_delete.add(idx)
                            doi_merged.append((main_title, other_title))
        
        # Remove duplicates after DOI merging
        if articles_to_delete:
            self.articles = self.articles.drop(index=list(articles_to_delete))
            self.articles = self.articles.reset_index(drop=True)
            print(f"\nRemoved {len(articles_to_delete)} duplicate articles with same DOI. Remaining: {len(self.articles)}")
        
        # Step 2: Identify and merge articles with the same title
        print("\nStep 2: Identifying articles with the same title")
        title_duplicates, title_conflicts = self.identify_title_duplicates()
        print(f"Found {len(title_duplicates)} groups of articles with the same title (no DOI conflicts).")
        print(f"Found {len(title_conflicts)} groups with title duplicates but DOI conflicts.")
        
        # Print title conflicts with detailed information
        if title_conflicts:
            print("\nArticles with same title but different DOIs (real conflicts):")
            for i, (title, doi_groups) in enumerate(title_conflicts):
                print(f"\n  Conflict #{i+1}: '{title}'")
                
                # Process each DOI group to show detailed article information
                for doi, article_ids in doi_groups:
                    if doi == 'empty':
                        print(f"\n    DOI: empty")
                    else:
                        print(f"\n    DOI: {doi}")
                    
                    # Get indices for these articles
                    article_indices = []
                    for aid in article_ids:
                        indices = self.articles.index[self.articles['Article_ID'] == aid].tolist()
                        if indices:
                            article_indices.append(indices[0])
                    
                    # Print detailed info for each article in this DOI group
                    for idx in article_indices:
                        article = self.articles.loc[idx]
                        
                        # Format article info
                        article_id = article['Article_ID']
                        article_title = article['title']
                        article_doi = article['doi'] if pd.notna(article['doi']) else ""
                        article_authors = ", ".join(str(a) for a in article['authors']) if isinstance(article['authors'], list) else str(article['authors'])
                        
                        print(f"      Article:")
                        print(f"        ID: {article_id}")
                        print(f"        Title: '{article_title}'")
                        print(f"        DOI: {article_doi}")
                        print(f"        Authors: {article_authors}")
        
        # Reset the deletion tracking set for title merging
        articles_to_delete = set()
        title_merged = []
        
        for title, indices in title_duplicates:
            # We no longer need to check for indices already marked for deletion
            # since we've already removed them after the DOI step
            if len(indices) <= 1:
                continue
            
            # Before merging, collect all DOIs from this group
            all_dois = []
            for idx in indices:
                curr_doi = self.articles.loc[idx]['doi']
                if curr_doi and pd.notna(curr_doi) and curr_doi != 'empty':
                    clean_doi = str(curr_doi).replace('https://doi.org/', '').lower()
                    all_dois.append(clean_doi)
                
                # Also collect old_dois
                old_dois = self.articles.loc[idx]['old_dois']
                if isinstance(old_dois, list) and old_dois:
                    all_dois.extend(old_dois)
            
            # Remove duplicates
            all_dois = list(set(all_dois))
            
            # Get article IDs for this group
            article_ids = [self.articles.loc[idx]['Article_ID'] for idx in indices]
            
            # Merge them
            success, main_id = self.merge_articles(article_ids)
            
            if success:
                # Find main index
                main_idx = None
                for idx in indices:
                    if self.articles.loc[idx]['Article_ID'] == main_id:
                        main_idx = idx
                        break
                        
                if main_idx is not None:
                    main_title = self.articles.loc[main_idx]['title']
                    
                    # Update old_dois for the main article
                    self.articles.at[main_idx, 'old_dois'] = all_dois
                    
                    # Mark other articles for deletion
                    for idx in indices:
                        if idx != main_idx:
                            other_title = self.articles.loc[idx]['title']
                            articles_to_delete.add(idx)
                            title_merged.append((main_title, other_title))
        
        # Remove duplicates after title merging
        if articles_to_delete:
            self.articles = self.articles.drop(index=list(articles_to_delete))
            self.articles = self.articles.reset_index(drop=True)
            print(f"\nRemoved {len(articles_to_delete)} duplicate articles with same title. Remaining: {len(self.articles)}")
        
        # Step 3: Identify articles with similar titles
        print("\nStep 3: Identifying articles with similar titles")
        similar_titles = self.identify_similar_titles(max_pairs=20)
        
        # Print similar title pairs - no deletion tracking needed here
        # since we're just reporting similar titles for manual review
        if similar_titles:
            print("\nTop similar title pairs:")
            for i, (shared_ngrams, idx1, idx2) in enumerate(similar_titles):
                article1 = self.articles.loc[idx1]
                article2 = self.articles.loc[idx2]
                
                # Format article 1 info
                article1_id = article1['Article_ID']
                article1_title = article1['title']
                article1_doi = article1['doi'] if pd.notna(article1['doi']) else "empty"
                article1_authors = ", ".join(str(a) for a in article1['authors']) if isinstance(article1['authors'], list) else str(article1['authors'])
                
                # Format article 2 info
                article2_id = article2['Article_ID']
                article2_title = article2['title']
                article2_doi = article2['doi'] if pd.notna(article2['doi']) else "empty"
                article2_authors = ", ".join(str(a) for a in article2['authors']) if isinstance(article2['authors'], list) else str(article2['authors'])
                
                print(f"\n  {i+1}. Shared word sequences: {shared_ngrams}")
                print(f"     Article 1:")
                print(f"       ID: {article1_id}")
                print(f"       Title: '{article1_title}'")
                print(f"       DOI: {article1_doi}")
                print(f"       Authors: {article1_authors}")
                print(f"     Article 2:")
                print(f"       ID: {article2_id}")
                print(f"       Title: '{article2_title}'")
                print(f"       DOI: {article2_doi}")
                print(f"       Authors: {article2_authors}")
        
        # Clean up temporary columns
        if 'clean_doi' in self.articles.columns:
            self.articles = self.articles.drop(columns=['clean_doi'])
        if 'std_title' in self.articles.columns:
            self.articles = self.articles.drop(columns=['std_title'])
        
        print("Cleanup completed.")

    def identify_title_duplicates(self):
        """
        Identify articles with the same title.
        
        Returns:
            list: Groups of duplicate articles by title
            list: Groups with title duplicates but DOI conflicts
        """
        if self.articles is None or len(self.articles) == 0:
            return [], []
            
        # Create standardized titles
        self.articles['std_title'] = self.articles['title'].apply(
            lambda x: str(x).lower().translate(str.maketrans('', '', string.punctuation)) if pd.notna(x) else ''
        )
        
        # Group by standardized title
        title_groups = {}
        for idx, row in self.articles.iterrows():
            std_title = row['std_title']
            if isinstance(std_title, str) and std_title.strip() and len(std_title) >= 5:  # Check for valid title
                if std_title not in title_groups:
                    title_groups[std_title] = []
                title_groups[std_title].append(idx)
        
        # Identify title duplicates
        title_duplicates = [(title, indices) for title, indices in title_groups.items() if len(indices) > 1]
        
        # Process each title group
        title_no_conflicts = []  # Groups that can be safely merged
        title_conflicts = []     # Groups with DOI conflicts
        
        for title, indices in title_duplicates:
            # Group articles by DOI
            doi_groups = {}
            no_doi_indices = []
            
            for idx in indices:
                doi = self.articles.loc[idx]['doi']
                if pd.isna(doi) or doi == '' or doi == 'empty':
                    # Track articles without DOIs separately
                    no_doi_indices.append(idx)
                else:
                    # Group articles by their DOI
                    clean_doi = str(doi).lower().replace('https://doi.org/', '')
                    if clean_doi not in doi_groups:
                        doi_groups[clean_doi] = []
                    doi_groups[clean_doi].append(idx)
            
            # Check if there are DOI conflicts (more than one unique non-empty DOI)
            if len(doi_groups) > 1:
                # We have multiple different DOIs - this is a real conflict
                conflict_info = (
                    self.articles.loc[indices[0]]['title'], 
                    [(doi, [self.articles.loc[idx]['Article_ID'] for idx in group]) 
                    for doi, group in doi_groups.items()]
                )
                
                # Add no-DOI articles as a separate group in the conflict
                if no_doi_indices:
                    conflict_info[1].append(
                        ('empty', [self.articles.loc[idx]['Article_ID'] for idx in no_doi_indices])
                    )
                
                title_conflicts.append(conflict_info)
            
            elif len(doi_groups) == 1:
                # We have exactly one DOI group
                doi = list(doi_groups.keys())[0]
                doi_indices = doi_groups[doi]
                
                if no_doi_indices:
                    # We have one DOI group AND some no-DOI articles
                    # These can all be merged, with the DOI ones taking precedence
                    all_indices = doi_indices + no_doi_indices
                    title_no_conflicts.append((title, all_indices))
                else:
                    # We have just one DOI group with no no-DOI articles
                    title_no_conflicts.append((title, doi_indices))
            
            else:
                # We have only no-DOI articles - these can be merged
                title_no_conflicts.append((title, no_doi_indices))
        
        return title_no_conflicts, title_conflicts

    def save(self, file_path: str) -> None:
        """
        Save the SONaa data to a pickle file.
        
        Args:
            file_path: Path to the pickle file
        """
        import pickle
        
        data = {
            'authors': self.authors,
            'articles': self.articles,
            'files': self.files
        }
        
        with open(file_path, 'wb') as f:
            pickle.dump(data, f)
        
        print(f"Data saved to {file_path}")

    def load(self, file_path: str) -> None:
        """
        Load the SONaa data from a pickle file.
        
        Args:
            file_path: Path to the pickle file
        """
        import pickle
        
        with open(file_path, 'rb') as f:
            data = pickle.load(f)
        
        self.authors = data.get('authors')
        self.articles = data.get('articles')
        self.files = data.get('files')
        
        # Print summary of loaded data
        print(f"Loaded {len(self.authors) if self.authors is not None else 0} authors")
        print(f"Loaded {len(self.articles) if self.articles is not None else 0} articles")
        print(f"Loaded {len(self.files) if self.files is not None else 0} file entries")
        
        # Re-initialize lookup and matcher if authors are loaded
        if self.authors is not None:
            self._initialize_lookup_and_matcher()