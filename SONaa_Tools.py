import pandas as pd
from functools import lru_cache
import re
import unicodedata
import os
import logging
from typing import Dict, List, Tuple, Optional, Union, Any

class NameStandardizer:
    """Class for standardizing author names with efficient caching."""
    
    # Define character constants
    DASH_CHARS = [
        '\u002D', '\u058A', '\u05BE', '\u1400', '\u1806', '\u2010', '\u2011', 
        '\u2012', '\u2013', '\u2014', '\u2015', '\u2027', '\u2043', '\u2053',
        '\u207B', '\u208B', '\u2212', '\u2E17', '\u2E1A', '\u301C', '\u3030',
        '\u30A0', '\uFE58', '\uFE63', '\uFF0D'
    ]
    
    WHITESPACE_CHARS = [
        '\u00A0', '\u1680', '\u2000', '\u2001', '\u2002', '\u2003', '\u2004',
        '\u2005', '\u2006', '\u2007', '\u2008', '\u2009', '\u200A', '\u202F',
        '\u205F', '\u3000'
    ]
    
    def __init__(self, cache_size: int = 1024):
        """
        Initialize the name standardizer with LRU cache.
        
        Args:
            cache_size: Number of names to cache (default: 1024)
        """
        # Create a cached version of the standardize method
        self.standardize = lru_cache(maxsize=cache_size)(self._standardize)
        
    def _standardize(self, name_str: str, to_lower: bool = True) -> str:
        """
        Standardize a name string for consistent comparison.
        
        Args:
            name_str: Name to standardize
            to_lower: Whether to convert to lowercase
            
        Returns:
            Standardized name
        """
        if not isinstance(name_str, str):
            return ""
        
        # Replace underscores with spaces
        name_str = name_str.replace('_', ' ')
        
        # Normalize Unicode characters
        name_str = unicodedata.normalize('NFKD', name_str)
        
        # Replace dash characters with standard hyphen
        for dash in self.DASH_CHARS:
            name_str = name_str.replace(dash, '-')
        
        # Remove spaces around hyphens
        name_str = re.sub(r'\s*-\s*', '-', name_str)
        
        # Replace whitespace variants with standard space
        for ws in self.WHITESPACE_CHARS:
            name_str = name_str.replace(ws, ' ')
        
        # Normalize multiple spaces to single space
        name_str = re.sub(r'\s+', ' ', name_str)
        
        # Strip leading/trailing spaces
        name_str = name_str.strip()
        
        # Convert to lowercase if requested
        if to_lower:
            name_str = name_str.lower()
            
        return name_str

class AuthorLookup:
    """Class to manage author lookup data structures."""
    
    def __init__(self, standardizer):
        """
        Initialize lookup tables.
        
        Args:
            standardizer: NameStandardizer instance for name standardization
        """
        self.by_name = {}       # standardized name -> Aid
        self.by_aid = {}        # Aid -> author info
        self.by_orcid = {}      # orcid -> Aid
        self.by_alt_name = {}   # standardized alternative name -> Aid
        self.standardizer = standardizer
        
    def add_author(self, aid, std_name, fullname, orcid, idx):
        """
        Add an author to all lookup tables.
        
        Args:
            aid: Author ID
            std_name: Standardized name
            fullname: Full name
            orcid: ORCID identifier (optional)
            idx: Index in the DataFrame
        """
        self.by_name[std_name] = aid
        self.by_aid[aid] = {
            'fullname': fullname,
            'std_name': std_name,
            'orcid': orcid,
            'idx': idx
        }
        
        if pd.notna(orcid) and orcid:
            self.by_orcid[orcid] = aid
            
    def add_alternative_name(self, aid, alt_name):
        """
        Add an alternative name for an author.
        
        Args:
            aid: Author ID
            alt_name: Alternative name
        """
        if isinstance(alt_name, str):
            std_alt = self.standardizer.standardize(alt_name)
            self.by_alt_name[std_alt] = aid
            
    def lookup_by_name(self, name):
        """
        Lookup author by standardized name.
        
        Args:
            name: Name to lookup
            
        Returns:
            Author ID if found, None otherwise
        """
        std_name = self.standardizer.standardize(name)
        return self.by_name.get(std_name)
    
    def lookup_by_orcid(self, orcid):
        """
        Lookup author by ORCID.
        
        Args:
            orcid: ORCID to lookup
            
        Returns:
            Author ID if found, None otherwise
        """
        return self.by_orcid.get(orcid)
    
    def lookup_by_alt_name(self, name):
        """
        Lookup author by alternative name.
        
        Args:
            name: Alternative name to lookup
            
        Returns:
            Author ID if found, None otherwise
        """
        std_name = self.standardizer.standardize(name)
        return self.by_alt_name.get(std_name)

class NameMatcher:
    """Handles the complex logic of matching author names."""
    
    def __init__(self, author_lookup, authors_df, standardizer):
        """
        Initialize the name matcher.
        
        Args:
            author_lookup: AuthorLookup instance
            authors_df: DataFrame containing author information
            standardizer: NameStandardizer instance
        """
        self.lookup = author_lookup
        self.authors = authors_df
        self.standardizer = standardizer
        self._name_cache = {}  # Cache for previous match results
        
    def extract_name_components(self, name):
        """
        Extract components from a name.
        
        Args:
            name: Name to extract components from
            
        Returns:
            Dictionary with name components
        """
        parts = name.split()
        if len(parts) == 0:
            return {"first": "", "last": "", "middle": []}
        elif len(parts) == 1:
            return {"first": parts[0], "last": "", "middle": []}
        elif len(parts) == 2:
            return {"first": parts[0], "last": parts[1], "middle": []}
        else:
            return {"first": parts[0], "last": parts[-1], "middle": parts[1:-1]}
    
    def extract_complex_name(self, name):
        """
        Extract complex components including hyphenated parts.
        
        Args:
            name: Name to extract components from
            
        Returns:
            Dictionary with detailed name components
        """
        basic = self.extract_name_components(name)
        result = {
            "first": basic["first"],
            "middle": basic["middle"],
            "last": basic["last"],
            "last_parts": [],
            "all_parts": name.split()
        }
        
        # Process hyphenated last name
        if '-' in basic["last"]:
            result["last_parts"] = basic["last"].split('-')
            result["last_parts"].append(basic["last"])
        else:
            result["last_parts"] = [basic["last"]]
        
        # Process potential compound surnames
        if len(basic["middle"]) > 0:
            compound_indicators = ["van", "von", "der", "den", "de", "la", "le", "di", "da"]
            for i, part in enumerate(basic["middle"]):
                if part.lower() in compound_indicators:
                    potential_compound = " ".join(basic["middle"][i:] + [basic["last"]])
                    result["last_parts"].append(potential_compound)
        
        return result
    
    def add_alternative_name(self, author_idx, new_alt_name):
        """
        Add an alternative name to an author record.
        
        Args:
            author_idx: Index of the author in the DataFrame
            new_alt_name: New alternative name to add
            
        Returns:
            True if the name was added, False otherwise
        """
        if not new_alt_name:
            return False
            
        if 'alternative_names' not in self.authors.columns:
            self.authors['alternative_names'] = None
            
        alt_names = self.authors.at[author_idx, 'alternative_names']
        if not isinstance(alt_names, list):
            alt_names = []
            
        if new_alt_name not in alt_names:
            alt_names.append(new_alt_name)
            self.authors.at[author_idx, 'alternative_names'] = alt_names
            
            # Update the lookup table
            aid = self.authors.at[author_idx, 'Aid']
            self.lookup.add_alternative_name(aid, new_alt_name)
            return True
            
        return False
    
    def match_name(self, name, orcid=None, name_cache=None):
        """
        Match a name against the authors database.
        
        Args:
            name: Name to match
            orcid: ORCID identifier (optional)
            name_cache: External cache dictionary (optional) - if provided, uses this instead of internal cache
            
        Returns:
            Tuple of (author_id, db_name, is_alternative)
        """
        # Use external cache if provided, otherwise use internal cache
        cache = name_cache if name_cache is not None else self._name_cache
        
        # Check cache
        cache_key = (name, orcid)
        if cache_key in cache:
            return cache[cache_key]
        
        # Clean inputs
        name = name.strip() if isinstance(name, str) else ""
        orcid = orcid.strip() if isinstance(orcid, str) else None
        
        # Extract potential alternative name in parentheses
        alt_name = None
        if '(' in name and ')' in name:
            main_name = name.split('(')[0].strip()
            alt_name = name[name.find("(")+1:name.find(")")].strip()
            name = main_name
        
        # Standardize the input name
        std_name = self.standardizer.standardize(name)
        
        # Try to match by ORCID first
        if orcid:
            author_aid = self.lookup.lookup_by_orcid(orcid)
            if author_aid:
                author_info = self.lookup.by_aid[author_aid]
                author_name_in_db = author_info['fullname']
                author_idx = author_info['idx']
                
                # If matched by ORCID but name is different, it's an alternative name
                is_alternative = std_name != self.standardizer.standardize(author_name_in_db)
                
                # Add the name as an alternative if it's different
                if is_alternative and name != author_name_in_db:
                    self.add_alternative_name(author_idx, name)
                    
                # Add alternative name if found in parentheses
                if alt_name:
                    self.add_alternative_name(author_idx, alt_name)
                
                # Store in cache
                result = (author_aid, author_name_in_db, is_alternative)
                cache[cache_key] = result
                return result
        
        # Try exact name match
        author_aid = self.lookup.lookup_by_name(std_name)
        if author_aid:
            author_info = self.lookup.by_aid[author_aid]
            author_name_in_db = author_info['fullname']
            author_idx = author_info['idx']
            
            # Add alternative name if found
            if alt_name:
                self.add_alternative_name(author_idx, alt_name)
            
            # Store in cache
            result = (author_aid, author_name_in_db, False)
            cache[cache_key] = result
            return result
        
        # Try alternative name match
        author_aid = self.lookup.lookup_by_alt_name(std_name)
        if author_aid:
            author_info = self.lookup.by_aid[author_aid]
            author_name_in_db = author_info['fullname']
            author_idx = author_info['idx']
            
            # Add additional alternative name if found
            if alt_name:
                self.add_alternative_name(author_idx, alt_name)
            
            # Store in cache
            result = (author_aid, author_name_in_db, True)
            cache[cache_key] = result
            return result
        
        # Component-based matching as last resort
        result = self._match_by_components(name, alt_name)
        cache[cache_key] = result
        return result
    
    def _match_by_components(self, name, alt_name):
        """
        Match a name by components (first name, last name).
        
        Args:
            name: Name to match
            alt_name: Alternative name found in parentheses (optional)
            
        Returns:
            Tuple of (author_id, db_name, is_alternative)
        """
        # Extract name components
        std_name = self.standardizer.standardize(name)
        name_parts = self.extract_complex_name(std_name)
        
        # Only proceed if we have at least some parts
        if not name_parts["first"] and not name_parts["last"]:
            return None, None, False
        
        # Find the best matching author
        best_match = None
        match_score = 0
        
        # Index authors by first letter of last name for efficiency
        last_first_letter = name_parts["last"][0] if name_parts["last"] else None
        
        # Filter candidates if possible
        candidates = self.authors
        if last_first_letter and len(self.authors) > 100:
            # Create a mask for matching last name first letter
            mask = candidates['std_fullname'].apply(
                lambda x: x.split()[-1][0] if x and len(x.split()) > 0 else "") == last_first_letter
            candidates = candidates[mask]
        
        # Score each candidate
        for idx, author_row in candidates.iterrows():
            db_name = author_row['std_fullname']
            db_parts = self.extract_complex_name(db_name)
            
            # Skip if first names don't match at all (optimization)
            if not name_parts["first"] or not db_parts["first"] or name_parts["first"][0] != db_parts["first"][0]:
                continue
            
            # Calculate score
            current_score = self._calculate_match_score(name_parts, db_parts)
            
            # If this is the best match so far, remember it
            if current_score > match_score and current_score >= 50:  # Minimum threshold
                match_score = current_score
                best_match = (author_row['Aid'], author_row['fullname'], idx)
        
        # If we found a good match
        if best_match:
            author_aid, author_name_in_db, author_idx = best_match
            
            # Add the original name as an alternative
            self.add_alternative_name(author_idx, name)
            
            # Add additional alternative name if found
            if alt_name:
                self.add_alternative_name(author_idx, alt_name)
            
            return author_aid, author_name_in_db, True
        
        return None, None, False
    
    def _calculate_match_score(self, name_parts, db_parts):
        """
        Calculate a score for how well two name component sets match.
        
        Args:
            name_parts: Components of the name to match
            db_parts: Components of the database name
            
        Returns:
            Match score (0-100)
        """
        current_score = 0
        
        # First name matching (up to 40 points)
        if name_parts["first"] == db_parts["first"]:
            current_score += 40  # Exact first name match
        elif len(name_parts["first"]) >= 3 and len(db_parts["first"]) >= 3:
            # First letters match
            min_len = min(len(name_parts["first"]), len(db_parts["first"]))
            # 5 points for each matching initial character (up to 20)
            current_score += min(20, 5 * sum(1 for i in range(min_len) 
                                             if name_parts["first"][i] == db_parts["first"][i]))
        
        # Last name matching (up to 60 points)
        if name_parts["last"] == db_parts["last"]:
            current_score += 60  # Exact last name match
        else:
            # Check for partial matches in surnames
            for part in name_parts["last_parts"]:
                if part in db_parts["last_parts"]:
                    current_score += 30  # Found a matching part
                    break
                
                # Check if any part is contained within another
                for db_part in db_parts["last_parts"]:
                    if (len(part) >= 4 and part in db_part) or (len(db_part) >= 4 and db_part in part):
                        current_score += 20  # One name contains the other
                        break
            
            # Special case for compound names
            if (len(name_parts["last"]) >= 4 and name_parts["last"] in db_parts["last"]) or \
               (len(db_parts["last"]) >= 4 and db_parts["last"] in name_parts["last"]):
                current_score += 20
            
            # Check individual parts of hyphenated names
            if '-' in name_parts["last"] and '-' in db_parts["last"]:
                name_hyphen_parts = name_parts["last"].split('-')
                db_hyphen_parts = db_parts["last"].split('-')
                
                # Count matching parts
                matching_parts = sum(1 for n_part in name_hyphen_parts 
                                     if any(n_part == d_part for d_part in db_hyphen_parts))
                current_score += min(30, matching_parts * 15)  # 15 points per matching part, up to 30
        
        return current_score