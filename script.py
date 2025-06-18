import json
import logging
import argparse

# ijson is used for efficient streaming of large JSON files
try:
    import ijson
except ImportError:
    raise ImportError("Please install ijson (e.g., via pip install ijson) to run this script.")

def detect_json_format(filepath):
    """
    Detects whether the JSON file at filepath is a single JSON array (returns 'array')
    or a JSON objects per line / single object (returns 'ndjson').
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        # Read until first non-whitespace character
        char = f.read(1)
        while char and char.isspace():
            char = f.read(1)
        if not char:
            return None
        if char == '[':
            return 'array'
        elif char == '{':
            return 'ndjson'
        else:
            return None

def main():
    # Parse command-line arguments for file paths
    parser = argparse.ArgumentParser(description="Merge missing abstracts into profiles JSON.")
    parser.add_argument("raw_assets_file", help="Path to raw assets (profiles) JSON file")
    parser.add_argument("abstracts_file", help="Path to publications abstracts JSON file")
    parser.add_argument("output_file", help="Path for output JSON file")
    args = parser.parse_args()

    raw_file = args.raw_assets_file
    abstracts_file = args.abstracts_file
    output_file = args.output_file

    # Configure logging for traceability
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    logging.info("Scanning raw assets file for profiles missing abstracts...")

    # Step 1: Scan raw assets and collect needed IDs/DOIs
    needed_ids = set()
    needed_dois = set()
    raw_format = detect_json_format(raw_file)
    if raw_format == 'array':
        # Stream through raw assets array using ijson
        with open(raw_file, 'rb') as f:
            for profile in ijson.items(f, 'item'):
                # Check if 'abstract' is missing or null
                if profile.get('abstract') is None:
                    pub_id = profile.get('publication_id', {}).get('$oid')
                    if pub_id:
                        needed_ids.add(pub_id)
                    doi = profile.get('publication_DOI')
                    if doi:
                        needed_dois.add(doi.strip().lower())
    elif raw_format == 'ndjson':
        # Each line is a JSON object
        with open(raw_file, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    profile = json.loads(line)
                except json.JSONDecodeError as e:
                    logging.warning(f"Skipping invalid JSON line: {e}")
                    continue
                if profile.get('abstract') is None:
                    pub_id = profile.get('publication_id', {}).get('$oid')
                    if pub_id:
                        needed_ids.add(pub_id)
                    doi = profile.get('publication_DOI')
                    if doi:
                        needed_dois.add(doi.strip().lower())
    else:
        logging.error("Unable to detect JSON format of raw assets file.")
        return

    logging.info(f"Profiles needing abstracts: {len(needed_ids)} by ID, {len(needed_dois)} by DOI")

    # Step 2: Stream abstracts file and match abstracts
    logging.info("Scanning abstracts file for matching publication abstracts...")
    id_to_abstract = {}
    doi_to_abstract = {}
    remaining_ids = set(needed_ids)
    remaining_dois = set(needed_dois)

    big_format = detect_json_format(abstracts_file)
    if big_format == 'array':
        # Stream through large abstracts JSON array
        with open(abstracts_file, 'rb') as f:
            for rec in ijson.items(f, 'item'):
                rec_id = rec.get('_id', {}).get('$oid')
                if rec_id and rec_id in remaining_ids:
                    abstract_text = rec.get('publication_abstract_cleaned')
                    if abstract_text:
                        id_to_abstract[rec_id] = abstract_text
                    remaining_ids.discard(rec_id)
                # Match by DOI if ID not found or not present
                doi = rec.get('publication_doi')
                if doi:
                    doi_key = doi.strip().lower()
                    if doi_key in remaining_dois:
                        abstract_text = rec.get('publication_abstract_cleaned')
                        if abstract_text:
                            doi_to_abstract[doi_key] = abstract_text
                        remaining_dois.discard(doi_key)
                # Break early if all needed found
                if not remaining_ids and not remaining_dois:
                    break
    elif big_format == 'ndjson':
        # Stream through large abstracts JSON lines
        with open(abstracts_file, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                rec_id = rec.get('_id', {}).get('$oid')
                if rec_id and rec_id in remaining_ids:
                    abstract_text = rec.get('publication_abstract_cleaned')
                    if abstract_text:
                        id_to_abstract[rec_id] = abstract_text
                    remaining_ids.discard(rec_id)
                doi = rec.get('publication_doi')
                if doi:
                    doi_key = doi.strip().lower()
                    if doi_key in remaining_dois:
                        abstract_text = rec.get('publication_abstract_cleaned')
                        if abstract_text:
                            doi_to_abstract[doi_key] = abstract_text
                        remaining_dois.discard(doi_key)
                if not remaining_ids and not remaining_dois:
                    break
    else:
        logging.error("Unable to detect JSON format of abstracts file.")
        return

    logging.info(f"Found abstracts for {len(id_to_abstract)} profiles by ID and {len(doi_to_abstract)} by DOI")

    # Step 3: Write output file with updated abstracts
    logging.info("Writing updated profiles to output file...")
    raw_format = detect_json_format(raw_file)  # re-detect for writing logic

    if raw_format == 'array':
        with open(output_file, 'w', encoding='utf-8') as out_f:
            out_f.write("[\n")
            first = True
            # Re-open raw file for reading
            with open(raw_file, 'rb') as f:
                for profile in ijson.items(f, 'item'):
                    # Insert abstract if missing and found
                    if profile.get('abstract') is None:
                        pub_id = profile.get('publication_id', {}).get('$oid')
                        if pub_id and pub_id in id_to_abstract:
                            profile['abstract'] = id_to_abstract[pub_id]
                        else:
                            doi = profile.get('publication_DOI')
                            if doi:
                                doi_key = doi.strip().lower()
                                if doi_key in doi_to_abstract:
                                    profile['abstract'] = doi_to_abstract[doi_key]
                    # Write out profile JSON
                    if not first:
                        out_f.write(",\n")
                    json.dump(profile, out_f)
                    first = False
            out_f.write("\n]\n")
    else:  # NDJSON output
        with open(output_file, 'w', encoding='utf-8') as out_f, open(raw_file, 'r', encoding='utf-8') as in_f:
            for line in in_f:
                try:
                    profile = json.loads(line)
                except json.JSONDecodeError:
                    logging.warning("Skipping malformed JSON in raw assets while writing output.")
                    continue
                if profile.get('abstract') is None:
                    pub_id = profile.get('publication_id', {}).get('$oid')
                    if pub_id and pub_id in id_to_abstract:
                        profile['abstract'] = id_to_abstract[pub_id]
                    else:
                        doi = profile.get('publication_DOI')
                        if doi:
                            doi_key = doi.strip().lower()
                            if doi_key in doi_to_abstract:
                                profile['abstract'] = doi_to_abstract[doi_key]
                out_f.write(json.dumps(profile) + "\n")

    logging.info("Done. Updated data saved to '%s'.", output_file)

if __name__ == "__main__":
    main()
