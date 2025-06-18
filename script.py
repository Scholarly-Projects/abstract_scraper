import json
import logging
import argparse
import os

# ijson is used for efficient streaming of large JSON files
try:
    import ijson
except ImportError:
    raise ImportError("Please install ijson (e.g., via pip install ijson) to run this script.")

def detect_json_format(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
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
    parser = argparse.ArgumentParser(description="Merge missing abstracts into profiles JSON.")
    parser.add_argument("raw_assets_file", help="Path to raw assets (profiles) JSON file")
    parser.add_argument("abstracts_file", help="Path to publications abstracts JSON file")
    parser.add_argument("output_dir", help="Path to output directory")
    args = parser.parse_args()

    raw_file = args.raw_assets_file
    abstracts_file = args.abstracts_file
    output_dir = args.output_dir

    full_output_path = os.path.join(output_dir, "full_merged_entries.json")
    merged_only_output_path = os.path.join(output_dir, "only_merged_entries.json")

    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    logging.info("Scanning raw assets file for profiles missing abstracts...")

    needed_ids = set()
    needed_dois = set()
    raw_format = detect_json_format(raw_file)
    if raw_format == 'array':
        with open(raw_file, 'rb') as f:
            for profile in ijson.items(f, 'item'):
                if profile.get('abstract') is None:
                    pub_id = profile.get('publication_id', {}).get('$oid')
                    if pub_id:
                        needed_ids.add(pub_id)
                    doi = profile.get('publication_DOI')
                    if doi:
                        needed_dois.add(doi.strip().lower())
    elif raw_format == 'ndjson':
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

    logging.info("Scanning abstracts file for matching publication abstracts...")
    id_to_abstract = {}
    doi_to_abstract = {}
    remaining_ids = set(needed_ids)
    remaining_dois = set(needed_dois)

    big_format = detect_json_format(abstracts_file)
    if big_format == 'array':
        with open(abstracts_file, 'rb') as f:
            for rec in ijson.items(f, 'item'):
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
    elif big_format == 'ndjson':
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

    logging.info("Writing updated profiles to output file...")
    full_merged = []
    only_merged = []

    if raw_format == 'array':
        with open(raw_file, 'rb') as f:
            for profile in ijson.items(f, 'item'):
                updated = False
                if profile.get('abstract') is None:
                    pub_id = profile.get('publication_id', {}).get('$oid')
                    if pub_id and pub_id in id_to_abstract:
                        profile['abstract'] = id_to_abstract[pub_id]
                        updated = True
                    else:
                        doi = profile.get('publication_DOI')
                        if doi:
                            doi_key = doi.strip().lower()
                            if doi_key in doi_to_abstract:
                                profile['abstract'] = doi_to_abstract[doi_key]
                                updated = True
                full_merged.append(profile)
                if updated:
                    only_merged.append(profile)
    else:
        with open(raw_file, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    profile = json.loads(line)
                except json.JSONDecodeError:
                    continue
                updated = False
                if profile.get('abstract') is None:
                    pub_id = profile.get('publication_id', {}).get('$oid')
                    if pub_id and pub_id in id_to_abstract:
                        profile['abstract'] = id_to_abstract[pub_id]
                        updated = True
                    else:
                        doi = profile.get('publication_DOI')
                        if doi:
                            doi_key = doi.strip().lower()
                            if doi_key in doi_to_abstract:
                                profile['abstract'] = doi_to_abstract[doi_key]
                                updated = True
                full_merged.append(profile)
                if updated:
                    only_merged.append(profile)

    with open(full_output_path, 'w', encoding='utf-8') as out_f:
        json.dump(full_merged, out_f, indent=2)

    with open(merged_only_output_path, 'w', encoding='utf-8') as out_f:
        json.dump(only_merged, out_f, indent=2)

    logging.info("Done.")
    logging.info(f"Full dataset written to: {full_output_path}")
    logging.info(f"Merged-only dataset written to: {merged_only_output_path}")

if __name__ == "__main__":
    main()
