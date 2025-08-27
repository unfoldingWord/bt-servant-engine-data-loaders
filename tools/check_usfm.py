#!/usr/bin/env python3
"""
check_usfm.py

Scan for .usfm files (recursively) and parse them with usfm-grammar's USFMParser.
Prints any parser errors per file and exits with code 1 if any errors found.

Usage:
  python3 check_usfm.py [--dir DIR] [--pattern GLOB] [--quiet]

Requirements:
  pip install usfm-grammar

"""
import sys
import os
import argparse
import json
from glob import glob


def find_usfm_files(root_dir, pattern):
    # non-recursive in root dir and immediate subdirs? use recursive glob
    search = os.path.join(root_dir, '**', pattern)
    return sorted(glob(search, recursive=True))


def read_file(path):
    for enc in ('utf-8', 'utf-8-sig', 'latin-1'):
        try:
            with open(path, 'r', encoding=enc) as f:
                return f.read()
        except UnicodeDecodeError:
            continue
    # last resort
    with open(path, 'rb') as f:
        return f.read().decode('latin-1', errors='replace')


def main():
    parser = argparse.ArgumentParser(description='Check .usfm files for parser errors using usfm-grammar')
    parser.add_argument('--dir', '-d', default='.', help='Directory to scan (default: current directory)')
    parser.add_argument('--pattern', '-p', default='*.usfm', help='Glob pattern for USFM files (default: "*.usfm")')
    parser.add_argument('--quiet', '-q', action='store_true', help='Only output files with errors')
    args = parser.parse_args()

    try:
        from usfm_grammar import USFMParser, Filter
    except Exception as e:
        print('Failed to import usfm_grammar. Install it with: pip install usfm-grammar')
        print('Import error:', str(e))
        sys.exit(2)

    files = find_usfm_files(args.dir, args.pattern)
    if not files:
        print('No .usfm files found (pattern: {}).'.format(args.pattern))
        sys.exit(0)

    any_errors = False
    summary = []

    for fp in files:
        try:
            text = read_file(fp)
        except Exception as e:
            print(f'Error reading {fp}: {e}')
            any_errors = True
            summary.append({'file': fp, 'error': f'read_error: {e}'})
            continue

        try:
            p = USFMParser(text)
            errs = p.errors
        except Exception as e:
            # parser threw; treat as error
            errs = [{'exception': str(e)}]

        # normalize
        if not errs:
            if not args.quiet:
                print(f'{fp}: OK')
            summary.append({'file': fp, 'errors': []})
        else:
            any_errors = True
            print(f'{fp}: {len(errs)} error(s)')
            try:
                print(json.dumps(errs, ensure_ascii=False, indent=2))
            except Exception:
                print(errs)
            summary.append({'file': fp, 'errors': errs})

    # write a short JSON summary
    out_summary = 'usfm_check_summary.json'
    try:
        with open(out_summary, 'w', encoding='utf-8') as outf:
            json.dump(summary, outf, ensure_ascii=False, indent=2)
        if not args.quiet:
            print('\nWrote summary to', out_summary)
    except Exception as e:
        print('Failed to write summary file:', e)

    if any_errors:
        print('\nOne or more files had errors.')
        sys.exit(1)
    else:
        print('\nAll files parsed without errors.')
        sys.exit(0)


if __name__ == '__main__':
    main()
