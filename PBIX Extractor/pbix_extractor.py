#!/usr/bin/env python3
"""
PBIX Complete Metadata Extractor
=================================
Extracts ALL metadata from Power BI (.pbix) files and generates
a comprehensive, LLM-ready PDF report.

Usage:
    python pbix_extractor.py <file.pbix> [output.pdf]
    python pbix_extractor.py <file.pbix> --text   (plain text output)

Requirements:
    pip install reportlab

Security:
    - NEVER reads row data / compressed data partitions
    - Blocks: DataModel binary data, .abf, xpress9, vertipaq streams
    - Extracts schema/metadata only
"""

import sys
import os
import zipfile
import json
import xml.etree.ElementTree as ET
import re
import struct
import datetime
import argparse
from pathlib import Path
from collections import defaultdict

# ── PDF generation ──────────────────────────────────────────────────────────
try:
    from reportlab.lib.pagesizes import A4, letter
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import inch, cm
    from reportlab.lib import colors
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, PageBreak,
        Table, TableStyle, HRFlowable, KeepTogether, Preformatted
    )
    from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
    REPORTLAB_AVAILABLE = True
except ImportError:
    REPORTLAB_AVAILABLE = False
    print("WARNING: reportlab not installed. PDF output unavailable.")
    print("Install with: pip install reportlab")

# ═══════════════════════════════════════════════════════════════════════════
# SECURITY CONSTANTS - Files we NEVER read data from
# ═══════════════════════════════════════════════════════════════════════════
BLOCKED_PATTERNS = [
    r'DataModel$',           # Main binary tabular model data
    r'\.abf$',               # Analysis Services backup files
    r'xpress9',              # xpress9 compressed data
    r'vertipaq',             # VertiPaq column store data
    r'\.data$',              # Raw data files
    r'Partition',            # Data partitions
    r'StorageFiles',         # Storage data files
]

SAFE_TEXT_FILES = [
    'Report/Layout',
    'Connections',
    '[Content_Types].xml',
    'DiagramLayout',
    'SecurityBindings',
    'Version',
    'Settings',
    'Metadata',
    'Report/StaticResources',
    'DataMashup',
    'CustomVisuals',
]

# ═══════════════════════════════════════════════════════════════════════════
# DATA EXTRACTION ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class PBIXExtractor:
    def __init__(self, pbix_path: str):
        self.pbix_path = pbix_path
        self.file_name = Path(pbix_path).name
        self.metadata = {
            'file_info': {},
            'zip_contents': [],
            'report_layout': {},
            'pages': [],
            'visuals': [],
            'bookmarks': [],
            'themes': {},
            'data_model': {},
            'tables': [],
            'columns': [],
            'measures': [],
            'calculated_columns': [],
            'calculated_tables': [],
            'relationships': [],
            'hierarchies': [],
            'calculation_groups': [],
            'perspectives': [],
            'translations': [],
            'data_sources': [],
            'connections': {},
            'power_query': [],
            'parameters': [],
            'rls_roles': [],
            'aggregations': [],
            'field_parameters': [],
            'annotations': [],
            'custom_visuals': [],
            'errors': [],
            'warnings': [],
        }
        self.raw_files = {}

    def is_blocked(self, name: str) -> bool:
        for pattern in BLOCKED_PATTERNS:
            if re.search(pattern, name, re.IGNORECASE):
                return True
        return False

    def extract(self):
        """Main extraction pipeline."""
        print(f"[*] Opening: {self.pbix_path}")

        # File info
        stat = os.stat(self.pbix_path)
        self.metadata['file_info'] = {
            'filename': self.file_name,
            'path': str(Path(self.pbix_path).resolve()),
            'size_bytes': stat.st_size,
            'size_mb': round(stat.st_size / (1024 * 1024), 2),
            'extracted_at': datetime.datetime.now().isoformat(),
        }

        try:
            with zipfile.ZipFile(self.pbix_path, 'r') as zf:
                self._catalog_contents(zf)
                self._read_safe_files(zf)
        except zipfile.BadZipFile:
            self.metadata['errors'].append("File is not a valid ZIP/PBIX archive.")
            return self.metadata

        self._parse_version()
        self._parse_connections()
        self._parse_layout()
        self._parse_data_mashup()
        self._parse_security_bindings()
        self._parse_data_model_schema()
        self._extract_theme_details()
        self._build_slicer_and_filter_inventory()
        self._generate_page_narratives()
        self._generate_llm_briefing()

        print(f"[✓] Extraction complete.")
        return self.metadata

    # ── ZIP Catalog ──────────────────────────────────────────────────────────
    def _catalog_contents(self, zf: zipfile.ZipFile):
        contents = []
        for info in zf.infolist():
            blocked = self.is_blocked(info.filename)
            contents.append({
                'name': info.filename,
                'size_bytes': info.file_size,
                'compressed_bytes': info.compress_size,
                'compress_type': info.compress_type,
                'blocked': blocked,
            })
        self.metadata['zip_contents'] = contents
        print(f"  ZIP contains {len(contents)} entries, {sum(1 for c in contents if c['blocked'])} blocked for security")

    # ── Read safe files ───────────────────────────────────────────────────────
    def _read_safe_files(self, zf: zipfile.ZipFile):
        for info in zf.infolist():
            if self.is_blocked(info.filename):
                continue
            # Skip large binary files we don't handle
            if info.file_size > 50 * 1024 * 1024:
                self.metadata['warnings'].append(f"Skipped large file: {info.filename} ({info.file_size} bytes)")
                continue
            try:
                data = zf.read(info.filename)
                self.raw_files[info.filename] = data
            except Exception as e:
                self.metadata['warnings'].append(f"Could not read {info.filename}: {e}")

    # ── Version ───────────────────────────────────────────────────────────────
    def _parse_version(self):
        raw = self.raw_files.get('Version', b'')
        if raw:
            try:
                self.metadata['file_info']['pbix_version'] = raw.decode('utf-8', errors='replace').strip()
            except Exception:
                pass

    # ── Connections ───────────────────────────────────────────────────────────
    def _parse_connections(self):
        raw = self.raw_files.get('Connections', b'')
        if not raw:
            return
        try:
            conn_data = json.loads(raw.decode('utf-8-sig', errors='replace'))
            self.metadata['connections'] = conn_data

            # Detect live/remote connection to Power BI Service
            remote_artifacts = conn_data.get('RemoteArtifacts', [])
            if remote_artifacts:
                for ra in remote_artifacts:
                    self.metadata['data_sources'].append({
                        'name': 'Power BI Service Dataset (Live Connection)',
                        'connection_type': 'PowerBIServiceLive',
                        'dataset_id': ra.get('DatasetId', ''),
                        'report_id': ra.get('ReportId', ''),
                        'workspace_id': conn_data.get('OriginalWorkspaceObjectId', ''),
                        'provider': 'Power BI Service',
                        'mode': 'DirectQuery/Live',
                        'note': (
                            'This report connects to a published Power BI Service dataset. '
                            'The full data model (tables, columns, measures, relationships) '
                            'is defined in the service dataset, not embedded in this .pbix file. '
                            'Schema shown below is inferred from visual field bindings.'
                        ),
                    })
                self.metadata['file_info']['connection_type'] = 'PowerBIServiceLive'

            # Local/import connections
            connections = conn_data.get('Connections', [])
            for conn in connections:
                ds = {
                    'name': conn.get('Name', ''),
                    'connection_string': self._sanitize_connection_string(conn.get('ConnectionString', '')),
                    'provider': conn.get('Provider', ''),
                    'pbi_model_database_name': conn.get('PbiModelDatabaseName', ''),
                    'pbi_service_model_id': conn.get('PbiServiceModelId', ''),
                    'mode': conn.get('Mode', ''),
                }
                self.metadata['data_sources'].append(ds)
        except Exception as e:
            self.metadata['warnings'].append(f"Connections parse error: {e}")

    def _sanitize_connection_string(self, cs: str) -> str:
        """Remove credentials from connection strings."""
        if not cs:
            return cs
        # Mask password values
        cs = re.sub(r'(password|pwd|secret)=[^;]+', r'\1=***REDACTED***', cs, flags=re.IGNORECASE)
        cs = re.sub(r'(uid|user id|userid)=[^;]+', r'\1=***REDACTED***', cs, flags=re.IGNORECASE)
        return cs

    # ── Report Layout ─────────────────────────────────────────────────────────
    def _parse_layout(self):
        raw = self.raw_files.get('Report/Layout', b'')
        if not raw:
            self.metadata['warnings'].append("Report/Layout not found")
            return
        try:
            # PBIX layout uses UTF-16 LE encoding
            try:
                text = raw.decode('utf-16-le', errors='replace')
            except Exception:
                text = raw.decode('utf-8', errors='replace')

            layout = json.loads(text)
            self._extract_report_properties(layout)
            self._extract_pages(layout)
            self._extract_bookmarks(layout)
            self._extract_custom_visuals(layout)
        except json.JSONDecodeError as e:
            self.metadata['warnings'].append(f"Layout JSON parse error: {e}")
        except Exception as e:
            self.metadata['warnings'].append(f"Layout parse error: {e}")

    def _extract_report_properties(self, layout: dict):
        props = {
            'id': layout.get('id', ''),
            'report_id': layout.get('reportId', ''),
            'theme_name': layout.get('theme', {}).get('name', '') if isinstance(layout.get('theme'), dict) else '',
            'layout_optimized_for': layout.get('layoutOptimizedFor', ''),
            'slow_data_source_settings': layout.get('slowDataSourceSettings', {}),
            'pods_settings': layout.get('podsSettings', {}),
        }

        # Config blob
        config_raw = layout.get('config', '{}')
        if isinstance(config_raw, str):
            try:
                config = json.loads(config_raw)
                props['report_config'] = config
                # Theme
                theme = config.get('themeCollection', {}).get('baseTheme', {})
                self.metadata['themes'] = {
                    'name': theme.get('name', ''),
                    'version': theme.get('version', ''),
                    'type': theme.get('type', ''),
                }
            except Exception:
                pass

        self.metadata['report_layout'] = props

    def _extract_pages(self, layout: dict):
        sections = layout.get('sections', [])
        for section in sections:
            page = {
                'name': section.get('name', ''),
                'display_name': section.get('displayName', ''),
                'width': section.get('width', 0),
                'height': section.get('height', 0),
                'order': section.get('ordinal', 0),
                'hidden': section.get('visibility', 0) == 1,
                'visual_count': 0,
                'visuals': [],
            }
            config_raw = section.get('config', '{}')
            if isinstance(config_raw, str):
                try:
                    config = json.loads(config_raw)
                    page['background'] = config.get('background', {})
                    page['wallpaper'] = config.get('wallpaper', {})
                    page['display_option'] = config.get('defaultDisplayOption', '')
                except Exception:
                    pass

            # Page-level filters (store raw for later decoding)
            pf_raw = section.get('filters', '[]')
            if isinstance(pf_raw, str):
                try:
                    page['_raw_page_filters'] = json.loads(pf_raw)
                    for f in page['_raw_page_filters']:
                        self._walk_expr_for_fields(f, {})
                except Exception:
                    page['_raw_page_filters'] = []
            else:
                page['_raw_page_filters'] = []

            containers = section.get('visualContainers', [])
            for vc in containers:
                visual = self._parse_visual(vc, page['name'])
                if visual:
                    page['visuals'].append(visual)
                    self.metadata['visuals'].append(visual)

            page['visual_count'] = len(page['visuals'])
            self.metadata['pages'].append(page)

        # Report-level filters
        rlf_raw = layout.get('filters', '[]')
        if isinstance(rlf_raw, str):
            try:
                raw_rf = json.loads(rlf_raw)
                self.metadata['_raw_report_filters'] = raw_rf
                for f in raw_rf:
                    self._walk_expr_for_fields(f, {})
            except Exception:
                self.metadata['_raw_report_filters'] = []
        else:
            self.metadata['_raw_report_filters'] = []

        print(f"  Parsed {len(self.metadata['pages'])} pages, {len(self.metadata['visuals'])} visuals")

    def _build_alias_map(self, pq: dict) -> dict:
        """Build alias->entity map from the From clause of a prototypeQuery."""
        alias_map = {}
        for src in pq.get('From', []):
            name = src.get('Name', '')
            entity = src.get('Entity', '')
            if name and entity:
                alias_map[name] = entity
        return alias_map

    def _resolve_entity(self, source_ref: dict, alias_map: dict) -> str:
        """Resolve a SourceRef to an entity (table) name."""
        src = source_ref.get('Source', '')
        if src and src in alias_map:
            return alias_map[src]
        # Fallback: direct Entity field
        entity = source_ref.get('Entity', '')
        return alias_map.get(entity, entity)

    def _walk_expr_for_fields(self, obj, alias_map: dict):
        """
        Recursively walk any JSON structure and collect all Column/Measure
        references into self.metadata['_field_inventory'].
        """
        inv = self.metadata.setdefault('_field_inventory', {})
        if not isinstance(obj, dict):
            if isinstance(obj, list):
                for item in obj:
                    self._walk_expr_for_fields(item, alias_map)
            return

        col = obj.get('Column')
        if isinstance(col, dict):
            sr = col.get('Expression', {}).get('SourceRef', {})
            entity = self._resolve_entity(sr, alias_map)
            prop = col.get('Property', '')
            if entity and prop and len(entity) < 100 and len(prop) < 200:
                inv.setdefault(entity, {'columns': set(), 'measures': set(), 'hierarchies': {}})
                inv[entity]['columns'].add(prop)

        meas = obj.get('Measure')
        if isinstance(meas, dict):
            sr = meas.get('Expression', {}).get('SourceRef', {})
            entity = self._resolve_entity(sr, alias_map)
            prop = meas.get('Property', '')
            if entity and prop and len(entity) < 100 and len(prop) < 200:
                inv.setdefault(entity, {'columns': set(), 'measures': set(), 'hierarchies': {}})
                inv[entity]['measures'].add(prop)

        agg = obj.get('Aggregation')
        if isinstance(agg, dict):
            self._walk_expr_for_fields(agg.get('Expression', {}), alias_map)

        hl = obj.get('HierarchyLevel')
        if isinstance(hl, dict):
            hier_obj = hl.get('Expression', {}).get('Hierarchy', {})
            pvs = hier_obj.get('Expression', {}).get('PropertyVariationSource', {})
            sr = pvs.get('Expression', {}).get('SourceRef', {})
            entity = self._resolve_entity(sr, alias_map)
            level = hl.get('Level', '')
            hier_name = hier_obj.get('Hierarchy', '')
            if entity and hier_name:
                inv.setdefault(entity, {'columns': set(), 'measures': set(), 'hierarchies': {}})
                inv[entity]['hierarchies'].setdefault(hier_name, set()).add(level)

        for v in obj.values():
            self._walk_expr_for_fields(v, alias_map)

    def _parse_visual(self, vc: dict, page_name: str) -> dict:
        visual = {
            'page': page_name,
            'x': vc.get('x', 0),
            'y': vc.get('y', 0),
            'z': vc.get('z', 0),
            'width': vc.get('width', 0),
            'height': vc.get('height', 0),
            'visual_type': '',
            'title': '',
            'hidden': vc.get('visibility', 0) == 1,
            'data_fields': [],
            'filters': [],
            'field_roles': {},
            'decoded_filters': [],
            'slicer_details': {},
        }

        config_raw = vc.get('config', '{}')
        if not isinstance(config_raw, str):
            return visual
        try:
            config = json.loads(config_raw)
            single_visual = config.get('singleVisual', {})
            visual['visual_type'] = single_visual.get('visualType', '')

            # Title from vcObjects
            vcObjects = single_visual.get('vcObjects', {})
            title_obj = vcObjects.get('title', [{}])
            if title_obj and isinstance(title_obj, list):
                props = title_obj[0].get('properties', {})
                text_val = props.get('text', {})
                if isinstance(text_val, dict):
                    lit = text_val.get('expr', {}).get('Literal', {}).get('Value', '')
                    visual['title'] = lit.strip("'\"" ) if lit else ''

            pq = single_visual.get('prototypeQuery', {})
            alias_map = self._build_alias_map(pq)

            # Walk the entire prototypeQuery for field inventory
            self._walk_expr_for_fields(pq, alias_map)

            # Build the data_fields list for the visual (human-readable)
            for sel in pq.get('Select', []):
                field_str = self._format_select_item(sel, alias_map)
                if field_str:
                    visual['data_fields'].append(field_str)

            # Field roles (projections: Y, Category, Values, Series, etc.)
            visual['field_roles'] = self._parse_field_roles(single_visual, alias_map)

            # Slicer details
            if visual['visual_type'] == 'slicer':
                visual['slicer_details'] = self._parse_slicer_details(single_visual, alias_map)

            # Visual filters — raw + decoded
            filters_raw = vc.get('filters', '[]')
            if isinstance(filters_raw, str):
                try:
                    raw_filters = json.loads(filters_raw)
                    for f in raw_filters:
                        self._walk_expr_for_fields(f, {})
                        visual['filters'].append(self._parse_filter(f))
                    visual['decoded_filters'] = self._decode_filters_list(raw_filters, alias_map)
                except Exception:
                    pass

        except Exception:
            pass

        return visual

    def _format_select_item(self, sel: dict, alias_map: dict) -> str:
        """Format a Select clause item as a human-readable string."""
        if not sel:
            return ''
        # Use the NativeReferenceName or Name as the label
        native = sel.get('NativeReferenceName', '')
        name = sel.get('Name', '')

        col = sel.get('Column')
        if col:
            entity = self._resolve_entity(col.get('Expression', {}).get('SourceRef', {}), alias_map)
            prop = col.get('Property', '')
            return f"{entity}[{prop}]" if entity else prop

        meas = sel.get('Measure')
        if meas:
            entity = self._resolve_entity(meas.get('Expression', {}).get('SourceRef', {}), alias_map)
            prop = meas.get('Property', '')
            return f"{entity}[{prop}]" if entity else prop

        agg = sel.get('Aggregation')
        if agg:
            inner_col = agg.get('Expression', {}).get('Column', {})
            entity = self._resolve_entity(
                inner_col.get('Expression', {}).get('SourceRef', {}), alias_map)
            prop = inner_col.get('Property', '')
            func_map = {0: 'Sum', 1: 'Avg', 3: 'Min', 4: 'Max', 5: 'Count', 6: 'CountNonNull'}
            func = func_map.get(agg.get('Function', -1), 'Agg')
            return f"{func}({entity}[{prop}])" if entity else f"{func}({prop})"

        hl = sel.get('HierarchyLevel')
        if hl:
            level = hl.get('Level', '')
            hier_name = hl.get('Expression', {}).get('Hierarchy', {}).get('Hierarchy', '')
            pvs = (hl.get('Expression', {}).get('Hierarchy', {})
                     .get('Expression', {}).get('PropertyVariationSource', {}))
            entity = self._resolve_entity(
                pvs.get('Expression', {}).get('SourceRef', {}), alias_map)
            return f"{entity}[{hier_name}.{level}]" if entity else f"{hier_name}.{level}"

        return native or name or ''

    def _parse_filter(self, f: dict) -> dict:
        expr = f.get('expression', {})
        col = expr.get('Column', {})
        meas = expr.get('Measure', {})
        entity = ''
        prop = ''
        if col:
            entity = col.get('Expression', {}).get('SourceRef', {}).get('Entity', '')
            prop = col.get('Property', '')
        elif meas:
            entity = meas.get('Expression', {}).get('SourceRef', {}).get('Entity', '')
            prop = meas.get('Property', '')
        return {
            'type': f.get('type', ''),
            'entity': entity,
            'field': prop,
            'display': f"{entity}[{prop}]" if entity and prop else f.get('name', ''),
        }

    def _extract_bookmarks(self, layout: dict):
        bookmarks_raw = layout.get('config', '{}')
        if isinstance(bookmarks_raw, str):
            try:
                config = json.loads(bookmarks_raw)
                bookmarks = config.get('bookmarks', [])
                for bm in bookmarks:
                    self.metadata['bookmarks'].append({
                        'name': bm.get('displayName', bm.get('name', '')),
                        'id': bm.get('name', ''),
                        'report_page': bm.get('explorationState', {}).get('activeSection', ''),
                    })
            except Exception:
                pass

    def _extract_custom_visuals(self, layout: dict):
        # Custom visuals referenced
        custom_raw = layout.get('publicCustomVisuals', [])
        for cv in custom_raw:
            self.metadata['custom_visuals'].append({
                'name': cv.get('name', ''),
                'guid': cv.get('guid', ''),
                'version': cv.get('version', ''),
            })

    # ── DataMashup (Power Query) ───────────────────────────────────────────────
    def _parse_data_mashup(self):
        raw = self.raw_files.get('DataMashup', b'')
        if not raw:
            return
        # DataMashup is a ZIP-in-ZIP containing Formulas/Section1.m etc.
        try:
            import io
            # The DataMashup has a 4-byte version header, then a ZIP
            # Try to find the nested ZIP
            zip_start = raw.find(b'PK\x03\x04')
            if zip_start == -1:
                zip_start = 0
            inner_data = raw[zip_start:]
            with zipfile.ZipFile(io.BytesIO(inner_data), 'r') as inner_zf:
                self._parse_mashup_zip(inner_zf)
        except Exception as e:
            self.metadata['warnings'].append(f"DataMashup parse error: {e}")

    def _parse_mashup_zip(self, zf: zipfile.ZipFile):
        for name in zf.namelist():
            if name.endswith('.m') or name.endswith('.pq') or 'Section' in name:
                try:
                    m_code = zf.read(name).decode('utf-8', errors='replace')
                    queries = self._parse_m_code(m_code, name)
                    self.metadata['power_query'].extend(queries)
                except Exception as e:
                    self.metadata['warnings'].append(f"M code parse error in {name}: {e}")

            elif name == 'Config/Package.xml':
                try:
                    xml_text = zf.read(name).decode('utf-8', errors='replace')
                    root = ET.fromstring(xml_text)
                    ns = {'m': 'http://schemas.microsoft.com/DataMashup'}
                    # Extract parameters
                    for param in root.iter('AllowedValue'):
                        pass  # handled by M parsing
                except Exception:
                    pass

    def _parse_m_code(self, m_code: str, source_file: str) -> list:
        """Parse M code to extract individual queries."""
        queries = []
        # M code in Power BI is a "section document"
        # section Section1; shared QueryName = ...
        lines = m_code.split('\n')
        current_query = None
        current_lines = []

        for line in lines:
            # Check for shared query definition
            shared_match = re.match(r'^\s*shared\s+([^\s=]+)\s*=', line)
            let_block_match = re.match(r'^\s*([A-Za-z_][A-Za-z0-9_\s]*)\s*=\s*let\b', line)

            if shared_match:
                if current_query and current_lines:
                    queries.append({
                        'name': current_query,
                        'source_file': source_file,
                        'm_code': '\n'.join(current_lines).strip(),
                        'type': 'query',
                    })
                current_query = shared_match.group(1).strip('"').strip()
                current_lines = [line]
            else:
                if current_query:
                    current_lines.append(line)

        if current_query and current_lines:
            queries.append({
                'name': current_query,
                'source_file': source_file,
                'm_code': '\n'.join(current_lines).strip(),
                'type': 'query',
            })

        if not queries and m_code.strip():
            # Return raw code as single unnamed query
            queries.append({
                'name': Path(source_file).stem,
                'source_file': source_file,
                'm_code': m_code.strip(),
                'type': 'raw',
            })

        print(f"  Parsed {len(queries)} Power Query queries")
        return queries

    # ── Security Bindings (RLS) ──────────────────────────────────────────────
    def _parse_security_bindings(self):
        raw = self.raw_files.get('SecurityBindings', b'')
        if not raw:
            return
        # Strip BOM; handle empty or binary-only files silently
        cleaned = raw.lstrip(b'\xff\xfe\xfe\xff\xef\xbb\xbf').replace(b'\x00', b'').strip()
        if not cleaned:
            return
        try:
            for enc in ('utf-8-sig', 'utf-16', 'utf-8'):
                try:
                    text = raw.decode(enc, errors='replace').strip().lstrip('\ufeff')
                    if text and text[0] in ('{', '['):
                        data = json.loads(text)
                        break
                except Exception:
                    continue
            else:
                return
            roles = data.get('Roles', data.get('roles', []))
            for role in roles:
                rls = {
                    'name': role.get('Name', role.get('name', '')),
                    'model_permission': role.get('ModelPermission', ''),
                    'table_filters': [],
                    'members': role.get('Members', []),
                }
                for tp in role.get('TablePermissions', []):
                    rls['table_filters'].append({
                        'table': tp.get('Name', ''),
                        'filter_expression': tp.get('FilterExpression', ''),
                    })
                self.metadata['rls_roles'].append(rls)
        except Exception:
            pass  # SecurityBindings is optional

    # ── Diagram Layout (table names & positions) ─────────────────────────────
    def _parse_diagram_layout(self):
        raw = self.raw_files.get('DiagramLayout', b'')
        if not raw:
            return
        try:
            for enc in ('utf-8-sig', 'utf-16', 'utf-8'):
                try:
                    text = raw.decode(enc, errors='replace').strip().lstrip('\ufeff')
                    if text and text[0] in ('{', '['):
                        data = json.loads(text)
                        break
                except Exception:
                    continue
            else:
                return

            diagram_tables = []
            for diagram in data.get('diagrams', []):
                for node in diagram.get('nodes', []):
                    # DiagramLayout uses nodeIndex (newer) or name (older) for table identity
                    tbl_name = (node.get('nodeIndex') or node.get('name') or '').strip()
                    loc = node.get('location', {})
                    sz = node.get('size', {})
                    if tbl_name:
                        diagram_tables.append({
                            'name': tbl_name,
                            'x': loc.get('x', node.get('x', 0)),
                            'y': loc.get('y', node.get('y', 0)),
                            'width': sz.get('width', node.get('width', 0)),
                            'height': sz.get('height', node.get('height', 0)),
                        })

            if diagram_tables:
                self.metadata['diagram_tables'] = diagram_tables
                print(f"  DiagramLayout: found {len(diagram_tables)} table nodes")
        except Exception as e:
            self.metadata['warnings'].append(f"DiagramLayout parse error: {e}")

    # ── Metadata file ────────────────────────────────────────────────────────
    def _parse_metadata_file(self):
        raw = self.raw_files.get('Metadata', b'')
        if not raw:
            return
        try:
            for enc in ('utf-8-sig', 'utf-16', 'utf-8'):
                try:
                    text = raw.decode(enc, errors='replace').strip().lstrip('\ufeff')
                    if text and text[0] in ('{', '['):
                        data = json.loads(text)
                        self.metadata['file_info']['metadata_version'] = data.get('version', '')
                        self.metadata['file_info']['metadata'] = data
                        break
                except Exception:
                    continue
        except Exception:
            pass

    # ── Data Model Schema (JSON embedded in binary DataModel) ─────────────────
    def _parse_data_model_schema(self):
        """
        Extract TOM (Tabular Object Model) JSON from DataModel binary.
        Tries multiple encodings and search strategies. Never reads row data.
        """
        self._parse_diagram_layout()
        self._parse_metadata_file()

        raw = self.raw_files.get('DataModel')
        if raw is None:
            self._try_parse_datamodel_from_zip()
        else:
            self._parse_datamodel_bytes(raw)

        # Always run visual-field inference as a supplement/fallback
        self._infer_schema_from_visuals()

    def _try_parse_datamodel_from_zip(self):
        """Read first 15MB of DataModel entry (schema is at the start, row data later)."""
        try:
            with zipfile.ZipFile(self.pbix_path, 'r') as zf:
                names = [i.filename for i in zf.infolist()]
                datamodel_name = next((n for n in names if n == 'DataModel'), None)
                if not datamodel_name:
                    self.metadata['warnings'].append("DataModel entry not found in PBIX")
                    return
                info = zf.getinfo(datamodel_name)
                max_read = min(info.file_size, 15 * 1024 * 1024)
                with zf.open(datamodel_name) as f:
                    raw = f.read(max_read)
                self._parse_datamodel_bytes(raw)
        except Exception as e:
            self.metadata['warnings'].append(f"DataModel read error: {e}")

    def _parse_datamodel_bytes(self, raw: bytes):
        """
        Multi-strategy JSON extraction from DataModel binary.
        Analysis Services / Power BI Desktop stores the model schema as
        a JSON blob, but the encoding and position vary by version.
        """
        print("  Parsing DataModel binary for schema...")
        schema_found = False

        # ── Strategy 1: search for JSON in both UTF-8 and UTF-16 LE ──────────
        candidates = []

        # UTF-8 text version
        text_utf8 = raw.decode('utf-8', errors='replace').replace('\x00', '')

        # UTF-16 LE text version (Analysis Services often uses this)
        try:
            text_utf16 = raw.decode('utf-16-le', errors='replace')
        except Exception:
            text_utf16 = ''

        for text in [text_utf8, text_utf16]:
            if not text:
                continue
            # Find every position of common TOM JSON markers
            markers_str = [
                '"tables":[', '"tables": [',
                '"relationships":[', '"relationships": [',
                '"compatibilityLevel"',
                '"model":{', '"model": {',
                '"measures":[', '"measures": [',
                '"columns":[', '"columns": [',
            ]
            for marker in markers_str:
                pos = text.find(marker)
                if pos == -1:
                    continue
                # Walk back to find the enclosing {
                start = pos
                depth = 0
                for i in range(pos, max(pos - 50000, 0), -1):
                    ch = text[i]
                    if ch == '}':
                        depth += 1
                    elif ch == '{':
                        if depth == 0:
                            start = i
                            break
                        depth -= 1
                if start < pos:
                    candidates.append((start, text))

        # De-duplicate and try to parse
        seen_starts = set()
        for start, text in candidates:
            if start in seen_starts:
                continue
            seen_starts.add(start)
            parsed = self._try_json_parse_text(text[start:])
            if parsed:
                tables_found = self._count_tables_in_json(parsed)
                if tables_found > 0 or self._looks_like_model(parsed):
                    self._extract_model_from_json(parsed)
                    schema_found = True
                    break

        # ── Strategy 2: find standalone "tables" array ────────────────────────
        if not schema_found:
            for text in [text_utf8, text_utf16]:
                parsed = self._extract_tables_array(text)
                if parsed:
                    schema_found = True
                    break

        # ── Strategy 3: deep regex scan for measures, columns, relationships ──
        if not schema_found or (not self.metadata['tables'] and not self.metadata['measures']):
            self._extract_model_with_regex_deep(text_utf8, text_utf16)

        if not schema_found and not self.metadata['tables']:
            self.metadata['warnings'].append(
                "DataModel schema could not be fully parsed. "
                "Tables/measures shown below are inferred from visual field bindings."
            )

    def _looks_like_model(self, d: dict) -> bool:
        """Check if a parsed dict looks like a TOM model."""
        if not isinstance(d, dict):
            return False
        model_keys = {'tables', 'relationships', 'compatibilityLevel', 'model',
                      'measures', 'columns', 'culture', 'collation'}
        return bool(model_keys & set(d.keys()))

    def _count_tables_in_json(self, d: dict) -> int:
        """Count tables in a parsed JSON object, navigating nested model containers."""
        if not isinstance(d, dict):
            return 0
        for key in ('tables', ):
            if key in d and isinstance(d[key], list):
                return len(d[key])
        for sub_key in ('model', 'database', 'create'):
            if sub_key in d:
                sub = d[sub_key]
                if isinstance(sub, dict):
                    count = self._count_tables_in_json(sub)
                    if count:
                        return count
        return 0

    def _try_json_parse_text(self, text: str) -> dict:
        """Try to parse JSON from text, handling truncation gracefully."""
        text = text.strip()
        if not text or text[0] != '{':
            return None
        # Direct parse
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass
        # Brace-matching parse (handles trailing garbage)
        depth = 0
        in_string = False
        escape = False
        for i, ch in enumerate(text):
            if escape:
                escape = False
                continue
            if ch == '\\' and in_string:
                escape = True
                continue
            if ch == '"':
                in_string = not in_string
            if not in_string:
                if ch == '{':
                    depth += 1
                elif ch == '}':
                    depth -= 1
                    if depth == 0:
                        try:
                            return json.loads(text[:i + 1])
                        except Exception:
                            return None
        return None

    def _extract_tables_array(self, text: str) -> bool:
        """Find and parse a standalone tables array from text."""
        for marker in ['"tables":[', '"tables": [']:
            pos = text.find(marker)
            if pos == -1:
                continue
            arr_start = text.find('[', pos)
            if arr_start == -1:
                continue
            # Find matching ]
            depth = 0
            for i in range(arr_start, min(arr_start + 5000000, len(text))):
                if text[i] == '[':
                    depth += 1
                elif text[i] == ']':
                    depth -= 1
                    if depth == 0:
                        try:
                            tables_json = json.loads(text[arr_start:i + 1])
                            for tbl in tables_json:
                                if isinstance(tbl, dict) and 'name' in tbl:
                                    self._parse_table(tbl)
                            if self.metadata['tables']:
                                print(f"  Extracted {len(self.metadata['tables'])} tables from tables array")
                                return True
                        except Exception:
                            pass
                        break
        return False

    def _extract_model_from_json(self, data: dict):
        """Extract model objects from parsed TOM JSON."""
        print("  TOM JSON schema found and parsed!")

        # Navigate to the model — handle various nesting patterns
        model = data
        for nav in ['create', 'database', 'model']:
            if nav in model and isinstance(model[nav], dict):
                model = model[nav]

        # Also try: {"model": {...}} at root or {"database": {"model": {...}}}
        if 'model' in model and isinstance(model['model'], dict):
            model = model['model']

        dm = self.metadata['data_model']
        dm['name'] = model.get('name', data.get('name', ''))
        dm['id'] = model.get('id', data.get('id', ''))
        dm['compatibility_level'] = (
            model.get('compatibilityLevel') or
            data.get('compatibilityLevel', '')
        )
        dm['culture'] = model.get('culture', '')
        dm['collation'] = model.get('collation', '')
        dm['default_power_bi_data_source_version'] = model.get('defaultPowerBIDataSourceVersion', '')
        dm['source_query_culture'] = model.get('sourceQueryCulture', '')
        dm['annotations'] = model.get('annotations', [])

        for tbl in model.get('tables', []):
            self._parse_table(tbl)

        for rel in model.get('relationships', []):
            self._parse_relationship(rel)

        for role in model.get('roles', []):
            rls = {
                'name': role.get('name', ''),
                'model_permission': role.get('modelPermission', ''),
                'table_filters': [],
                'members': [],
            }
            for tp in role.get('tablePermissions', []):
                rls['table_filters'].append({
                    'table': tp.get('name', ''),
                    'filter_expression': tp.get('filterExpression', ''),
                })
            for member in role.get('members', []):
                rls['members'].append({
                    'member_name': member.get('memberName', ''),
                    'identity_provider': member.get('identityProvider', ''),
                })
            if not self.metadata['rls_roles']:
                self.metadata['rls_roles'].append(rls)

        for persp in model.get('perspectives', []):
            self.metadata['perspectives'].append({
                'name': persp.get('name', ''),
                'tables': [t.get('name', '') for t in persp.get('perspectiveTables', [])],
            })

        for trans in model.get('cultures', []):
            self.metadata['translations'].append({
                'name': trans.get('name', ''),
                'linguistic_metadata': trans.get('linguisticMetadata', {}),
            })

        print(f"  Extracted {len(self.metadata['tables'])} tables, "
              f"{len(self.metadata['measures'])} measures, "
              f"{len(self.metadata['relationships'])} relationships")

    def _parse_table(self, tbl: dict):
        table = {
            'name': tbl.get('name', ''),
            'description': tbl.get('description', ''),
            'hidden': tbl.get('isHidden', False),
            'storage_mode': tbl.get('storageMode', ''),
            'show_as_variations_only': tbl.get('showAsVariationsOnly', False),
            'is_private': tbl.get('isPrivate', False),
            'annotations': tbl.get('annotations', []),
            'lineage_tag': tbl.get('lineageTag', ''),
            'source_lineage_tag': tbl.get('sourceLineageTag', ''),
            'columns': [],
            'measures': [],
            'partitions': [],
            'hierarchies': [],
        }

        for part in tbl.get('partitions', []):
            source = part.get('source', {})
            p = {
                'name': part.get('name', ''),
                'mode': part.get('mode', ''),
                'source_type': source.get('type', ''),
                'm_expression': source.get('expression', ''),
                'refresh_policy_section': part.get('refreshPolicy', {}),
            }
            table['partitions'].append(p)
            if p['m_expression']:
                existing_names = [q['name'] for q in self.metadata['power_query']]
                if table['name'] not in existing_names:
                    m_expr = p['m_expression']
                    if isinstance(m_expr, list):
                        m_expr = '\n'.join(m_expr)
                    self.metadata['power_query'].append({
                        'name': table['name'],
                        'source_file': 'DataModel/Partition',
                        'm_code': m_expr,
                        'type': 'partition',
                    })

        for col in tbl.get('columns', []):
            col_info = {
                'table': table['name'],
                'name': col.get('name', ''),
                'data_type': col.get('dataType', ''),
                'format_string': col.get('formatString', ''),
                'hidden': col.get('isHidden', False),
                'description': col.get('description', ''),
                'column_type': col.get('type', 'data'),
                'sort_by_column': col.get('sortByColumn', ''),
                'summarize_by': col.get('summarizeBy', ''),
                'is_key': col.get('isKey', False),
                'is_nullable': col.get('isNullable', True),
                'is_unique': col.get('isUnique', False),
                'dax_expression': col.get('expression', ''),
                'source_column': col.get('sourceColumn', ''),
                'display_folder': col.get('displayFolder', ''),
                'annotations': col.get('annotations', []),
                'lineage_tag': col.get('lineageTag', ''),
            }
            table['columns'].append(col_info)
            if col_info['column_type'] == 'calculated':
                self.metadata['calculated_columns'].append(col_info)
            else:
                self.metadata['columns'].append(col_info)

        for meas in tbl.get('measures', []):
            m = {
                'table': table['name'],
                'name': meas.get('name', ''),
                'expression': meas.get('expression', ''),
                'format_string': meas.get('formatString', ''),
                'display_folder': meas.get('displayFolder', ''),
                'description': meas.get('description', ''),
                'hidden': meas.get('isHidden', False),
                'annotations': meas.get('annotations', []),
                'lineage_tag': meas.get('lineageTag', ''),
                'data_type': meas.get('dataType', ''),
                'kpi': meas.get('kpi', {}),
            }
            table['measures'].append(m)
            self.metadata['measures'].append(m)

        for hier in tbl.get('hierarchies', []):
            h = {
                'table': table['name'],
                'name': hier.get('name', ''),
                'description': hier.get('description', ''),
                'hidden': hier.get('isHidden', False),
                'levels': [],
            }
            for lvl in hier.get('levels', []):
                h['levels'].append({
                    'ordinal': lvl.get('ordinal', 0),
                    'name': lvl.get('name', ''),
                    'column': lvl.get('column', ''),
                })
            table['hierarchies'].append(h)
            self.metadata['hierarchies'].append(h)

        has_calculated_partition = any(
            p['source_type'] in ('calculated', 'calculatedTable')
            for p in table['partitions']
        )
        if has_calculated_partition and table['partitions']:
            expr = table['partitions'][0].get('m_expression', '')
            if isinstance(expr, list):
                expr = '\n'.join(expr)
            self.metadata['calculated_tables'].append({
                'name': table['name'],
                'expression': expr,
            })

        self.metadata['tables'].append(table)

    def _parse_relationship(self, rel: dict):
        r = {
            'name': rel.get('name', ''),
            'from_table': rel.get('fromTable', rel.get('fromTableId', '')),
            'from_column': rel.get('fromColumn', ''),
            'to_table': rel.get('toTable', rel.get('toTableId', '')),
            'to_column': rel.get('toColumn', ''),
            'cardinality': rel.get('fromCardinality', '') + ':' + rel.get('toCardinality', ''),
            'cross_filtering_behavior': rel.get('crossFilteringBehavior', 'oneDirection'),
            'is_active': rel.get('isActive', True),
            'assume_referential_integrity': rel.get('assumeReferentialIntegrity', False),
            'security_filtering_behavior': rel.get('securityFilteringBehavior', ''),
        }
        self.metadata['relationships'].append(r)

    def _extract_model_with_regex_deep(self, text_utf8: str, text_utf16: str):
        """
        Deep regex extraction: pull tables, columns, measures, relationships
        directly from the raw text when JSON parsing fails.
        """
        print("  Using deep regex extraction for DataModel...")
        found_something = False

        for text in [text_utf8, text_utf16]:
            if not text or len(text) < 100:
                continue

            # ── Extract measure blocks ────────────────────────────────────────
            # Pattern: {"name":"MeasureName",...,"expression":"DAX expression",...}
            # Try multiple orderings of name/expression
            measure_pattern = re.compile(
                r'[{][^{}]{0,500}?"name"\s*:\s*"([^"]+)"[^{}]{0,2000}?"expression"\s*:\s*"([^"]{0,5000}?)"[^{}]{0,500}?[}]',
                re.DOTALL
            )
            for m in measure_pattern.finditer(text):
                name, expr = m.group(1), m.group(2)
                if len(expr) > 3 and any(kw in expr.upper() for kw in [
                    'CALCULATE', 'SUM(', 'COUNT', 'FILTER', 'IF(', 'DIVIDE',
                    'SUMX', 'AVERAGE', 'MAX(', 'MIN(', 'RELATED', 'ALL(',
                    'DISTINCTCOUNT', 'COUNTA', 'VALUES(', 'SWITCH',
                ]):
                    existing = [m['name'] for m in self.metadata['measures']]
                    if name not in existing:
                        self.metadata['measures'].append({
                            'table': 'Unknown',
                            'name': name,
                            'expression': expr.replace('\\"', '"').replace('\\n', '\n'),
                            'format_string': '',
                            'display_folder': '',
                            'description': '',
                            'hidden': False,
                            'inferred': True,
                        })
                        found_something = True

            # ── Extract table names from "name":"TableName" near "columns" ────
            table_pattern = re.compile(
                r'"name"\s*:\s*"([^"]+)"[^{]{0,500}?"columns"\s*:\s*\[',
                re.DOTALL
            )
            existing_table_names = {t['name'] for t in self.metadata['tables']}
            for m in table_pattern.finditer(text):
                tbl_name = m.group(1)
                if (tbl_name and tbl_name not in existing_table_names and
                        len(tbl_name) < 100 and not tbl_name.startswith('{')):
                    self.metadata['tables'].append({
                        'name': tbl_name,
                        'description': '',
                        'hidden': False,
                        'storage_mode': '',
                        'columns': [],
                        'measures': [],
                        'partitions': [],
                        'hierarchies': [],
                        'inferred': True,
                    })
                    existing_table_names.add(tbl_name)
                    found_something = True

            # ── Extract relationships ─────────────────────────────────────────
            rel_pattern = re.compile(
                r'"fromTable"\s*:\s*"([^"]+)"[^}]{0,300}?"fromColumn"\s*:\s*"([^"]+)"'
                r'[^}]{0,300}?"toTable"\s*:\s*"([^"]+)"[^}]{0,300}?"toColumn"\s*:\s*"([^"]+)"',
                re.DOTALL
            )
            for m in rel_pattern.finditer(text):
                from_tbl, from_col, to_tbl, to_col = m.groups()
                existing_rels = [(r['from_table'], r['from_column']) for r in self.metadata['relationships']]
                if (from_tbl, from_col) not in existing_rels:
                    self.metadata['relationships'].append({
                        'name': '',
                        'from_table': from_tbl,
                        'from_column': from_col,
                        'to_table': to_tbl,
                        'to_column': to_col,
                        'cardinality': ':',
                        'cross_filtering_behavior': '',
                        'is_active': True,
                        'assume_referential_integrity': False,
                        'inferred': True,
                    })
                    found_something = True

        if found_something:
            print(f"  Deep regex: {len(self.metadata['tables'])} tables, "
                  f"{len(self.metadata['measures'])} measures, "
                  f"{len(self.metadata['relationships'])} relationships")

    # ── Build schema from _field_inventory (populated during visual parsing) ────
    def _infer_schema_from_visuals(self):
        """
        Merge the _field_inventory (built by _walk_expr_for_fields during layout parsing)
        and diagram table nodes into the tables/columns/measures metadata.
        The inventory uses correct alias resolution, so entity names are authoritative.
        """
        inv = self.metadata.pop('_field_inventory', {})

        existing_table_names = {t['name'] for t in self.metadata['tables']}
        new_tables_added = 0

        # Sort tables: dimension/fact tables first, measures tables (starting with _) last
        def table_sort_key(name):
            return (1 if name.startswith('_') else 0, name.lower())

        for tbl_name in sorted(inv.keys(), key=table_sort_key):
            info = inv[tbl_name]
            columns = sorted(info.get('columns', set()))
            measures = sorted(info.get('measures', set()))
            hierarchies = info.get('hierarchies', {})

            # Classify: tables whose name starts with _ are measures tables
            is_measures_table = tbl_name.startswith('_')

            if tbl_name in existing_table_names:
                tbl_entry = next(t for t in self.metadata['tables'] if t['name'] == tbl_name)
            else:
                tbl_entry = {
                    'name': tbl_name,
                    'description': '',
                    'hidden': False,
                    'storage_mode': 'Live/Service',
                    'columns': [],
                    'measures': [],
                    'partitions': [],
                    'hierarchies': [],
                    'inferred': True,
                }
                self.metadata['tables'].append(tbl_entry)
                existing_table_names.add(tbl_name)
                new_tables_added += 1

            existing_col_names = {c['name'] for c in tbl_entry.get('columns', [])}
            existing_meas_names = {m['name'] for m in tbl_entry.get('measures', [])}

            # If it's a measures table, all fields are measures
            for field_name in columns:
                if is_measures_table:
                    if field_name not in existing_meas_names:
                        m_entry = {
                            'table': tbl_name, 'name': field_name,
                            'expression': '(DAX stored in Power BI Service dataset)',
                            'format_string': '', 'display_folder': '',
                            'description': '', 'hidden': False, 'inferred': True,
                        }
                        tbl_entry['measures'].append(m_entry)
                        self.metadata['measures'].append(m_entry)
                        existing_meas_names.add(field_name)
                else:
                    if field_name not in existing_col_names:
                        c_entry = {
                            'table': tbl_name, 'name': field_name,
                            'data_type': '', 'format_string': '',
                            'hidden': False, 'description': '',
                            'column_type': 'data', 'inferred': True,
                        }
                        tbl_entry['columns'].append(c_entry)
                        self.metadata['columns'].append(c_entry)
                        existing_col_names.add(field_name)

            for field_name in measures:
                if field_name not in existing_meas_names:
                    m_entry = {
                        'table': tbl_name, 'name': field_name,
                        'expression': '(DAX stored in Power BI Service dataset)',
                        'format_string': '', 'display_folder': '',
                        'description': '', 'hidden': False, 'inferred': True,
                    }
                    tbl_entry['measures'].append(m_entry)
                    self.metadata['measures'].append(m_entry)
                    existing_meas_names.add(field_name)

            # Hierarchies
            for hier_name, levels in hierarchies.items():
                existing_hier_names = {h['name'] for h in tbl_entry.get('hierarchies', [])}
                if hier_name not in existing_hier_names:
                    h = {
                        'table': tbl_name,
                        'name': hier_name,
                        'description': '',
                        'hidden': False,
                        'levels': [{'ordinal': i, 'name': lvl, 'column': lvl}
                                   for i, lvl in enumerate(sorted(levels))],
                        'inferred': True,
                    }
                    tbl_entry['hierarchies'].append(h)
                    self.metadata['hierarchies'].append(h)

        # Add diagram-only tables (visible in model view but not used in any visual)
        for dt in self.metadata.get('diagram_tables', []):
            if dt['name'] not in existing_table_names:
                self.metadata['tables'].append({
                    'name': dt['name'],
                    'description': '',
                    'hidden': False,
                    'storage_mode': 'Live/Service',
                    'columns': [], 'measures': [],
                    'partitions': [], 'hierarchies': [],
                    'inferred': True,
                    'diagram_only': True,
                })
                existing_table_names.add(dt['name'])
                new_tables_added += 1

        # Add table co-occurrence hints as relationship suggestions
        self._infer_relationships_from_co_occurrence()

        total_measures = len(self.metadata['measures'])
        total_cols = len(self.metadata['columns'])
        total_tables = len(self.metadata['tables'])
        print(f"  Schema from visuals: {total_tables} tables, {total_cols} columns, {total_measures} measures")

    def _infer_relationships_from_co_occurrence(self):
        """
        When tables appear together in the same visual query, it strongly implies
        a relationship. Record these as 'suggested relationships' for the report.
        Uses display names for pages (not internal GUIDs).
        """
        from collections import defaultdict
        co_occur = defaultdict(set)

        # Build internal_name -> display_name map
        page_display = {p.get('name', ''): p.get('display_name', p.get('name', ''))
                        for p in self.metadata.get('pages', [])}

        for v in self.metadata.get('visuals', []):
            page = page_display.get(v.get('page', ''), v.get('page', ''))
            # Extract table names from data_fields: "Table[Field]" or "Agg(Table[Field])" format
            tables_in_visual = set()
            for field in v.get('data_fields', []):
                # Strip aggregation wrapper: Sum(Table[col]) -> Table[col]
                clean = re.sub(r'^[A-Za-z]+\(', '', field).rstrip(')')
                bracket = re.match(r'^([^\[]+)\[', clean)
                if bracket:
                    tbl = bracket.group(1).strip()
                    if len(tbl) < 80 and not tbl.startswith('('):
                        tables_in_visual.add(tbl)
            tables_list = sorted(tables_in_visual)
            for i, t1 in enumerate(tables_list):
                for t2 in tables_list[i+1:]:
                    pair = tuple(sorted([t1, t2]))
                    co_occur[pair].add(page)

        if not co_occur:
            return

        suggested = []
        for pair, pages in sorted(co_occur.items(), key=lambda x: -len(x[1])):
            suggested.append({
                'tables': list(pair),
                'used_together_on_pages': sorted(pages),
                'note': 'Tables used in the same visual — likely related',
            })

        if suggested:
            self.metadata['relationship_suggestions'] = suggested
            print(f"  Identified {len(suggested)} table co-occurrence relationship hints")

    # ═══════════════════════════════════════════════════════════════════════
    # FILTER DECODING ENGINE
    # ═══════════════════════════════════════════════════════════════════════

    def _decode_filter_condition(self, cond: dict, alias_map: dict) -> str:
        """Recursively decode a filter Condition dict into a human-readable string."""
        if not cond or not isinstance(cond, dict):
            return ''

        def resolve(col_expr):
            sr = col_expr.get('Expression', {}).get('SourceRef', {})
            src = sr.get('Source', sr.get('Entity', ''))
            entity = alias_map.get(src, src)
            prop = col_expr.get('Property', '')
            return f"{entity}[{prop}]" if entity else prop

        def lit_val(expr):
            v = expr.get('Literal', {}).get('Value', '')
            return v.strip("'\"" ) if v else str(expr)[:30]

        # Comparison: =, <, >, etc.
        comp = cond.get('Comparison')
        if comp:
            ops = {0: '=', 1: '>', 2: '>=', 3: '<', 4: '<=', 5: '!='}
            left = comp.get('Left', {})
            right = comp.get('Right', {})
            col = left.get('Column', left.get('Measure', {}))
            field = resolve(col) if col else str(left)[:30]
            val = lit_val(right)
            return f"{field} {ops.get(comp.get('ComparisonKind', 0), '?')} {val}"

        # In: field IN (values)
        in_expr = cond.get('In')
        if in_expr:
            exprs = in_expr.get('Expressions', [])
            values = in_expr.get('Values', [])
            fields = []
            for e in exprs:
                col = e.get('Column', e.get('Measure', {}))
                fields.append(resolve(col) if col else str(e)[:20])
            vals = []
            for v in values:
                if isinstance(v, list) and v:
                    vals.append(lit_val(v[0]))
                elif isinstance(v, dict):
                    vals.append(lit_val(v))
            val_str = ', '.join(vals[:6]) + ('...' if len(vals) > 6 else '')
            return f"{' & '.join(fields)} IN ({val_str})"

        # Not
        not_expr = cond.get('Not')
        if not_expr:
            inner = self._decode_filter_condition(not_expr.get('Expression', {}), alias_map)
            return f"NOT ({inner})" if inner else 'NOT (...)'

        # And
        and_expr = cond.get('And')
        if and_expr:
            left = self._decode_filter_condition(and_expr.get('Left', {}), alias_map)
            right = self._decode_filter_condition(and_expr.get('Right', {}), alias_map)
            return f"({left}) AND ({right})" if left and right else left or right

        # Or
        or_expr = cond.get('Or')
        if or_expr:
            left = self._decode_filter_condition(or_expr.get('Left', {}), alias_map)
            right = self._decode_filter_condition(or_expr.get('Right', {}), alias_map)
            return f"({left}) OR ({right})" if left and right else left or right

        # Between
        between = cond.get('Between')
        if between:
            col = between.get('Expression', {}).get('Column', between.get('Expression', {}).get('Measure', {}))
            field = resolve(col)
            lower = lit_val(between.get('LowerBound', {}).get('Literal', between.get('LowerBound', {})))
            upper = lit_val(between.get('UpperBound', {}).get('Literal', between.get('UpperBound', {})))
            return f"{field} BETWEEN {lower} AND {upper}"

        # Contains
        contains = cond.get('Contains')
        if contains:
            col = contains.get('Left', {}).get('Column', {})
            field = resolve(col)
            val = lit_val(contains.get('Right', {}).get('Literal', contains.get('Right', {})))
            return f"{field} CONTAINS '{val}'"

        return ''

    def _decode_filters_list(self, filters: list, alias_map: dict) -> list:
        """Decode a list of filter objects into human-readable descriptions."""
        results = []
        HOW_CREATED = {0: 'user', 1: 'fixed', 2: 'drillthrough', 3: 'drilldown', 5: 'cross-filter'}
        for f in filters:
            if not isinstance(f, dict):
                continue
            expr = f.get('expression', {})
            col_expr = expr.get('Column', expr.get('Measure', {}))
            sr = col_expr.get('Expression', {}).get('SourceRef', {})
            entity = alias_map.get(sr.get('Source', ''), sr.get('Source', sr.get('Entity', '')))
            prop = col_expr.get('Property', '')
            ftype = f.get('type', '')
            how = HOW_CREATED.get(f.get('howCreated', 0), str(f.get('howCreated', '')))

            where = f.get('filter', {}).get('Where', [])
            # Build alias map from filter's own From clause if present
            filter_alias = dict(alias_map)
            for src in f.get('filter', {}).get('From', []):
                filter_alias[src.get('Name', '')] = src.get('Entity', '')

            conditions = []
            for w in where:
                decoded = self._decode_filter_condition(w.get('Condition', {}), filter_alias)
                if decoded:
                    conditions.append(decoded)

            field_ref = f"{entity}[{prop}]" if entity and prop else (entity or prop or '?')
            cond_str = ' AND '.join(conditions) if conditions else '(active — no condition decoded)'
            is_inverted = f.get('objects', {}).get('general', [{}])[0].get('properties', {}).get(
                'isInvertedSelectionMode', {}).get('expr', {}).get('Literal', {}).get('Value', '') == 'true'

            results.append({
                'field': field_ref,
                'type': ftype,
                'how': how,
                'condition': cond_str,
                'inverted': is_inverted,
                'display': f"{field_ref}: {cond_str}" + (' [inverted]' if is_inverted else ''),
            })
        return results

    # ═══════════════════════════════════════════════════════════════════════
    # SLICER DETAIL EXTRACTOR
    # ═══════════════════════════════════════════════════════════════════════

    def _parse_slicer_details(self, sv: dict, alias_map: dict) -> dict:
        """Extract detailed slicer configuration."""
        objects = sv.get('objects', {})
        data_props = objects.get('data', [{}])[0].get('properties', {}) if objects.get('data') else {}

        def lit(expr_dict):
            import re as _r
            v = expr_dict.get('expr', {}).get('Literal', {}).get('Value', '').strip("'\"" )
            # Strip Power BI date wrappers: datetime'2025-01-01T...' -> 2025-01-01
            dm = _r.search(r'(\d{4}-\d{2}-\d{2})', v)
            if dm:
                return dm.group(1)
            return v

        mode = lit(data_props.get('mode', {})) or 'List'
        rel_range = lit(data_props.get('relativeRange', {}))
        rel_duration = lit(data_props.get('relativeDuration', {}))
        start_date = lit(data_props.get('startDate', {}))
        end_date = lit(data_props.get('endDate', {}))

        slider_props = objects.get('slider', [{}])[0].get('properties', {}) if objects.get('slider') else {}
        show_slider = lit(slider_props.get('show', {}))

        # Determine slicer type label
        def clean_dt(s):
            # Strip datetime'' wrapper: datetime'2025-11-01T00:00:00' -> 2025-11-01
            import re as _re
            m = _re.search(r'(\d{4}-\d{2}-\d{2})', s)
            return m.group(1) if m else s[:10]

        if mode == 'Between' or start_date:
            slicer_type = 'Date Range (Between)'
            sd_clean = clean_dt(start_date) if start_date else ''
            ed_clean = clean_dt(end_date) if end_date else ''
            if sd_clean and ed_clean:
                default_val = f"{sd_clean} to {ed_clean}"
            elif sd_clean:
                default_val = f"From {sd_clean}"
            else:
                default_val = 'No default set'
        elif mode == 'Basic' and rel_range:
            period = lit(data_props.get('relativePeriod', {})).capitalize() or 'periods'
            dur_num = rel_duration.rstrip('D').rstrip('M').rstrip('Y') if rel_duration else ''
            slicer_type = 'Relative Date'
            default_val = f"{rel_range} {dur_num} {period}".strip() if dur_num and dur_num != '0' else f"{rel_range} {period}"
        elif mode == 'Dropdown':
            slicer_type = 'Dropdown'
            default_val = 'All (no default)'
        elif mode == 'List' or not mode:
            slicer_type = 'List'
            default_val = 'All (no default)'
        else:
            slicer_type = mode
            default_val = 'Unknown'

        import re as _re2
        def _clean_dt(s):
            m = _re2.search(r'(\d{4}-\d{2}-\d{2})', s)
            return m.group(1) if m else s[:10]
        return {
            'slicer_type': slicer_type,
            'mode': mode,
            'default_value': default_val,
            'show_slider': show_slider,
            'relative_range': rel_range,
            'relative_duration': rel_duration,
            'start_date': _clean_dt(start_date) if start_date else '',
            'end_date': _clean_dt(end_date) if end_date else '',
        }

    # ═══════════════════════════════════════════════════════════════════════
    # FIELD ROLES EXTRACTOR (projections)
    # ═══════════════════════════════════════════════════════════════════════

    def _parse_field_roles(self, sv: dict, alias_map: dict) -> dict:
        """Extract field-role assignments (Y axis, Category, Legend, Values, etc.)"""
        projections = sv.get('projections', {})
        if not projections:
            return {}

        pq = sv.get('prototypeQuery', {})
        # Build queryRef -> human label map from Select
        qr_map = {}
        for sel in pq.get('Select', []):
            qr = sel.get('Name', sel.get('NativeReferenceName', ''))
            label = self._format_select_item(sel, alias_map)
            if qr:
                qr_map[qr] = label

        roles = {}
        for role, items in projections.items():
            fields = []
            for item in items:
                qr = item.get('queryRef', '')
                fields.append(qr_map.get(qr, qr))
            if fields:
                roles[role] = fields
        return roles

    # ═══════════════════════════════════════════════════════════════════════
    # THEME EXTRACTOR
    # ═══════════════════════════════════════════════════════════════════════

    def _extract_theme_details(self):
        """Read theme JSON from static resources."""
        theme_data = {}
        for fname, raw in self.raw_files.items():
            if 'BaseThemes' in fname and fname.endswith('.json'):
                try:
                    td = json.loads(raw.decode('utf-8', errors='replace'))
                    theme_data['name'] = td.get('name', '')
                    theme_data['data_colors'] = td.get('dataColors', [])
                    theme_data['background'] = td.get('background', '')
                    theme_data['foreground'] = td.get('foreground', '')
                    theme_data['table_accent'] = td.get('tableAccent', '')
                    theme_data['source_file'] = fname
                    break
                except Exception:
                    pass
        if theme_data:
            self.metadata['theme_details'] = theme_data

    # ═══════════════════════════════════════════════════════════════════════
    # LLM BRIEFING GENERATOR
    # ═══════════════════════════════════════════════════════════════════════

    def _generate_llm_briefing(self):
        """Synthesise all extracted metadata into a structured plain-English briefing."""
        fi = self.metadata['file_info']
        pages = self.metadata.get('pages', [])
        tables = self.metadata.get('tables', [])
        measures = self.metadata.get('measures', [])
        slicers = self.metadata.get('slicer_inventory', [])
        filters = self.metadata.get('filter_inventory', {})
        conn_type = fi.get('connection_type', '')

        L = []  # lines buffer
        def a(s): L.append(s)

        a("# POWER BI REPORT — LLM CONTEXT BRIEFING")
        a("=" * 60)
        a(f"File: {fi.get('filename', '')}")
        a(f"Size: {fi.get('size_mb', 0)} MB  |  Pages: {len(pages)}  |  Tables: {len(tables)}  |  Measures: {len(measures)}")
        a("")

        if conn_type == 'PowerBIServiceLive':
            ds = self.metadata.get('data_sources', [{}])[0]
            a("CONNECTION TYPE: Live Connection to Power BI Service Dataset")
            a(f"Dataset ID: {ds.get('dataset_id', 'unknown')}")
            a(f"Workspace ID: {ds.get('workspace_id', 'unknown')}")
            a("NOTE: DAX measure formulas are stored in the service dataset, not in this .pbix file.")
            a("      Table/column/measure names extracted below are complete and authoritative.")
            a("")

        # DATA MODEL
        a("## DATA MODEL")
        a("-" * 40)
        for tbl in sorted(tables, key=lambda t: (t.get('name','').startswith('_'), t.get('name','').lower())):
            name = tbl.get('name', '')
            cols = [c['name'] for c in tbl.get('columns', [])]
            meas_list = [m['name'] for m in tbl.get('measures', [])]
            hiers = tbl.get('hierarchies', [])
            note = ' [measures table]' if name.startswith('_') else ''
            if tbl.get('diagram_only'):
                note += ' [in model diagram — not directly used in any visual]'
            a(f"")
            a(f"Table: {name}{note}")
            if cols:
                a(f"  Columns ({len(cols)}): {', '.join(cols)}")
            if meas_list:
                a(f"  Measures ({len(meas_list)}): {', '.join(meas_list)}")
            if hiers:
                hier_detail = []
                for h in hiers:
                    lvls = [lv['name'] for lv in h.get('levels', [])]
                    hier_detail.append(f"{h['name']} ({' > '.join(lvls)})")
                a(f"  Hierarchies: {', '.join(hier_detail)}")

        # RELATIONSHIPS
        a("")
        a("## RELATIONSHIPS")
        a("-" * 40)
        rels = self.metadata.get('relationships', [])
        suggestions = self.metadata.get('relationship_suggestions', [])
        if rels:
            for r in rels:
                active = '' if r.get('is_active', True) else ' [inactive]'
                a(f"  {r['from_table']}[{r['from_column']}] -> {r['to_table']}[{r['to_column']}]"
                  f"  ({r.get('cardinality', '')} {r.get('cross_filtering_behavior', '')}){active}")
        elif suggestions:
            a("  Explicit relationships not stored in this .pbix (live connection).")
            a("  Table co-occurrence strongly indicates these relationships exist:")
            for s in suggestions:
                a(f"  {s['tables'][0]} <-> {s['tables'][1]}"
                  f"  (used together on: {', '.join(s['used_together_on_pages'][:4])})")
        else:
            a("  No relationships found.")

        # SLICERS
        a("")
        a("## SLICERS (user filter controls)")
        a("-" * 40)
        if slicers:
            by_field = defaultdict(list)
            for s in slicers:
                by_field[s['field']].append(s)
            for field, field_slicers in sorted(by_field.items()):
                s0 = field_slicers[0]
                page_list = ', '.join(s['page'] for s in field_slicers)
                sync = f" -- on {len(field_slicers)} pages: {page_list}" if len(field_slicers) > 1 else f" -- page: {page_list}"
                a(f"  Field: {field}")
                a(f"    Type: {s0['slicer_type']}  |  Default: {s0['default_value']}{sync}")
        else:
            a("  No slicers found.")

        # REPORT-LEVEL FILTERS
        report_filters = filters.get('report', [])
        if report_filters:
            a("")
            a("## REPORT-LEVEL FILTERS (apply to ALL pages)")
            a("-" * 40)
            for f in report_filters:
                if f.get('field'):
                    a(f"  {f['display']}")

        # PAGE-LEVEL FILTERS
        page_filters = filters.get('pages', {})
        if any(page_filters.values()):
            a("")
            a("## PAGE-LEVEL FILTERS")
            a("-" * 40)
            for page_name, pf_list in sorted(page_filters.items()):
                if pf_list:
                    a(f"  [{page_name}]:")
                    for f in pf_list:
                        if f.get('field'):
                            a(f"    {f['display']}")

        # PAGE SUMMARIES
        a("")
        a("## REPORT PAGES -- COMPLETE BREAKDOWN")
        a("-" * 40)
        for page in pages:
            pname = page.get('display_name', page.get('name', ''))
            visuals = page.get('visuals', [])
            hidden = ' [HIDDEN]' if page.get('hidden') else ''
            a("")
            a(f"### Page: {pname}{hidden}  ({page.get('width',0)}x{page.get('height',0)}px, {len(visuals)} visuals)")

            narrative = page.get('narrative', '')
            if narrative:
                a(f"SUMMARY: {narrative}")

            data_vs = [v for v in visuals if v.get('visual_type') not in
                      ('textbox', 'image', 'shape', 'basicShape', 'actionButton')]
            chrome_count = len(visuals) - len(data_vs)

            if data_vs:
                a(f"Data Visuals ({len(data_vs)}):")
                for v in data_vs:
                    vtype = v.get('visual_type', '')
                    title = v.get('title', '') or '(no title)'
                    roles = v.get('field_roles', {})
                    w, h = int(v.get('width',0)), int(v.get('height',0))
                    x, y = int(v.get('x',0)), int(v.get('y',0))

                    if vtype == 'slicer':
                        sd = v.get('slicer_details', {})
                        fields = ', '.join(v.get('data_fields', []))
                        a(f"  [{vtype}] '{title}'  ({w}x{h} at {x},{y})")
                        a(f"    Field: {fields} | Type: {sd.get('slicer_type','')} | Default: {sd.get('default_value','')}")
                    elif roles:
                        a(f"  [{vtype}] '{title}'  ({w}x{h} at {x},{y})")
                        for role, role_fields in roles.items():
                            a(f"    {role}: {', '.join(role_fields)}")
                        decoded = v.get('decoded_filters', [])
                        active_vf = [f for f in decoded if 'IN' in f.get('condition','') or '=' in f.get('condition','')]
                        if active_vf:
                            a(f"    Visual filters: {' | '.join(f['display'] for f in active_vf[:3])}")
                    else:
                        fields_str = ', '.join(v.get('data_fields', []))[:120]
                        a(f"  [{vtype}] '{title}': {fields_str}  ({w}x{h} at {x},{y})")

            if chrome_count:
                a(f"  Layout elements: {chrome_count} (textboxes, images, shapes)")

        # VISUAL TYPE INVENTORY
        a("")
        a("## VISUAL TYPE INVENTORY")
        a("-" * 40)
        vtype_count = defaultdict(int)
        for v in self.metadata.get('visuals', []):
            vtype_count[v.get('visual_type', 'unknown')] += 1
        for vtype, count in sorted(vtype_count.items(), key=lambda x: -x[1]):
            a(f"  {vtype}: {count}")

        # GUIDANCE
        a("")
        a("## GUIDANCE FOR MAKING CHANGES")
        a("-" * 40)
        a("When modifying this report, note:")
        a(f"  - {len(slicers)} slicers control data filtering. Check sync requirements before adding pages.")
        for s in slicers:
            if s.get('start_date') or s.get('end_date'):
                a(f"  - Slicer on {s['field']} has HARDCODED date range: {s.get('start_date','')} to {s.get('end_date','')}")
        table_pages = [p.get('display_name','') for p in pages
                      if any(v.get('visual_type')=='tableEx' for v in p.get('visuals',[]))]
        if table_pages:
            a(f"  - Detail tables (tableEx) present on: {', '.join(table_pages)}")
        if report_filters:
            a(f"  - {len(report_filters)} report-level filters are active across all pages.")
        a("  - When adding a new page, replicate the slicer pattern from existing pages.")
        a("  - Visual filters with hardcoded conditions are documented in Section 4.")

        self.metadata['llm_briefing'] = '\n'.join(L)
        print(f"  LLM briefing generated ({len(L)} lines, {len(self.metadata['llm_briefing'])} chars)")

    # PAGE NARRATIVE GENERATOR
    # ═══════════════════════════════════════════════════════════════════════

    def _generate_page_narratives(self):
        """Generate a plain-English summary sentence for each page."""
        VISUAL_LABELS = {
            'card': 'KPI card', 'multiRowCard': 'multi-row KPI card',
            'tableEx': 'detail table', 'barChart': 'bar chart',
            'clusteredColumnChart': 'column chart', 'columnChart': 'column chart',
            'lineChart': 'line chart', 'pieChart': 'pie chart',
            'donutChart': 'donut chart', 'slicer': 'slicer',
            'textbox': 'text label', 'image': 'image',
            'shape': 'shape', 'basicShape': 'shape',
            'actionButton': 'navigation button', 'gauge': 'gauge',
            'kpiVisual': 'KPI visual', 'ribbonChart': 'ribbon chart',
            'waterfallChart': 'waterfall chart', 'scatterChart': 'scatter chart',
        }

        for page in self.metadata.get('pages', []):
            visuals = page.get('visuals', [])
            pname = page.get('display_name', '')

            # Count by type
            type_counts = defaultdict(int)
            for v in visuals:
                type_counts[v.get('visual_type', 'unknown')] += 1

            # Collect KPI card measures
            card_measures = []
            for v in visuals:
                if v.get('visual_type') in ('card', 'multiRowCard'):
                    for f in v.get('data_fields', []):
                        m = re.search(r'\[([^\]]+)\]', f)
                        if m:
                            card_measures.append(m.group(1))

            # Collect slicer fields
            slicer_fields = []
            for v in visuals:
                if v.get('visual_type') == 'slicer':
                    for f in v.get('data_fields', []):
                        m = re.search(r'\[([^\]]+)\]', f)
                        if m:
                            slicer_fields.append(m.group(1))

            # Build narrative
            data_visuals = {k: v for k, v in type_counts.items()
                           if k not in ('textbox', 'image', 'shape', 'basicShape', 'actionButton')}
            chrome_count = sum(type_counts.get(t, 0)
                              for t in ('textbox', 'image', 'shape', 'basicShape', 'actionButton'))

            parts = []
            for vtype, count in sorted(data_visuals.items(), key=lambda x: -x[1]):
                label = VISUAL_LABELS.get(vtype, vtype)
                parts.append(f"{count} {label}{'s' if count > 1 else ''}")

            narrative = f"{pname} page"
            if parts:
                narrative += f": {', '.join(parts)}"
            if card_measures:
                unique_measures = list(dict.fromkeys(card_measures))[:5]
                narrative += f". Key metrics: {', '.join(unique_measures)}"
            if slicer_fields:
                narrative += f". User filters: {', '.join(slicer_fields)}"
            if chrome_count:
                narrative += f". Plus {chrome_count} layout elements (text, images, shapes)"

            page['narrative'] = narrative

    # ═══════════════════════════════════════════════════════════════════════
    # SLICER & FILTER INVENTORY BUILDERS
    # ═══════════════════════════════════════════════════════════════════════

    def _build_slicer_and_filter_inventory(self):
        """Walk all pages and build flat slicer_inventory and filter_inventory."""
        slicer_inventory = []
        filter_inventory = {'report': [], 'pages': {}, 'visual_summary': []}

        # Report-level filters
        raw_report_filters = self.metadata.get('_raw_report_filters', [])
        filter_inventory['report'] = self._decode_filters_list(raw_report_filters, {})

        for page in self.metadata.get('pages', []):
            pname = page.get('display_name', page.get('name', ''))

            # Page-level filters
            raw_pf = page.get('_raw_page_filters', [])
            filter_inventory['pages'][pname] = self._decode_filters_list(raw_pf, {})

            for v in page.get('visuals', []):
                if v.get('visual_type') == 'slicer':
                    sd = v.get('slicer_details', {})
                    fields = v.get('data_fields', [])
                    slicer_inventory.append({
                        'page': pname,
                        'title': v.get('title', ''),
                        'field': ', '.join(fields),
                        'slicer_type': sd.get('slicer_type', ''),
                        'mode': sd.get('mode', ''),
                        'default_value': sd.get('default_value', ''),
                        'start_date': sd.get('start_date', ''),
                        'end_date': sd.get('end_date', ''),
                        'relative_range': sd.get('relative_range', ''),
                        'show_slider': sd.get('show_slider', ''),
                    })

                # Visual filter summary (active conditions only)
                decoded = v.get('decoded_filters', [])
                active = [f for f in decoded if 'IN' in f.get('condition', '') or
                         '=' in f.get('condition', '') or 'NOT' in f.get('condition', '')]
                if active and v.get('visual_type') not in ('slicer',):
                    filter_inventory['visual_summary'].append({
                        'page': pname,
                        'visual_type': v.get('visual_type', ''),
                        'title': v.get('title', ''),
                        'active_filters': active,
                    })

        self.metadata['slicer_inventory'] = slicer_inventory
        self.metadata['filter_inventory'] = filter_inventory
        print(f"  Built: {len(slicer_inventory)} slicers, "
              f"{len(filter_inventory['report'])} report filters, "
              f"{len(filter_inventory['visual_summary'])} visuals with active filters")

    # ═══════════════════════════════════════════════════════════════════════
    # CLEAN JSON EXPORT
    # ═══════════════════════════════════════════════════════════════════════

    def build_clean_json(self) -> dict:
        """
        Return a clean, well-structured dict ready for JSON serialisation.
        Strips all internal _raw_* keys, de-duplicates references between
        pages and visuals, and organises the output into logical sections.
        """
        import copy

        def clean_page(page: dict) -> dict:
            """Strip temp fields from a page dict and clean its visuals."""
            p = {k: v for k, v in page.items()
                 if not k.startswith('_') and k not in ('background', 'wallpaper', 'config')}
            p['visuals'] = [clean_visual(v) for v in page.get('visuals', [])]
            return p

        def clean_visual(v: dict) -> dict:
            """Return a concise visual dict with only useful fields."""
            out = {
                'visual_type': v.get('visual_type', ''),
                'title':       v.get('title', ''),
                'hidden':      v.get('hidden', False),
                'position':    {'x': v.get('x', 0), 'y': v.get('y', 0),
                                'width': v.get('width', 0), 'height': v.get('height', 0)},
                'data_fields': v.get('data_fields', []),
                'field_roles': v.get('field_roles', {}),
            }
            if v.get('visual_type') == 'slicer' and v.get('slicer_details'):
                out['slicer_details'] = v['slicer_details']
            if v.get('decoded_filters'):
                # Only keep filters that have a real condition
                active = [f for f in v['decoded_filters'] if f.get('field')]
                if active:
                    out['filters'] = [{'field': f['field'], 'type': f['type'],
                                       'condition': f['condition'], 'how': f.get('how',''),
                                       'inverted': f.get('inverted', False)}
                                      for f in active]
            return out

        def clean_table(t: dict) -> dict:
            return {
                'name':         t.get('name', ''),
                'storage_mode': t.get('storage_mode', ''),
                'hidden':       t.get('hidden', False),
                'description':  t.get('description', ''),
                'inferred':     t.get('inferred', False),
                'diagram_only': t.get('diagram_only', False),
                'columns':  [{'name': c.get('name',''), 'data_type': c.get('data_type',''),
                               'column_type': c.get('column_type',''), 'format': c.get('format_string',''),
                               'hidden': c.get('hidden', False), 'description': c.get('description','')}
                             for c in t.get('columns', [])],
                'measures': [{'name': m.get('name',''), 'expression': m.get('expression',''),
                               'format': m.get('format_string',''), 'folder': m.get('display_folder',''),
                               'hidden': m.get('hidden', False), 'description': m.get('description','')}
                             for m in t.get('measures', [])],
                'hierarchies': [{'name': h.get('name',''),
                                  'levels': [lv.get('name','') for lv in h.get('levels', [])]}
                                for h in t.get('hierarchies', [])],
                'partitions': t.get('partitions', []),
            }

        md = self.metadata

        # Build the filter inventory with clean structure
        fi = md.get('filter_inventory', {})
        filter_section = {
            'report_level': [{'field': f['field'], 'type': f['type'],
                               'condition': f['condition'], 'inverted': f.get('inverted', False)}
                             for f in fi.get('report', []) if f.get('field')],
            'page_level': {
                page: [{'field': f['field'], 'type': f['type'], 'condition': f['condition']}
                       for f in flist if f.get('field')]
                for page, flist in fi.get('pages', {}).items()
                if any(f.get('field') for f in flist)
            },
            'visual_hardcoded': [
                {
                    'page': vf['page'],
                    'visual_type': vf['visual_type'],
                    'title': vf.get('title', ''),
                    'filters': [{'field': f['field'], 'condition': f['condition']}
                                for f in vf.get('active_filters', [])],
                }
                for vf in fi.get('visual_summary', [])
            ],
        }

        # Connection/data source info
        conn = md.get('connections', {})
        ds_list = md.get('data_sources', [])

        return {
            # ── File & connection metadata ─────────────────────────────────
            'file_info': md.get('file_info', {}),
            'connection': {
                'type':         md.get('file_info', {}).get('connection_type', 'Import'),
                'sources':      ds_list,
                'power_query':  md.get('power_query', []),
                'parameters':   md.get('parameters', []),
            },

            # ── Data model ─────────────────────────────────────────────────
            'data_model': {
                'info':          md.get('data_model', {}),
                'tables':        [clean_table(t) for t in md.get('tables', [])],
                'relationships': md.get('relationships', []),
                'relationship_suggestions': md.get('relationship_suggestions', []),
                'rls_roles':     md.get('rls_roles', []),
                'perspectives':  md.get('perspectives', []),
                'translations':  md.get('translations', []),
            },

            # ── Report layout ──────────────────────────────────────────────
            'report': {
                'theme':    md.get('themes', {}),
                'pages':    [clean_page(p) for p in md.get('pages', [])],
                'bookmarks': md.get('bookmarks', []),
                'custom_visuals': md.get('custom_visuals', []),
            },

            # ── Interaction layer ──────────────────────────────────────────
            'slicers':          md.get('slicer_inventory', []),
            'filters':          filter_section,

            # ── Diagnostics & extras ───────────────────────────────────────
            'diagram_tables':   md.get('diagram_tables', []),
            'security':         md.get('rls_roles', []),
            'errors':           md.get('errors', []),
            'warnings':         md.get('warnings', []),

            # ── LLM context ───────────────────────────────────────────────
            'llm_briefing':     md.get('llm_briefing', ''),
        }



# ═══════════════════════════════════════════════════════════════════════════
# PDF REPORT GENERATOR
# ═══════════════════════════════════════════════════════════════════════════

class PDFReportGenerator:
    def __init__(self, metadata: dict, output_path: str):
        self.meta = metadata
        self.output_path = output_path
        self.story = []
        self._setup_styles()

    def _setup_styles(self):
        base_styles = getSampleStyleSheet()

        self.styles = {
            'title': ParagraphStyle('Title', parent=base_styles['Title'],
                fontSize=24, spaceAfter=12, textColor=colors.HexColor('#164d3d')),
            'h1': ParagraphStyle('H1', parent=base_styles['Heading1'],
                fontSize=16, spaceBefore=18, spaceAfter=8,
                textColor=colors.HexColor('#164d3d'),
                borderPad=4),
            'h2': ParagraphStyle('H2', parent=base_styles['Heading2'],
                fontSize=13, spaceBefore=12, spaceAfter=6,
                textColor=colors.HexColor('#1e7a5e')),
            'h3': ParagraphStyle('H3', parent=base_styles['Heading3'],
                fontSize=11, spaceBefore=8, spaceAfter=4,
                textColor=colors.HexColor('#2aaa7a')),
            'normal': ParagraphStyle('Normal', parent=base_styles['Normal'],
                fontSize=9, spaceAfter=3, leading=13),
            'small': ParagraphStyle('Small', parent=base_styles['Normal'],
                fontSize=8, spaceAfter=2, leading=11),
            'code': ParagraphStyle('Code', parent=base_styles['Code'],
                fontSize=7.5, fontName='Courier', spaceAfter=4,
                backColor=colors.HexColor('#f4faf8'),
                borderPad=4, leading=11),
            'label': ParagraphStyle('Label', parent=base_styles['Normal'],
                fontSize=8.5, textColor=colors.HexColor('#2d6b58'), spaceAfter=1),
            'toc_entry': ParagraphStyle('TOC', parent=base_styles['Normal'],
                fontSize=10, spaceAfter=4, leftIndent=0),
            'meta_key': ParagraphStyle('MetaKey', parent=base_styles['Normal'],
                fontSize=9, fontName='Helvetica-Bold', spaceAfter=1),
            'badge': ParagraphStyle('Badge', parent=base_styles['Normal'],
                fontSize=8, backColor=colors.HexColor('#edf7f3'),
                textColor=colors.HexColor('#1e7a5e'), spaceAfter=2),
        }

    def _add_hr(self, thickness=0.5, color=colors.HexColor('#b2d8ca')):
        self.story.append(HRFlowable(width='100%', thickness=thickness, color=color, spaceAfter=6))

    def _add_h1(self, text):
        self.story.append(Paragraph(text, self.styles['h1']))
        self._add_hr(1.0, colors.HexColor('#1e7a5e'))

    def _add_h2(self, text):
        self.story.append(Spacer(1, 4))
        self.story.append(Paragraph(text, self.styles['h2']))

    def _add_h3(self, text):
        self.story.append(Paragraph(text, self.styles['h3']))

    def _add_p(self, text, style='normal'):
        """Render plain text (will be XML-escaped automatically)."""
        safe = self._safe(str(text))
        self.story.append(Paragraph(safe, self.styles[style]))

    def _add_html(self, html, style='normal'):
        """Render pre-formed HTML directly — do NOT pass user data here without escaping values first."""
        self.story.append(Paragraph(str(html), self.styles[style]))

    def _add_code(self, text, max_lines=500):
        """Render a block of monospace/preformatted text with line wrapping.
        Uses Paragraph with <br/> instead of Preformatted so long lines wrap
        rather than bleeding off the page.
        """
        if not text:
            return
        import textwrap as _tw
        lines = str(text).split('\n')
        if len(lines) > max_lines:
            lines = lines[:max_lines] + [f'... ({len(lines) - max_lines} more lines)']
        
        # Wrap any line wider than 95 chars (fits A4 Courier 7.5pt)
        wrapped = []
        for line in lines:
            if len(line) > 95:
                # Hard-wrap preserving leading spaces
                indent = len(line) - len(line.lstrip())
                prefix = ' ' * indent
                chunks = _tw.wrap(line, width=95, subsequent_indent=prefix + '  ')
                wrapped.extend(chunks if chunks else [line])
            else:
                wrapped.append(line)
        
        # Build as Paragraph with <br/> separators so ReportLab can reflow
        safe_lines = [self._safe(l) for l in wrapped]
        html = '<br/>'.join(safe_lines)
        
        # Code-style paragraph with monospace font and light background
        code_style = ParagraphStyle(
            'CodeBlock',
            fontName='Courier',
            fontSize=7.5,
            leading=10,
            spaceAfter=4,
            spaceBefore=2,
            backColor=colors.HexColor('#f4faf8'),
            leftIndent=6,
            rightIndent=6,
            borderPad=4,
        )
        try:
            self.story.append(Paragraph(html, code_style))
        except Exception:
            # Last resort: split into individual short paragraphs
            for line in wrapped[:200]:
                self._add_p(line[:120], 'small')

    def _safe(self, text) -> str:
        """Escape text for ReportLab XML."""
        if text is None:
            return ''
        text = str(text)
        text = text.replace('&', '&amp;')
        text = text.replace('<', '&lt;')
        text = text.replace('>', '&gt;')
        text = text.replace('"', '&quot;')
        # Remove control characters
        text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', text)
        return text

    def _kv_table(self, items: list, col_widths=None):
        """Render a list of (key, value) pairs as a styled table."""
        if not items:
            return
        data = [[Paragraph(self._safe(str(k)), self.styles['meta_key']),
                 Paragraph(self._safe(str(v)), self.styles['normal'])]
                for k, v in items if v not in (None, '', [], {})]
        if not data:
            return
        w = col_widths or [2.2 * inch, 4.5 * inch]
        tbl = Table(data, colWidths=w)
        tbl.setStyle(TableStyle([
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('ROWBACKGROUNDS', (0, 0), (-1, -1), [colors.white, colors.HexColor('#f4faf8')]),
            ('GRID', (0, 0), (-1, -1), 0.25, colors.HexColor('#c8e6dc')),
            ('LEFTPADDING', (0, 0), (-1, -1), 6),
            ('RIGHTPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 3),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
        ]))
        self.story.append(tbl)
        self.story.append(Spacer(1, 4))

    def _data_table(self, headers: list, rows: list, col_widths=None):
        """Render a data table with headers."""
        if not rows:
            return
        header_row = [Paragraph(f'<b>{self._safe(h)}</b>', self.styles['small']) for h in headers]
        data = [header_row]
        for row in rows:
            data.append([Paragraph(self._safe(str(cell)), self.styles['small']) for cell in row])

        n_cols = len(headers)
        if not col_widths:
            page_width = 6.5 * inch
            col_widths = [page_width / n_cols] * n_cols

        tbl = Table(data, colWidths=col_widths, repeatRows=1)
        tbl.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1e7a5e')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#edf7f3')]),
            ('GRID', (0, 0), (-1, -1), 0.25, colors.HexColor('#b2d8ca')),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('LEFTPADDING', (0, 0), (-1, -1), 5),
            ('RIGHTPADDING', (0, 0), (-1, -1), 5),
            ('TOPPADDING', (0, 0), (-1, -1), 3),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
        ]))
        self.story.append(tbl)
        self.story.append(Spacer(1, 6))

    def generate(self):
        doc = SimpleDocTemplate(
            self.output_path,
            pagesize=A4,
            leftMargin=1.5 * cm,
            rightMargin=1.5 * cm,
            topMargin=2 * cm,
            bottomMargin=2 * cm,
            title=f"PBIX Metadata: {self.meta['file_info'].get('filename', '')}",
            author="PBIX Metadata Extractor",
        )

        self._build_cover()
        self._build_toc()
        self._build_llm_briefing()
        self._build_executive_summary()
        self._build_report_pages()
        self._build_slicer_inventory()
        self._build_filter_inventory()
        self._build_data_model()
        self._build_dax_calculations()
        self._build_power_query()
        self._build_data_sources()
        self._build_security()
        self._build_advanced_features()
        self._build_appendix()

        doc.build(self.story)
        print(f"[✓] PDF saved: {self.output_path}")

    # ── LLM Briefing Section ─────────────────────────────────────────────────
    def _build_llm_briefing(self):
        self._add_h1("0. LLM Context Briefing — Complete Report Summary")
        briefing = self.meta.get('llm_briefing', '')
        if not briefing:
            self._add_p("No briefing generated.")
            self.story.append(PageBreak())
            return

        self._add_p(
            "This section is purpose-built for AI assistants (Copilot, ChatGPT, Claude). "
            "It contains the complete report context in structured plain English. "
            "Paste this section into your AI chat to give it full knowledge of the report "
            "before asking it to make changes.",
            'label'
        )
        self.story.append(Spacer(1, 6))
        self._add_code(briefing, max_lines=500)
        self.story.append(PageBreak())

    # ── Enhanced Report Pages ─────────────────────────────────────────────────
    def _build_report_pages(self):
        self._add_h1("2. Report Pages & Visuals")
        pages = self.meta.get('pages', [])

        if not pages:
            self._add_p("No report pages found.")
            self.story.append(PageBreak())
            return

        for page in pages:
            pname = page.get('display_name', page.get('name', 'Unnamed'))
            self._add_h2(f"Page: {pname}")

            items = [
                ('Display Name', pname),
                ('Internal Name', page.get('name', '')),
                ('Dimensions', f"{page.get('width', 0)} × {page.get('height', 0)} px"),
                ('Order', str(page.get('order', 0))),
                ('Hidden', 'Yes' if page.get('hidden') else 'No'),
                ('Visual Count', str(page.get('visual_count', 0))),
            ]
            self._kv_table(items)

            # Page narrative
            narrative = page.get('narrative', '')
            if narrative:
                self._add_p(f"📋 {narrative}", 'label')

            # Page-level filters
            page_filters = self.meta.get('filter_inventory', {}).get('pages', {}).get(pname, [])
            if page_filters:
                self._add_h3("Page-Level Filters (apply to all visuals on this page):")
                rows = [[f['field'], f['type'], f['condition'][:80], '✓' if f['inverted'] else '']
                        for f in page_filters if f.get('field')]
                if rows:
                    self._data_table(
                        ['Field', 'Type', 'Condition', 'Inverted'],
                        rows,
                        col_widths=[1.8*inch, 0.9*inch, 3.2*inch, 0.7*inch]
                    )

            # Visuals — split by type
            visuals = page.get('visuals', [])
            slicers = [v for v in visuals if v.get('visual_type') == 'slicer']
            data_visuals = [v for v in visuals if v.get('visual_type') not in
                           ('slicer', 'textbox', 'image', 'shape', 'basicShape', 'actionButton')]
            chrome = [v for v in visuals if v.get('visual_type') in
                     ('textbox', 'image', 'shape', 'basicShape', 'actionButton')]

            # Slicers
            if slicers:
                self._add_h3(f"Slicers ({len(slicers)}):")
                rows = []
                for v in slicers:
                    sd = v.get('slicer_details', {})
                    fields = ', '.join(v.get('data_fields', []))
                    rows.append([
                        v.get('title', '') or '(untitled)',
                        fields[:50],
                        sd.get('slicer_type', ''),
                        sd.get('default_value', ''),
                    ])
                self._data_table(
                    ['Title', 'Field', 'Type', 'Default Value'],
                    rows,
                    col_widths=[1.3*inch, 1.8*inch, 1.7*inch, 1.8*inch]
                )

            # Data visuals with field roles
            if data_visuals:
                self._add_h3(f"Data Visuals ({len(data_visuals)}):")
                for v in data_visuals:
                    vtype = v.get('visual_type', '')
                    title = v.get('title', '') or '(no title)'
                    roles = v.get('field_roles', {})
                    w, h = int(v.get('width', 0)), int(v.get('height', 0))
                    x, y = int(v.get('x', 0)), int(v.get('y', 0))
                    hidden_flag = ' [HIDDEN]' if v.get('hidden') else ''

                    self._add_html(
                        f'<b>{self._safe(vtype)}</b> — “{self._safe(title)}”{self._safe(hidden_flag)} '
                        f'({w}×{h}px at {x},{y})',
                        'normal'
                    )

                    if roles:
                        role_rows = []
                        for role, fields in roles.items():
                            role_rows.append([role, ', '.join(f[:60] for f in fields)])
                        self._data_table(
                            ['Role / Axis', 'Fields'],
                            role_rows,
                            col_widths=[1.2*inch, 5.4*inch]
                        )
                    elif v.get('data_fields'):
                        fields_str = ', '.join(v['data_fields'])[:200]
                        self._add_p(f"  Fields: {fields_str}", 'small')

                    # Active visual filters
                    decoded = v.get('decoded_filters', [])
                    active = [f for f in decoded if
                             'IN' in f.get('condition', '') or
                             '=' in f.get('condition', '') or
                             'NOT' in f.get('condition', '') or
                             'BETWEEN' in f.get('condition', '')]
                    if active:
                        self._add_html(
                            "<i>Visual filters: " +
                            self._safe(" | ".join(f['display'][:80] for f in active[:5])) + "</i>",
                            'small'
                        )

            if chrome:
                self._add_p(f"Layout elements: {len(chrome)} (textboxes, images, shapes)", 'small')

            self.story.append(Spacer(1, 12))

        self.story.append(PageBreak())

    # ── Slicer Inventory ─────────────────────────────────────────────────────
    def _build_slicer_inventory(self):
        self._add_h1("3. Slicer Inventory")
        slicers = self.meta.get('slicer_inventory', [])

        if not slicers:
            self._add_p("No slicers found in this report.")
            self.story.append(PageBreak())
            return

        self._add_p(
            f"This report contains {len(slicers)} slicers. "
            "Each slicer controls which data is shown on its page (or across pages if synced). "
            "When adding new pages, replicate the slicer pattern shown here.",
            'label'
        )
        self.story.append(Spacer(1, 6))

        # Group by field to show where each slicer appears
        from collections import defaultdict as _dd
        by_field = _dd(list)
        for s in slicers:
            by_field[s['field']].append(s)

        for field, field_slicers in sorted(by_field.items()):
            self._add_h2(f"Field: {field}")
            rows = []
            for s in field_slicers:
                rows.append([
                    s['page'],
                    s['title'] or '(untitled)',
                    s['slicer_type'],
                    s['default_value'],
                    s.get('start_date', '') or s.get('relative_range', ''),
                    s.get('end_date', ''),
                ])
            self._data_table(
                ['Page', 'Title', 'Slicer Type', 'Default / State', 'Start / Rel. Range', 'End Date'],
                rows,
                col_widths=[1.1*inch, 1.2*inch, 1.4*inch, 1.4*inch, 0.9*inch, 0.9*inch]
            )
            if len(field_slicers) > 1:
                self._add_p(
                    f"⚠ This field has {len(field_slicers)} slicers across pages — "
                    "consider using slicer sync groups to keep them aligned.",
                    'label'
                )

        self.story.append(PageBreak())

    # ── Filter Inventory ─────────────────────────────────────────────────────
    def _build_filter_inventory(self):
        self._add_h1("4. Filter Inventory")
        fi = self.meta.get('filter_inventory', {})
        conn_type = self.meta.get('file_info', {}).get('connection_type', '')

        self._add_p(
            "Filters in Power BI operate in four layers: Report → Page → Visual → Slicer. "
            "This section documents the first three layers. Slicers are covered in Section 3.",
            'label'
        )

        # Report-level filters
        self._add_h2("Report-Level Filters (apply to all pages)")
        report_filters = fi.get('report', [])
        if report_filters:
            rows = [[f['field'], f['type'], f['condition'][:90], '✓' if f['inverted'] else '']
                    for f in report_filters if f.get('field')]
            if rows:
                self._data_table(
                    ['Field', 'Type', 'Condition', 'Inverted'],
                    rows,
                    col_widths=[1.9*inch, 0.9*inch, 3.1*inch, 0.7*inch]
                )
        else:
            self._add_p("No report-level filters defined.")

        # Page-level filters
        self._add_h2("Page-Level Filters")
        page_filters = fi.get('pages', {})
        has_any = any(pf for pf in page_filters.values())
        if has_any:
            for page_name, pf_list in sorted(page_filters.items()):
                if not pf_list:
                    continue
                self._add_h3(f"Page: {page_name}")
                rows = [[f['field'], f['type'], f['condition'][:80], f.get('how',''), '✓' if f['inverted'] else '']
                        for f in pf_list if f.get('field')]
                if rows:
                    self._data_table(
                        ['Field', 'Type', 'Condition', 'Origin', 'Inverted'],
                        rows,
                        col_widths=[1.8*inch, 0.85*inch, 2.6*inch, 0.85*inch, 0.5*inch]
                    )
        else:
            self._add_p("No page-level filters defined.")

        # Visual-level filters (active conditions only)
        self._add_h2("Visual-Level Filters with Active Conditions")
        self._add_p(
            "Only visuals with hardcoded filter conditions are shown here. "
            "User-adjustable filters (slicers, drillthrough) are excluded.",
            'small'
        )
        visual_filters = fi.get('visual_summary', [])
        if visual_filters:
            for vf in visual_filters:
                title = vf.get('title', '') or '(untitled)'
                self._add_h3(f"[{vf['page']}] {vf['visual_type']} -- '{title}'")
                rows = [[f['field'], f['condition'][:90], f.get('how', '')]
                        for f in vf['active_filters']]
                if rows:
                    self._data_table(
                        ['Field', 'Condition', 'Origin'],
                        rows,
                        col_widths=[2.0*inch, 3.7*inch, 0.9*inch]
                    )
        else:
            self._add_p("No visuals with explicit hardcoded filter conditions found.")

        self.story.append(PageBreak())

    # ── Cover Page ──────────────────────────────────────────────────────────
    def _build_cover(self):
        fi = self.meta['file_info']
        self.story.append(Spacer(1, 1.5 * inch))

        # Title block
        self.story.append(Paragraph("POWER BI METADATA REPORT", ParagraphStyle(
            'CoverSuper', fontSize=11, textColor=colors.HexColor('#9ec9bd'),
            alignment=TA_CENTER, spaceAfter=8, fontName='Helvetica'
        )))
        self.story.append(Paragraph(
            self._safe(fi.get('filename', 'Unknown File')),
            ParagraphStyle('CoverTitle', fontSize=26, textColor=colors.HexColor('#164d3d'),
                           alignment=TA_CENTER, spaceAfter=6, fontName='Helvetica-Bold')
        ))
        self.story.append(HRFlowable(width='60%', thickness=2, color=colors.HexColor('#1e7a5e'),
                                     spaceAfter=20, hAlign='CENTER'))

        # Stats
        pages_count = len(self.meta.get('pages', []))
        visuals_count = len(self.meta.get('visuals', []))
        tables_count = len(self.meta.get('tables', []))
        measures_count = len(self.meta.get('measures', []))
        relationships_count = len(self.meta.get('relationships', []))

        stats = [
            ['Pages', str(pages_count)],
            ['Visuals', str(visuals_count)],
            ['Tables', str(tables_count)],
            ['Measures', str(measures_count)],
            ['Relationships', str(relationships_count)],
            ['File Size', f"{fi.get('size_mb', 0)} MB"],
        ]

        stat_data = [[Paragraph(f'<b>{self._safe(k)}</b>', ParagraphStyle(
                         'StatKey', fontSize=10, textColor=colors.white,
                         alignment=TA_CENTER, fontName='Helvetica-Bold')),
                      Paragraph(self._safe(v), ParagraphStyle(
                         'StatVal', fontSize=14, textColor=colors.HexColor('#d4f0e8'),
                         alignment=TA_CENTER, fontName='Helvetica-Bold'))]
                     for k, v in stats]

        stat_tbl = Table(stat_data, colWidths=[2.5 * inch, 2.5 * inch])
        stat_tbl.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor('#1e7a5e')),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#164d3d')),
            ('ROWBACKGROUNDS', (0, 0), (-1, -1),
             [colors.HexColor('#1e7a5e'), colors.HexColor('#165c47')]),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ]))
        wrapper = Table([[stat_tbl]], colWidths=[5 * inch])
        wrapper.setStyle(TableStyle([('ALIGN', (0, 0), (0, 0), 'CENTER')]))
        self.story.append(wrapper)

        self.story.append(Spacer(1, 0.5 * inch))
        self.story.append(Paragraph(
            f"Generated: {fi.get('extracted_at', '')[:19]}",
            ParagraphStyle('CoverDate', fontSize=10, textColor=colors.HexColor('#5a7a72'),
                           alignment=TA_CENTER)
        ))
        self.story.append(Paragraph(
            "METADATA ONLY — NO ROW DATA EXTRACTED",
            ParagraphStyle('Security', fontSize=9, textColor=colors.HexColor('#1e7a5e'),
                           alignment=TA_CENTER, spaceAfter=4, fontName='Helvetica-Bold')
        ))
        self.story.append(PageBreak())

    # ── TOC ───────────────────────────────────────────────────────────────────
    def _build_toc(self):
        self._add_h1("Table of Contents")
        sections = [
            ("0", "LLM Context Briefing — Complete Report Summary"),
            ("1", "Executive Summary"),
            ("2", "Report Pages & Visuals (with Field Roles & Filters)"),
            ("3", "Slicer Inventory"),
            ("4", "Filter Inventory (Report, Page & Visual Filters)"),
            ("5", "Data Model — Tables & Columns"),
            ("6", "Relationships & Co-occurrence"),
            ("7", "Hierarchies"),
            ("8", "DAX Measures"),
            ("9", "Calculated Columns & Tables"),
            ("10", "Power Query (M Code)"),
            ("11", "Data Sources & Connections"),
            ("12", "Security (RLS & Permissions)"),
            ("13", "Bookmarks & Custom Visuals"),
            ("14", "Advanced Features"),
            ("A", "Appendix — File Structure"),
        ]
        for num, title in sections:
            self.story.append(Paragraph(
                f'<b>{self._safe(num)}.</b>  {self._safe(title)}',
                self.styles['toc_entry']
            ))
        self.story.append(PageBreak())

    # ── Executive Summary ────────────────────────────────────────────────────
    def _build_executive_summary(self):
        self._add_h1("1. Executive Summary")
        fi = self.meta['file_info']
        dm = self.meta.get('data_model', {})

        self._add_h2("File Information")
        self._kv_table([
            ('File Name', fi.get('filename', '')),
            ('Full Path', fi.get('path', '')),
            ('File Size', f"{fi.get('size_mb', 0)} MB ({fi.get('size_bytes', 0):,} bytes)"),
            ('PBIX Version', fi.get('pbix_version', 'Unknown')),
            ('Extracted At', fi.get('extracted_at', '')[:19]),
        ])

        self._add_h2("Report Statistics")
        pages = self.meta.get('pages', [])
        visuals = self.meta.get('visuals', [])
        # Visual type breakdown
        vtype_counts = defaultdict(int)
        for v in visuals:
            vtype_counts[v.get('visual_type', 'unknown') or 'unknown'] += 1

        self._kv_table([
            ('Report Pages', str(len(pages))),
            ('Hidden Pages', str(sum(1 for p in pages if p.get('hidden')))),
            ('Total Visuals', str(len(visuals))),
            ('Visual Types', ', '.join(f"{k}({v})" for k, v in sorted(vtype_counts.items(), key=lambda x: -x[1])[:10])),
            ('Bookmarks', str(len(self.meta.get('bookmarks', [])))),
            ('Custom Visuals', str(len(self.meta.get('custom_visuals', [])))),
        ])

        self._add_h2("Data Model Statistics")
        self._kv_table([
            ('Model Name', dm.get('name', 'N/A')),
            ('Compatibility Level', str(dm.get('compatibility_level', 'N/A'))),
            ('Culture', dm.get('culture', 'N/A')),
            ('Tables', str(len(self.meta.get('tables', [])))),
            ('Regular Columns', str(len(self.meta.get('columns', [])))),
            ('Calculated Columns', str(len(self.meta.get('calculated_columns', [])))),
            ('Measures', str(len(self.meta.get('measures', [])))),
            ('Relationships', str(len(self.meta.get('relationships', [])))),
            ('Hierarchies', str(len(self.meta.get('hierarchies', [])))),
            ('Calculated Tables', str(len(self.meta.get('calculated_tables', [])))),
        ])

        self._add_h2("Data Sources")
        sources = self.meta.get('data_sources', [])
        pq_queries = self.meta.get('power_query', [])
        self._kv_table([
            ('Data Connections', str(len(sources))),
            ('Power Query Queries', str(len(pq_queries))),
            ('RLS Roles', str(len(self.meta.get('rls_roles', [])))),
            ('Parameters', str(len(self.meta.get('parameters', [])))),
        ])

        # Warnings
        if self.meta.get('errors') or self.meta.get('warnings'):
            self._add_h2("Extraction Notes")
            for err in self.meta.get('errors', []):
                self._add_p(f"❌ ERROR: {err}", 'small')
            for warn in self.meta.get('warnings', []):
                self._add_p(f"⚠ {warn}", 'small')

        self.story.append(PageBreak())

    def _build_data_model(self):
        self._add_h1("3. Data Model — Tables & Columns")
        tables = self.meta.get('tables', [])
        dm = self.meta.get('data_model', {})
        conn_type = self.meta.get('file_info', {}).get('connection_type', '')

        # Explain live connection context
        if conn_type == 'PowerBIServiceLive':
            self._add_p(
                "⚡ This report uses a Live Connection to a Power BI Service dataset. "
                "The full model schema (tables, columns, data types, DAX measures, relationships) "
                "is defined in the published dataset on the service — it is not embedded in the .pbix file. "
                "Tables and fields shown below are inferred from the visual field bindings in the report pages.",
                'label'
            )
            self.story.append(Spacer(1, 6))

        if dm and any(dm.get(k) for k in ('name', 'compatibility_level', 'culture')):
            self._kv_table([
                ('Model Name', dm.get('name', '')),
                ('Compatibility Level', str(dm.get('compatibility_level', ''))),
                ('Culture', dm.get('culture', '')),
                ('Collation', dm.get('collation', '')),
            ])

        if not tables:
            self._add_p("No table schema could be extracted from this file.")
        else:
            # Separate fully-parsed tables from inferred ones
            parsed_tables = [t for t in tables if not t.get('inferred')]
            inferred_tables = [t for t in tables if t.get('inferred')]

            if inferred_tables:
                self._add_p(
                    f"ℹ {len(inferred_tables)} table(s) below are inferred from visual field references "
                    f"(marked with ⚡). Column names and data types from the live dataset are shown where available.",
                    'label'
                )

            for tbl in tables:
                inferred_flag = " ⚡ inferred" if tbl.get('inferred') else ""
                self._add_h2(f"Table: {tbl.get('name', 'Unnamed')}{inferred_flag}")
                tbl_items = [
                    ('Description', tbl.get('description', '')),
                    ('Hidden', 'Yes' if tbl.get('hidden') else 'No'),
                    ('Storage Mode', tbl.get('storage_mode', '') or ('Live/Service' if conn_type == 'PowerBIServiceLive' else '')),
                ]
                self._kv_table([i for i in tbl_items if i[1]])

                # Columns
                all_cols = tbl.get('columns', [])
                if all_cols:
                    rows = []
                    for col in all_cols:
                        inf = " ⚡" if col.get('inferred') else ""
                        rows.append([
                            col.get('name', '') + inf,
                            col.get('data_type', '') or '(unknown)',
                            col.get('column_type', 'data'),
                            col.get('format_string', ''),
                            col.get('display_folder', ''),
                            'Hidden' if col.get('hidden') else '',
                            col.get('description', '')[:60],
                        ])
                    self._add_h3("Columns:")
                    self._data_table(
                        ['Name', 'Data Type', 'Type', 'Format', 'Folder', 'Visibility', 'Description'],
                        rows,
                        col_widths=[1.5*inch, 0.85*inch, 0.7*inch, 0.8*inch, 0.8*inch, 0.6*inch, 1.1*inch]
                    )

                # Measures on this table
                tbl_measures = tbl.get('measures', [])
                if tbl_measures:
                    rows = []
                    for m in tbl_measures:
                        inf = " ⚡" if m.get('inferred') else ""
                        expr_preview = str(m.get('expression', ''))[:80].replace('\n', ' ')
                        rows.append([
                            m.get('name', '') + inf,
                            m.get('format_string', ''),
                            m.get('display_folder', ''),
                            'Hidden' if m.get('hidden') else '',
                            expr_preview,
                        ])
                    self._add_h3("Measures:")
                    self._data_table(
                        ['Name', 'Format', 'Folder', 'Visibility', 'Expression (preview)'],
                        rows,
                        col_widths=[1.5*inch, 0.9*inch, 0.9*inch, 0.6*inch, 2.7*inch]
                    )

                partitions = tbl.get('partitions', [])
                for part in partitions:
                    m_expr = part.get('m_expression', '')
                    if m_expr:
                        if isinstance(m_expr, list):
                            m_expr = '\n'.join(m_expr)
                        self._add_h3(f"Source Query ({part.get('name', '')}):")
                        self._add_code(str(m_expr))

        self.story.append(PageBreak())

        # Relationships page
        self._add_h1("4. Relationships")
        rels = self.meta.get('relationships', [])
        if rels:
            rows = []
            for r in rels:
                inf = " ⚡" if r.get('inferred') else ""
                rows.append([
                    f"{r.get('from_table', '')}[{r.get('from_column', '')}]" + inf,
                    f"{r.get('to_table', '')}[{r.get('to_column', '')}]",
                    r.get('cardinality', ''),
                    r.get('cross_filtering_behavior', ''),
                    'Active' if r.get('is_active', True) else 'Inactive',
                    'Yes' if r.get('assume_referential_integrity') else 'No',
                ])
            self._data_table(
                ['From (Many)', 'To (One)', 'Cardinality', 'Cross-filter', 'Status', 'Ref. Integrity'],
                rows,
                col_widths=[1.8*inch, 1.8*inch, 0.85*inch, 0.85*inch, 0.65*inch, 0.7*inch]
            )
        else:
            if conn_type == 'PowerBIServiceLive':
                self._add_p(
                    "Relationships are defined in the Power BI Service dataset and are not "
                    "embedded in this .pbix file. The table co-occurrence analysis below identifies "
                    "which tables are used together in visuals, strongly indicating relationships exist between them."
                )
            else:
                self._add_p("No explicit relationships found in the DataModel.")

        # Co-occurrence / relationship suggestions
        suggestions = self.meta.get('relationship_suggestions', [])
        if suggestions:
            self._add_h2("Table Co-occurrence (Relationship Indicators)")
            self._add_p(
                "The following table pairs appear together in the same visuals, "
                "indicating active relationships between them in the data model.",
                'label'
            )
            rows = []
            for s in suggestions:
                tables = s.get('tables', [])
                pages = ', '.join(s.get('used_together_on_pages', []))
                rows.append([
                    tables[0] if tables else '',
                    tables[1] if len(tables) > 1 else '',
                    str(len(s.get('used_together_on_pages', []))),
                    pages[:80],
                ])
            self._data_table(
                ['Table A', 'Table B', '# Pages', 'Pages Used Together'],
                rows,
                col_widths=[1.8*inch, 1.8*inch, 0.6*inch, 2.4*inch]
            )
        self.story.append(PageBreak())

        # Hierarchies
        self._add_h1("5. Hierarchies")
        hiers = self.meta.get('hierarchies', [])
        if hiers:
            for h in hiers:
                self._add_h2(f"{h.get('table', '')} → {h.get('name', '')}")
                self._kv_table([
                    ('Description', h.get('description', '')),
                    ('Hidden', 'Yes' if h.get('hidden') else 'No'),
                ])
                levels = h.get('levels', [])
                if levels:
                    rows = [[str(l.get('ordinal', '')), l.get('name', ''), l.get('column', '')]
                            for l in sorted(levels, key=lambda x: x.get('ordinal', 0))]
                    self._data_table(['Level', 'Name', 'Column'], rows,
                                     col_widths=[0.7*inch, 2*inch, 4*inch])
        else:
            self._add_p("No hierarchies found.")
        self.story.append(PageBreak())

    # ── DAX ───────────────────────────────────────────────────────────────────
    def _build_dax_calculations(self):
        self._add_h1("6. DAX Measures")
        measures = self.meta.get('measures', [])
        conn_type = self.meta.get('file_info', {}).get('connection_type', '')

        if conn_type == 'PowerBIServiceLive':
            self._add_p(
                "⚡ This report uses a Live Connection. DAX expressions are stored in the "
                "Power BI Service dataset (the DataModel binary uses XPress9 compression "
                "which cannot be decompressed without the Analysis Services engine). "
                "All measures listed below were discovered from visual field bindings — "
                "their names are authoritative, but DAX formulas require access to the "
                "source dataset in Power BI Service.",
                'label'
            )
            self.story.append(Spacer(1, 6))

        if not measures:
            self._add_p("No measures found.")
        else:
            # Group by table
            by_table = defaultdict(list)
            for m in measures:
                by_table[m.get('table', 'Unknown')].append(m)

            for table_name, tbl_measures in sorted(by_table.items()):
                self._add_h2(f"Table: {table_name}  ({len(tbl_measures)} measures)")
                # Use a compact table view for inferred measures (no DAX available)
                has_dax = any(m.get('expression', '') and
                              m.get('expression') != '(DAX stored in Power BI Service dataset)'
                              for m in tbl_measures)
                if has_dax:
                    for m in tbl_measures:
                        self._add_h3(m.get('name', 'Unnamed'))
                        items = [
                            ('Format String', m.get('format_string', '')),
                            ('Display Folder', m.get('display_folder', '')),
                            ('Description', m.get('description', '')),
                            ('Hidden', 'Yes' if m.get('hidden') else 'No'),
                            ('Data Type', m.get('data_type', '')),
                        ]
                        self._kv_table([i for i in items if i[1]])
                        expr = m.get('expression', '')
                        if expr and expr != '(DAX stored in Power BI Service dataset)':
                            self._add_code(str(expr))
                else:
                    # Compact table for inferred measures
                    rows = [[m.get('name', ''),
                             m.get('format_string', ''),
                             m.get('display_folder', ''),
                             'Hidden' if m.get('hidden') else '']
                            for m in sorted(tbl_measures, key=lambda x: x.get('name', ''))]
                    self._data_table(
                        ['Measure Name', 'Format', 'Display Folder', 'Visibility'],
                        rows,
                        col_widths=[3.0*inch, 1.2*inch, 1.5*inch, 0.9*inch]
                    )

        self.story.append(PageBreak())

        # Calculated Columns
        self._add_h1("7. Calculated Columns & Tables")
        calc_cols = self.meta.get('calculated_columns', [])
        if calc_cols:
            self._add_h2("Calculated Columns")
            by_table = defaultdict(list)
            for c in calc_cols:
                by_table[c.get('table', 'Unknown')].append(c)
            for table_name, cols in sorted(by_table.items()):
                self._add_h3(f"Table: {table_name}")
                for col in cols:
                    self._add_p(f"<b>{self._safe(col.get('name', ''))}</b> "
                                f"[{self._safe(col.get('data_type', ''))}] "
                                f"Format: {self._safe(col.get('format_string', ''))}")
                    expr = col.get('dax_expression', '')
                    if expr:
                        self._add_code(str(expr))
        else:
            self._add_h2("Calculated Columns")
            self._add_p("No calculated columns found.")

        calc_tables = self.meta.get('calculated_tables', [])
        if calc_tables:
            self._add_h2("Calculated Tables")
            for ct in calc_tables:
                self._add_h3(ct.get('name', 'Unnamed'))
                expr = ct.get('expression', '')
                if expr:
                    self._add_code(str(expr))
        else:
            self._add_h2("Calculated Tables")
            self._add_p("No calculated tables found.")

        self.story.append(PageBreak())

    # ── Power Query ───────────────────────────────────────────────────────────
    def _build_power_query(self):
        self._add_h1("8. Power Query (M Code)")
        queries = self.meta.get('power_query', [])

        if not queries:
            self._add_p("No Power Query scripts found.")
            self.story.append(PageBreak())
            return

        for q in queries:
            self._add_h2(f"Query: {q.get('name', 'Unnamed')}")
            self._kv_table([
                ('Type', q.get('type', '')),
                ('Source File', q.get('source_file', '')),
            ])
            m_code = q.get('m_code', '')
            if m_code:
                self._add_code(str(m_code), max_lines=80)

        self.story.append(PageBreak())

    # ── Data Sources ──────────────────────────────────────────────────────────
    def _build_data_sources(self):
        self._add_h1("9. Data Sources & Connections")
        sources = self.meta.get('data_sources', [])

        if not sources:
            self._add_p("No data source connections found.")
        else:
            for i, ds in enumerate(sources, 1):
                conn_type = ds.get('connection_type', '')
                title = ds.get('name', ds.get('provider', f'Connection {i}'))
                self._add_h2(f"Connection {i}: {title}")

                if conn_type == 'PowerBIServiceLive':
                    # Prominent live-connection callout
                    self._kv_table([
                        ('Connection Type', '⚡ Power BI Service Live Connection'),
                        ('Dataset ID', ds.get('dataset_id', '')),
                        ('Report ID', ds.get('report_id', '')),
                        ('Workspace ID', ds.get('workspace_id', '')),
                        ('Mode', ds.get('mode', '')),
                    ])
                    self._add_p(ds.get('note', ''), 'label')
                else:
                    self._kv_table([
                        ('Name', ds.get('name', '')),
                        ('Provider', ds.get('provider', '')),
                        ('Mode', ds.get('mode', '')),
                        ('Connection String', ds.get('connection_string', '')),
                        ('Model Database', ds.get('pbi_model_database_name', '')),
                    ])

        self.story.append(PageBreak())

    # ── Security ──────────────────────────────────────────────────────────────
    def _build_security(self):
        self._add_h1("10. Security (Row-Level Security & Permissions)")
        roles = self.meta.get('rls_roles', [])

        if not roles:
            self._add_p("No RLS roles defined in this report.")
            self.story.append(PageBreak())
            return

        for role in roles:
            self._add_h2(f"Role: {role.get('name', 'Unnamed')}")
            self._kv_table([
                ('Model Permission', role.get('model_permission', '')),
            ])

            filters = role.get('table_filters', [])
            if filters:
                self._add_h3("Table Filter Expressions:")
                for f in filters:
                    self._add_p(f"<b>Table:</b> {self._safe(f.get('table', ''))}")
                    expr = f.get('filter_expression', '')
                    if expr:
                        self._add_code(str(expr))

            members = role.get('members', [])
            if members:
                self._add_h3("Members:")
                rows = [[m.get('member_name', ''), m.get('identity_provider', '')]
                        for m in members]
                self._data_table(['Member Name', 'Identity Provider'], rows,
                                 col_widths=[3*inch, 3.5*inch])

        self.story.append(PageBreak())

    # ── Bookmarks & Custom Visuals ─────────────────────────────────────────────
    def _build_advanced_features(self):
        self._add_h1("11. Bookmarks & Custom Visuals")

        bookmarks = self.meta.get('bookmarks', [])
        if bookmarks:
            self._add_h2(f"Bookmarks ({len(bookmarks)})")
            rows = [[b.get('name', ''), b.get('id', ''), b.get('report_page', '')]
                    for b in bookmarks]
            self._data_table(['Display Name', 'Internal ID', 'Report Page'],
                             rows, col_widths=[2.5*inch, 2*inch, 2*inch])
        else:
            self._add_h2("Bookmarks")
            self._add_p("No bookmarks found.")

        custom_visuals = self.meta.get('custom_visuals', [])
        if custom_visuals:
            self._add_h2(f"Custom Visuals ({len(custom_visuals)})")
            rows = [[cv.get('name', ''), cv.get('guid', ''), cv.get('version', '')]
                    for cv in custom_visuals]
            self._data_table(['Name', 'GUID', 'Version'], rows,
                             col_widths=[2.5*inch, 2.5*inch, 1.5*inch])
        else:
            self._add_h2("Custom Visuals")
            self._add_p("No custom visuals detected.")

        self.story.append(PageBreak())

        # Advanced features page
        self._add_h1("12. Advanced Features")

        perspectives = self.meta.get('perspectives', [])
        if perspectives:
            self._add_h2(f"Perspectives ({len(perspectives)})")
            for p in perspectives:
                self._add_p(f"<b>{self._safe(p.get('name', ''))}</b>: "
                            f"tables: {', '.join(p.get('tables', []))}")
        else:
            self._add_h2("Perspectives")
            self._add_p("None defined.")

        translations = self.meta.get('translations', [])
        if translations:
            self._add_h2(f"Translations / Cultures ({len(translations)})")
            for t in translations:
                self._add_p(f"Culture: {self._safe(t.get('name', ''))}")
        else:
            self._add_h2("Translations")
            self._add_p("None defined.")

        self.story.append(PageBreak())

    # ── Appendix ──────────────────────────────────────────────────────────────
    def _build_appendix(self):
        self._add_h1("Appendix A — ZIP File Structure")
        contents = self.meta.get('zip_contents', [])

        if not contents:
            self._add_p("No ZIP contents recorded.")
            return

        # Summary
        total_size = sum(c.get('size_bytes', 0) for c in contents)
        blocked = [c for c in contents if c.get('blocked')]
        safe = [c for c in contents if not c.get('blocked')]

        self._kv_table([
            ('Total Entries', str(len(contents))),
            ('Total Uncompressed Size', f"{total_size / 1024:.1f} KB"),
            ('Extracted (Safe)', str(len(safe))),
            ('Blocked (Security)', str(len(blocked))),
        ])

        self._add_h2("All Files")
        rows = []
        for c in sorted(contents, key=lambda x: x.get('name', '')):
            size_kb = c.get('size_bytes', 0) / 1024
            status = '🔒 BLOCKED' if c.get('blocked') else '✓ Safe'
            rows.append([
                c.get('name', ''),
                f"{size_kb:.1f} KB",
                f"{c.get('compressed_bytes', 0) / 1024:.1f} KB",
                status,
            ])
        self._data_table(
            ['File Name', 'Uncompressed', 'Compressed', 'Status'],
            rows,
            col_widths=[3.5*inch, 1*inch, 1*inch, 1.1*inch]
        )

        self._add_h2("Security Note")
        self._add_p(
            "This report was generated with strict data privacy controls. "
            "The following file types were NEVER read to prevent exposure of row data: "
            "DataModel binary (compressed VertiPaq column store), .abf backup files, "
            "xpress9 compressed streams, and partition data files. "
            "Only schema/metadata JSON embedded in the DataModel header was parsed."
        )


# ═══════════════════════════════════════════════════════════════════════════
# PLAIN TEXT REPORT GENERATOR
# ═══════════════════════════════════════════════════════════════════════════

class TextReportGenerator:
    def __init__(self, metadata: dict, output_path: str):
        self.meta = metadata
        self.output_path = output_path
        self.lines = []

    def _line(self, text=''):
        self.lines.append(str(text))

    def _section(self, title):
        self._line()
        self._line('=' * 70)
        self._line(f'  {title}')
        self._line('=' * 70)

    def _subsection(self, title):
        self._line()
        self._line(f'--- {title} ---')

    def generate(self):
        fi = self.meta['file_info']
        self._line(f"PBIX COMPLETE METADATA REPORT")
        self._line(f"File: {fi.get('filename', '')}")
        self._line(f"Size: {fi.get('size_mb', 0)} MB")
        self._line(f"Generated: {fi.get('extracted_at', '')}")
        self._line("SECURITY: NO ROW DATA EXTRACTED")

        self._section("EXECUTIVE SUMMARY")
        self._line(f"Pages: {len(self.meta.get('pages', []))}")
        self._line(f"Visuals: {len(self.meta.get('visuals', []))}")
        self._line(f"Tables: {len(self.meta.get('tables', []))}")
        self._line(f"Measures: {len(self.meta.get('measures', []))}")
        self._line(f"Relationships: {len(self.meta.get('relationships', []))}")
        self._line(f"Power Query Queries: {len(self.meta.get('power_query', []))}")
        self._line(f"RLS Roles: {len(self.meta.get('rls_roles', []))}")

        self._section("REPORT PAGES")
        for page in self.meta.get('pages', []):
            self._line(f"\nPage: {page.get('display_name', '')} | {page.get('width')}x{page.get('height')} | "
                       f"{'Hidden' if page.get('hidden') else 'Visible'}")
            for v in page.get('visuals', []):
                fields = ', '.join(v.get('data_fields', []))
                self._line(f"  [{v.get('visual_type', '?')}] {v.get('title', '')} "
                           f"@({v.get('x')},{v.get('y')}) {v.get('width')}x{v.get('height')} "
                           f"Fields: {fields}")

        self._section("DATA MODEL TABLES")
        for tbl in self.meta.get('tables', []):
            self._line(f"\nTable: {tbl.get('name')} | Hidden: {tbl.get('hidden')} | Mode: {tbl.get('storage_mode')}")
            for col in tbl.get('columns', []):
                self._line(f"  Column: {col.get('name')} [{col.get('data_type')}] "
                           f"{'(hidden)' if col.get('hidden') else ''} "
                           f"{'(calc)' if col.get('column_type') == 'calculated' else ''}")
            for m in tbl.get('measures', []):
                self._line(f"  Measure: {m.get('name')} | Format: {m.get('format_string')}")
                self._line(f"    = {m.get('expression', '')}")

        self._section("RELATIONSHIPS")
        for r in self.meta.get('relationships', []):
            self._line(f"  {r.get('from_table')}[{r.get('from_column')}] "
                       f"-> {r.get('to_table')}[{r.get('to_column')}] "
                       f"| {r.get('cardinality')} | {r.get('cross_filtering_behavior')} "
                       f"| {'Active' if r.get('is_active', True) else 'Inactive'}")

        self._section("DAX MEASURES")
        for m in self.meta.get('measures', []):
            self._line(f"\n[{m.get('table')}].[{m.get('name')}]")
            self._line(f"Format: {m.get('format_string')} | Folder: {m.get('display_folder')}")
            self._line(m.get('expression', ''))

        self._section("POWER QUERY (M CODE)")
        for q in self.meta.get('power_query', []):
            self._line(f"\nQuery: {q.get('name')}")
            self._line(q.get('m_code', ''))

        self._section("DATA SOURCES")
        for ds in self.meta.get('data_sources', []):
            self._line(f"  {ds.get('name')} | {ds.get('provider')} | {ds.get('connection_string')}")

        self._section("ROW-LEVEL SECURITY")
        for role in self.meta.get('rls_roles', []):
            self._line(f"\nRole: {role.get('name')} | Permission: {role.get('model_permission')}")
            for f in role.get('table_filters', []):
                self._line(f"  Table: {f.get('table')} => {f.get('filter_expression')}")

        self._section("ZIP FILE STRUCTURE")
        for f in self.meta.get('zip_contents', []):
            status = 'BLOCKED' if f.get('blocked') else 'safe'
            self._line(f"  {status:7} {f.get('size_bytes',0)/1024:8.1f} KB  {f.get('name')}")

        output = '\n'.join(self.lines)
        with open(self.output_path, 'w', encoding='utf-8') as fh:
            fh.write(output)
        print(f"[✓] Text report saved: {self.output_path}")


# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════
# GUI LAUNCHER
# ═══════════════════════════════════════════════════════════════════════════

def run_gui():
    """Full GUI mode: file picker → progress window → open result."""
    try:
        import tkinter as tk
        from tkinter import filedialog, messagebox, ttk
    except ImportError:
        print("tkinter not available — falling back to command-line mode.")
        run_cli()
        return

    # ── 1. Pick the .pbix file ───────────────────────────────────────────────
    root = tk.Tk()
    root.withdraw()  # Hide the root window; we only want the dialogs

    pbix_path = filedialog.askopenfilename(
        title="Select a Power BI file",
        filetypes=[("Power BI files", "*.pbix"), ("All files", "*.*")],
    )

    if not pbix_path:
        # User cancelled
        root.destroy()
        return

    # ── 2. Decide output path (same folder as the .pbix) ────────────────────
    pbix_path = Path(pbix_path)
    output_pdf = pbix_path.parent / (pbix_path.stem + "_metadata.pdf")

    # ── 3. Progress window ───────────────────────────────────────────────────
    win = tk.Toplevel(root)
    win.title("PBIX Metadata Extractor")
    win.geometry("480x220")
    win.resizable(False, False)
    win.configure(bg="#1B3A6B")

    # Center on screen
    win.update_idletasks()
    x = (win.winfo_screenwidth() - 480) // 2
    y = (win.winfo_screenheight() - 220) // 2
    win.geometry(f"+{x}+{y}")

    tk.Label(win, text="Power BI Metadata Extractor",
             font=("Helvetica", 14, "bold"), fg="white", bg="#1B3A6B").pack(pady=(18, 4))

    file_label = tk.Label(win, text=f"📄  {pbix_path.name}",
                          font=("Helvetica", 10), fg="#90CDF4", bg="#1B3A6B")
    file_label.pack(pady=(0, 12))

    status_var = tk.StringVar(value="Starting…")
    status_label = tk.Label(win, textvariable=status_var,
                            font=("Helvetica", 9), fg="#E2E8F0", bg="#1B3A6B")
    status_label.pack(pady=(0, 8))

    bar = ttk.Progressbar(win, mode="indeterminate", length=400)
    bar.pack(pady=(0, 12))
    bar.start(12)

    detail_var = tk.StringVar(value="")
    tk.Label(win, textvariable=detail_var,
             font=("Helvetica", 8), fg="#718096", bg="#1B3A6B").pack()

    win.update()

    # ── 4. Run extraction in the same thread (updates UI between steps) ──────
    import threading

    result = {"output_path": None, "error": None, "metadata": None}

    def do_extract():
        try:
            # Monkey-patch print so progress messages update the status label
            original_print = builtins_print
            def gui_print(*args, **kwargs):
                msg = " ".join(str(a) for a in args)
                original_print(msg)
                # Strip leading symbols for cleaner display
                clean = msg.strip().lstrip('[✓*] ').strip()
                if clean:
                    status_var.set(clean[:72])
                    win.update_idletasks()

            import builtins
            builtins.print = gui_print

            extractor = PBIXExtractor(str(pbix_path))
            metadata = extractor.extract()
            result["metadata"] = metadata

            status_var.set("Generating PDF report…")
            win.update_idletasks()

            if not REPORTLAB_AVAILABLE:
                output_path = str(output_pdf).replace('.pdf', '.txt')
                gen = TextReportGenerator(metadata, output_path)
                gen.generate()
            else:
                gen = PDFReportGenerator(metadata, str(output_pdf))
                gen.generate()
                result["output_path"] = str(output_pdf)

        except Exception as e:
            result["error"] = str(e)
        finally:
            import builtins
            builtins.print = original_print

    # Capture original print before patching
    import builtins
    builtins_print = builtins.print

    thread = threading.Thread(target=do_extract, daemon=True)
    thread.start()

    # Poll until the thread finishes
    def poll():
        if thread.is_alive():
            win.after(100, poll)
        else:
            finish()

    def finish():
        bar.stop()
        win.destroy()

        if result["error"]:
            messagebox.showerror(
                "Extraction Failed",
                f"An error occurred:\n\n{result['error']}\n\nCheck that the file is a valid .pbix and try again."
            )
            root.destroy()
            return

        meta = result["metadata"]
        pages = len(meta.get("pages", []))
        tables = len(meta.get("tables", []))
        measures = len(meta.get("measures", []))
        relationships = len(meta.get("relationships", []))
        visuals = len(meta.get("visuals", []))
        out = result["output_path"] or str(output_pdf).replace('.pdf', '.txt')

        summary = (
            f"Extraction complete!\n\n"
            f"  Pages:          {pages}\n"
            f"  Visuals:        {visuals}\n"
            f"  Tables:         {tables}\n"
            f"  Measures:       {measures}\n"
            f"  Relationships:  {relationships}\n\n"
            f"Saved to:\n{out}\n\n"
            f"Open the PDF now?"
        )

        open_it = messagebox.askyesno("Done! ✓", summary)
        if open_it and result["output_path"]:
            _open_file(result["output_path"])

        root.destroy()

    win.after(100, poll)
    root.mainloop()


def _open_file(path: str):
    """Open a file with the OS default application."""
    import subprocess
    try:
        if sys.platform == "win32":
            os.startfile(path)
        elif sys.platform == "darwin":
            subprocess.run(["open", path])
        else:
            subprocess.run(["xdg-open", path])
    except Exception as e:
        print(f"Could not open file automatically: {e}")


# ═══════════════════════════════════════════════════════════════════════════
# COMMAND-LINE MODE (unchanged, for scripting / automation)
# ═══════════════════════════════════════════════════════════════════════════

def run_cli():
    parser = argparse.ArgumentParser(
        description='Extract complete metadata from Power BI (.pbix) files.\n'
                    'Always produces BOTH a PDF report and a JSON file.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python pbix_extractor.py report.pbix
      → report_metadata.pdf  +  report_metadata.json

  python pbix_extractor.py report.pbix output.pdf
      → output.pdf  +  output.json

  python pbix_extractor.py report.pbix --text
      → report_metadata.txt  +  report_metadata.json

  python pbix_extractor.py report.pbix --no-json
      → report_metadata.pdf  (no JSON)
        """
    )
    parser.add_argument('pbix_file', help='Path to the .pbix file')
    parser.add_argument('output', nargs='?', help='Output PDF/TXT path (optional)')
    parser.add_argument('--text',    action='store_true',
                        help='Generate plain text report instead of PDF')
    parser.add_argument('--no-json', action='store_true', dest='no_json',
                        help='Suppress the automatic JSON output')
    # Legacy --json flag kept for backwards-compatibility (now a no-op)
    parser.add_argument('--json', action='store_true', help=argparse.SUPPRESS)
    args = parser.parse_args()

    if not os.path.exists(args.pbix_file):
        print(f"ERROR: File not found: {args.pbix_file}")
        sys.exit(1)

    # ── Derive output paths ──────────────────────────────────────────────────
    base_name = Path(args.pbix_file).stem
    if args.output:
        output_path = args.output
        out_dir     = Path(args.output).parent
        out_stem    = Path(args.output).stem
        json_path   = str(out_dir / f"{out_stem}.json")
    else:
        output_path = f"{base_name}_metadata.{'txt' if args.text else 'pdf'}"
        json_path   = f"{base_name}_metadata.json"

    # ── Extract ──────────────────────────────────────────────────────────────
    extractor = PBIXExtractor(args.pbix_file)
    metadata  = extractor.extract()

    # ── JSON output (default ON, suppressed by --no-json) ────────────────────
    if not args.no_json:
        clean = extractor.build_clean_json()
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(clean, f, indent=2, ensure_ascii=False, default=str)
        json_kb = Path(json_path).stat().st_size // 1024
        print(f"[✓] JSON saved: {json_path}  ({json_kb} KB)")

    # ── PDF / Text report ────────────────────────────────────────────────────
    if args.text:
        TextReportGenerator(metadata, output_path).generate()
    else:
        if not REPORTLAB_AVAILABLE:
            print("ERROR: reportlab not installed. pip install reportlab")
            TextReportGenerator(metadata, output_path.replace('.pdf', '.txt')).generate()
        else:
            PDFReportGenerator(metadata, output_path).generate()

    # ── Summary ──────────────────────────────────────────────────────────────
    print(f"\n✓ Done!")
    print(f"  PDF : {output_path}")
    if not args.no_json:
        print(f"  JSON: {json_path}")
    print(f"  Pages     : {len(metadata.get('pages', []))}")
    print(f"  Visuals   : {len(metadata.get('visuals', []))}")
    print(f"  Tables    : {len(metadata.get('tables', []))}")
    print(f"  Measures  : {len(metadata.get('measures', []))}")
    print(f"  Slicers   : {len(metadata.get('slicer_inventory', []))}")
    print(f"  Relationships: {len(metadata.get('relationships', []))}")


# ═══════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

def main():
    # If any command-line arguments were passed, use CLI mode (for scripting).
    # Otherwise, launch the GUI (double-click / desktop shortcut).
    if len(sys.argv) > 1:
        run_cli()
    else:
        run_gui()


if __name__ == '__main__':
    main()
