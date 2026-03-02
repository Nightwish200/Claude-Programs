"""
Dynamics AX 2012 R3 - XPO File Extractor
Parses an .xpo file and generates a comprehensive PDF report
suitable for uploading to an LLM for full project understanding.

Requirements:
    pip install reportlab

Run:
    python xpo_extractor.py
"""

import os
import re
import sys
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from datetime import datetime
from collections import defaultdict

from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, PageBreak,
    Table, TableStyle, HRFlowable, KeepTogether
)
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY


# ──────────────────────────────────────────────────────────────────────────────
#  HELPER: Strip leading '#' from XPO property values and names
# ──────────────────────────────────────────────────────────────────────────────

def strip_hash(value: str) -> str:
    """Strip leading '#' from an XPO value like '#SalesTable' -> 'SalesTable'."""
    if value and value.startswith("#"):
        return value[1:]
    return value


# ──────────────────────────────────────────────────────────────────────────────
#  XPO PARSER
# ──────────────────────────────────────────────────────────────────────────────

class XPOParser:
    """
    Parses Dynamics AX 2012 R3 .xpo export files.

    The XPO format uses '***Element: <TYPE>' markers to separate top-level
    objects.  Each object block contains PROPERTIES, FIELDS, INDICES,
    REFERENCES, METHODS, etc.  All names and values are prefixed with '#'.
    """

    # Map element marker codes to human-readable types
    ELEMENT_TYPE_MAP = {
        "DBT": "TABLE",
        "CLS": "CLASS",
        "FRM": "FORM",
        "QUE": "QUERY",
        "RPT": "REPORT",
        "MNU": "MENU",
        "JOB": "JOB",
        "ENM": "ENUM",
        "EDT": "EDT",
        "PRN": "PROJECT",
        "END": "END",
    }

    def __init__(self, filepath: str) -> None:
        self.filepath = filepath
        self.filename = os.path.basename(filepath)
        self.raw_content: str = ""
        self.objects: dict = defaultdict(list)
        self.summary_stats: dict = {}
        self.export_header: dict = {}

    def parse(self) -> None:
        with open(self.filepath, "r", encoding="utf-8", errors="replace") as fh:
            self.raw_content = fh.read()
        self._parse_header()
        self._parse_elements()
        self._compute_stats()

    # ── Header ────────────────────────────────────────────────────────────

    def _parse_header(self) -> None:
        header_patterns = {
            "ax_version":  r"Microsoft Dynamics AX\s+[\d\.]+",
            "export_date": r"Exported:\s*(.+)",
            "layer":       r"Layer\s*:\s*(\w+)",
            "model":       r"Model\s*:\s*(.+)",
        }
        sample = self.raw_content[:2000]
        for key, pattern in header_patterns.items():
            m = re.search(pattern, sample, re.IGNORECASE)
            if m:
                self.export_header[key] = (
                    m.group(0).strip() if not m.lastindex else m.group(1).strip()
                )

    # ── Element splitting (fixes the depth-counting bug) ─────────────────

    def _parse_elements(self) -> None:
        """Split file on '***Element:' markers, then parse each block."""
        parts = re.split(r"(\*\*\*Element:\s*\w+)", self.raw_content)

        i = 0
        while i < len(parts):
            part = parts[i].strip()
            m = re.match(r"\*\*\*Element:\s*(\w+)", part)
            if m:
                element_code = m.group(1).upper()
                element_type = self.ELEMENT_TYPE_MAP.get(element_code, element_code)
                # The block content is the next part
                block_text = parts[i + 1] if (i + 1) < len(parts) else ""
                if element_type not in ("END", "PROJECT"):
                    self._parse_element_block(element_type, block_text)
                i += 2
            else:
                i += 1

    def _parse_element_block(self, element_type: str, text: str) -> None:
        """Parse a single element block based on its type."""
        name = self._extract_element_name(element_type, text)

        obj = {
            "name": name,
            "type": element_type,
            "raw": text,
            "methods": [],
            "fields": [],
            "relations": [],
            "indexes": [],
            "properties": {},
            "data_sources": [],
            "enums": [],
            "delete_actions": [],
            "groups": [],
        }

        dispatch = {
            "TABLE": self._parse_table,
            "CLASS": self._parse_class,
            "FORM":  self._parse_form,
            "QUERY": self._parse_query,
            "ENUM":  self._parse_enum,
            "EDT":   self._parse_edt,
        }
        dispatch.get(element_type, self._parse_generic)(obj, text)
        self.objects[element_type].append(obj)

    def _extract_element_name(self, element_type: str, text: str) -> str:
        """Extract object name from comment line or keyword line."""
        # Try comment line: "; Microsoft Dynamics AX Table : SalesTable unloaded"
        m = re.search(r";\s*Microsoft Dynamics AX\s+\w+\s*:\s*(\S+)", text)
        if m:
            return strip_hash(m.group(1))
        # Try the keyword matching the element type FIRST, then others
        type_to_keyword = {
            "TABLE": "TABLE", "CLASS": "CLASS", "FORM": "FORM",
            "QUERY": "QUERY", "ENUM": "ENUM", "EDT": "EDT",
        }
        primary_kw = type_to_keyword.get(element_type)
        keywords = [primary_kw] if primary_kw else []
        keywords += [kw for kw in ["TABLE", "CLASS", "FORM", "QUERY", "ENUM", "EDT"]
                     if kw != primary_kw]
        for kw in keywords:
            m = re.search(rf"^\s*{kw}\s+#(\w+)", text, re.MULTILINE)
            if m:
                return m.group(1)
        return "Unknown"

    # ── Table parser ──────────────────────────────────────────────────────

    def _parse_table(self, obj: dict, text: str) -> None:
        """Parse a TABLE element: fields, indexes, references, methods, properties."""
        obj["properties"] = self._extract_table_properties(text)
        obj["fields"] = self._extract_fields(text)
        obj["indexes"] = self._extract_indexes(text)
        obj["relations"] = self._extract_references(text)
        obj["delete_actions"] = self._extract_delete_actions(text)
        obj["methods"] = self._extract_source_methods(text)

    def _extract_table_properties(self, text: str) -> dict:
        """Extract top-level table properties from the first PROPERTIES block."""
        m = re.search(r"^\s*PROPERTIES\s*\r?\n(.*?)^\s*ENDPROPERTIES",
                       text, re.MULTILINE | re.DOTALL)
        if not m:
            return {}
        prop_text = m.group(1)
        props = {}
        for key in ["Name", "Label", "ConfigurationKey", "TableGroup",
                     "CacheLookup", "PrimaryIndex", "ClusterIndex",
                     "CreatedBy", "ModifiedBy", "ModifiedDateTime",
                     "DeveloperDocumentation", "TitleField1", "TitleField2",
                     "SecurityKey", "CreateRecIdIndex"]:
            val = self._prop_value(prop_text, key)
            if val:
                props[key] = val
        return props

    def _extract_fields(self, text: str) -> list:
        """Extract all FIELD definitions from the FIELDS...ENDFIELDS block."""
        fields_block = self._section(text, "FIELDS", "ENDFIELDS")
        if not fields_block:
            return []
        fields = []
        # Pattern: "FIELD #FieldName\n        DATATYPE\n        PROPERTIES..."
        for m in re.finditer(
            r"FIELD\s+#(\w+)\s*\r?\n\s+(\w+)\s*\r?\n\s+PROPERTIES\s*\r?\n(.*?)ENDPROPERTIES",
            fields_block, re.DOTALL
        ):
            field_name = m.group(1)
            data_type = m.group(2)
            prop_text = m.group(3)

            label = self._prop_value(prop_text, "Label") or ""
            edt = self._prop_value(prop_text, "ExtendedDataType") or ""
            enum_type = self._prop_value(prop_text, "EnumType") or ""
            mandatory = "Yes" if self._prop_value(prop_text, "Mandatory") == "Yes" else "No"
            allow_edit = self._prop_value(prop_text, "AllowEdit") or "Yes"

            fields.append({
                "name": field_name,
                "type": data_type,
                "label": label,
                "edt": edt,
                "enum_type": enum_type,
                "mandatory": mandatory,
                "allow_edit": allow_edit,
            })
        return fields

    def _extract_indexes(self, text: str) -> list:
        """Extract all index definitions from the INDICES...ENDINDICES block."""
        idx_block = self._section(text, "INDICES", "ENDINDICES")
        if not idx_block:
            return []
        indexes = []
        for m in re.finditer(
            r"#(\w+)\s*\r?\n\s+PROPERTIES\s*\r?\n(.*?)ENDPROPERTIES\s*\r?\n\s*\r?\n\s+INDEXFIELDS\s*\r?\n(.*?)ENDINDEXFIELDS",
            idx_block, re.DOTALL
        ):
            idx_name = m.group(1)
            prop_text = m.group(2)
            fields_text = m.group(3)

            allow_dup = self._prop_value(prop_text, "AllowDuplicates") or "Yes"
            unique = (allow_dup == "No")
            alt_key = self._prop_value(prop_text, "AlternateKey") or "No"
            config_key = self._prop_value(prop_text, "ConfigurationKey") or ""

            idx_fields = re.findall(r"#(\w+)", fields_text)

            indexes.append({
                "name": idx_name,
                "fields": idx_fields,
                "unique": unique,
                "alternate_key": alt_key == "Yes",
                "config_key": config_key,
            })
        return indexes

    def _extract_references(self, text: str) -> list:
        """Extract foreign-key references from REFERENCES...ENDREFERENCES block."""
        ref_block = self._section(text, "REFERENCES", "ENDREFERENCES")
        if not ref_block:
            return []
        relations = []
        for m in re.finditer(
            r"REFERENCE\s+#(\w+)\s*\r?\n\s+PROPERTIES\s*\r?\n(.*?)ENDPROPERTIES\s*\r?\n.*?FIELDREFERENCES\s*\r?\n(.*?)ENDFIELDREFERENCES",
            ref_block, re.DOTALL
        ):
            ref_name = m.group(1)
            prop_text = m.group(2)
            field_ref_text = m.group(3)

            related_table = self._prop_value(prop_text, "Table") or ""
            cardinality = self._prop_value(prop_text, "Cardinality") or ""
            related_cardinality = self._prop_value(prop_text, "RelatedTableCardinality") or ""
            rel_type = self._prop_value(prop_text, "RelationshipType") or ""

            field_maps = []
            for fm in re.finditer(
                r"PROPERTIES\s*\r?\n(.*?)ENDPROPERTIES",
                field_ref_text, re.DOTALL
            ):
                fp = fm.group(1)
                local_field = self._prop_value(fp, "Field") or ""
                related_field = self._prop_value(fp, "RelatedField") or ""
                if local_field and related_field:
                    field_maps.append(f"{local_field} -> {related_field}")

            relations.append({
                "name": ref_name,
                "table": related_table,
                "cardinality": f"{related_cardinality} : {cardinality}" if cardinality else "",
                "relationship_type": rel_type,
                "field_mappings": field_maps,
            })
        return relations

    def _extract_delete_actions(self, text: str) -> list:
        """Extract DELETEACTIONS definitions."""
        da_block = self._section(text, "DELETEACTIONS", "ENDDELETEACTIONS")
        if not da_block:
            return []
        actions = []
        for m in re.finditer(
            r"PROPERTIES\s*\r?\n(.*?)ENDPROPERTIES",
            da_block, re.DOTALL
        ):
            prop_text = m.group(1)
            table = self._prop_value(prop_text, "Table") or ""
            action = self._prop_value(prop_text, "DeleteAction") or ""
            if table:
                actions.append({"table": table, "action": action})
        return actions

    # ── Class parser ──────────────────────────────────────────────────────

    def _parse_class(self, obj: dict, text: str) -> None:
        """Parse a CLASS element."""
        m = re.search(r"^\s*PROPERTIES\s*\r?\n(.*?)^\s*ENDPROPERTIES",
                       text, re.MULTILINE | re.DOTALL)
        if m:
            prop_text = m.group(1)
            for key in ["Name", "Extends", "Origin"]:
                val = self._prop_value(prop_text, key)
                if val:
                    obj["properties"][key] = val
        obj["methods"] = self._extract_source_methods(text)

    # ── Form parser ───────────────────────────────────────────────────────

    def _parse_form(self, obj: dict, text: str) -> None:
        """Parse a FORM element."""
        m = re.search(r"^\s*PROPERTIES\s*\r?\n(.*?)^\s*ENDPROPERTIES",
                       text, re.MULTILINE | re.DOTALL)
        if m:
            prop_text = m.group(1)
            for key in ["Name", "FormTemplate", "InteractionClass"]:
                val = self._prop_value(prop_text, key)
                if val:
                    obj["properties"][key] = val
        obj["methods"] = self._extract_source_methods(text)
        obj["data_sources"] = list(dict.fromkeys(
            re.findall(r"DATASOURCE\s+#(\w+)", text, re.IGNORECASE)
        ))

    # ── Query parser ──────────────────────────────────────────────────────

    def _parse_query(self, obj: dict, text: str) -> None:
        """Parse a QUERY element."""
        obj["data_sources"] = list(dict.fromkeys(
            re.findall(r"DATASOURCE\s+#(\w+)", text, re.IGNORECASE)
        ))
        obj["properties"] = {
            "title": self._prop_value(text, "TITLE") or "",
        }

    # ── Enum parser ───────────────────────────────────────────────────────

    def _parse_enum(self, obj: dict, text: str) -> None:
        """Parse an ENUM element."""
        obj["enums"] = [{"name": v} for v in re.findall(r"ENUMVALUE\s+#(\w+)", text)]
        obj["properties"] = {"label": self._prop_value(text, "LABEL") or ""}

    # ── EDT parser ────────────────────────────────────────────────────────

    def _parse_edt(self, obj: dict, text: str) -> None:
        """Parse an EDT (Extended Data Type) element."""
        obj["properties"] = {
            "label":       self._prop_value(text, "LABEL") or "",
            "help_text":   self._prop_value(text, "HELPTEXT") or "",
            "extends":     self._prop_value(text, "EXTENDS") or "",
            "base_type":   self._prop_value(text, "BASETYPE") or "",
            "string_size": self._prop_value(text, "STRINGSIZE") or "",
        }

    # ── Generic parser ────────────────────────────────────────────────────

    def _parse_generic(self, obj: dict, text: str) -> None:
        """Fallback parser for unknown element types."""
        obj["methods"] = self._extract_source_methods(text)
        obj["properties"] = {
            "label": self._prop_value(text, "LABEL") or "",
        }

    # ── Shared extraction helpers ─────────────────────────────────────────

    def _extract_source_methods(self, text: str) -> list:
        """Extract methods from SOURCE #name ... ENDSOURCE blocks."""
        methods = []
        for m in re.finditer(
            r"SOURCE\s+#(\w+)\s*\r?\n(.*?)ENDSOURCE",
            text, re.DOTALL
        ):
            name = m.group(1)
            raw_source = m.group(2)
            # Strip leading '#' from each source code line
            lines = []
            for line in raw_source.splitlines():
                stripped = line.strip()
                if stripped.startswith("#"):
                    lines.append(stripped[1:])
                elif stripped:
                    lines.append(stripped)
            source = "\n".join(lines)
            methods.append({
                "name": name,
                "source": source,
                "lines": len(lines),
            })
        return methods

    def _section(self, text: str, start_kw: str, end_kw: str) -> str:
        """Extract text between start_kw and end_kw (first occurrence)."""
        pattern = rf"^\s*{re.escape(start_kw)}\s*\r?\n(.*?)^\s*{re.escape(end_kw)}"
        m = re.search(pattern, text, re.MULTILINE | re.DOTALL)
        return m.group(1) if m else ""

    def _prop_value(self, text: str, prop_name: str) -> str:
        """Extract a property value, stripping the '#' prefix."""
        m = re.search(
            rf"^\s*{re.escape(prop_name)}\s+#(.+)$",
            text, re.MULTILINE | re.IGNORECASE
        )
        if m:
            return m.group(1).strip()
        return ""

    def _compute_stats(self) -> None:
        self.summary_stats = {ot: len(objs) for ot, objs in self.objects.items() if objs}


# ──────────────────────────────────────────────────────────────────────────────
#  PDF BUILDER
# ──────────────────────────────────────────────────────────────────────────────

class PDFBuilder:
    COLOR_PRIMARY   = colors.HexColor("#1F3864")
    COLOR_SECONDARY = colors.HexColor("#2E75B6")
    COLOR_ACCENT    = colors.HexColor("#D6E4F0")
    COLOR_CODE_BG   = colors.HexColor("#F5F5F5")
    COLOR_WHITE     = colors.white
    COLOR_BLACK     = colors.black
    COLOR_BORDER    = colors.HexColor("#AAAAAA")

    def __init__(self, parser: XPOParser, output_path: str) -> None:
        self.parser = parser
        self.output_path = output_path
        self.styles = self._build_styles()
        self.story: list = []

    def _build_styles(self):
        custom = {}
        def s(name, **kw):
            custom[name] = ParagraphStyle(name=name, **kw)
        s("H1",  fontName="Helvetica-Bold", fontSize=18, leading=22,
          textColor=self.COLOR_PRIMARY, spaceBefore=16, spaceAfter=8)
        s("H2",  fontName="Helvetica-Bold", fontSize=13, leading=17,
          textColor=self.COLOR_SECONDARY, spaceBefore=12, spaceAfter=6)
        s("H3",  fontName="Helvetica-Bold", fontSize=11, leading=14,
          textColor=self.COLOR_PRIMARY, spaceBefore=8, spaceAfter=4)
        s("Body", fontName="Helvetica", fontSize=9, leading=13,
          textColor=self.COLOR_BLACK, spaceAfter=4, alignment=TA_JUSTIFY)
        s("Code", fontName="Courier", fontSize=7.5, leading=10,
          textColor=colors.HexColor("#1A1A1A"),
          backColor=self.COLOR_CODE_BG, leftIndent=8, rightIndent=8,
          spaceBefore=2, spaceAfter=2,
          borderColor=self.COLOR_BORDER, borderWidth=0.5, borderPad=4)
        s("SummaryBullet", fontName="Helvetica", fontSize=9, leading=13,
          textColor=self.COLOR_BLACK, leftIndent=16, spaceAfter=2)
        s("TableHeader", fontName="Helvetica-Bold", fontSize=8, leading=10,
          textColor=self.COLOR_WHITE, alignment=TA_CENTER)
        s("TableCell", fontName="Helvetica", fontSize=8, leading=10,
          textColor=self.COLOR_BLACK)
        return custom

    def build(self):
        doc = SimpleDocTemplate(
            self.output_path, pagesize=letter,
            leftMargin=0.75 * inch, rightMargin=0.75 * inch,
            topMargin=0.85 * inch, bottomMargin=0.75 * inch,
            title=f"XPO Analysis: {self.parser.filename}",
            author="Dynamics AX 2012 R3 XPO Extractor",
        )
        self._build_cover()
        self._build_summary()
        self._build_toc()
        self._build_tables_section()
        self._build_classes_section()
        self._build_forms_section()
        self._build_queries_section()
        self._build_enums_section()
        self._build_edts_section()
        self._build_other_objects_section()
        self._build_full_source_appendix()
        doc.build(self.story)

    # ── Cover page ────────────────────────────────────────────────────────

    def _build_cover(self):
        self.story.append(Spacer(1, 2.0 * inch))
        self.story.append(Paragraph("Dynamics AX 2012 R3", self.styles["H1"]))
        self.story.append(Paragraph("XPO Export Analysis Report", self.styles["H2"]))
        self.story.append(Spacer(1, 0.3 * inch))
        self.story.append(HRFlowable(
            width="100%", thickness=3, color=self.COLOR_SECONDARY))
        self.story.append(Spacer(1, 0.3 * inch))

        info_lines = [f"<b>Source File:</b> {self.parser.filename}"]
        for k, v in self.parser.export_header.items():
            info_lines.append(f"<b>{k.replace('_', ' ').title()}:</b> {v}")
        info_lines.append(
            f"<b>Report Generated:</b> {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        for line in info_lines:
            self.story.append(Paragraph(line, self.styles["Body"]))

        self.story.append(Spacer(1, 0.5 * inch))
        total = sum(self.parser.summary_stats.values())
        self.story.append(Paragraph(
            f"This report documents <b>{total}</b> AOT object(s) "
            f"extracted from the XPO export.", self.styles["Body"]))
        self.story.append(PageBreak())

    # ── Executive summary ─────────────────────────────────────────────────

    def _build_summary(self):
        self._section_header("Executive Summary")
        p = self.parser

        rows = [["Object Type", "Count", "Names"]]
        for ot, objs in sorted(p.objects.items(), key=lambda x: -len(x[1])):
            names = ", ".join(o["name"] for o in objs[:10])
            if len(objs) > 10:
                names += f"  (+{len(objs) - 10} more)"
            rows.append([ot.title(), str(len(objs)), names])
        self.story.append(self._make_table(
            rows, col_widths=[1.4 * inch, 0.7 * inch, 4.9 * inch]))
        self.story.append(Spacer(1, 10))

        self.story.append(Paragraph("Key Technical Observations", self.styles["H2"]))
        for b in self._generate_bullets():
            self.story.append(
                Paragraph(f"&#8226;  {b}", self.styles["SummaryBullet"]))
        self.story.append(PageBreak())

    def _generate_bullets(self):
        p = self.parser
        bullets = []
        tables  = p.objects.get("TABLE", [])
        classes = p.objects.get("CLASS", [])
        forms   = p.objects.get("FORM", [])

        if tables:
            top3 = sorted(tables, key=lambda t: len(t.get("fields", [])),
                          reverse=True)[:3]
            bullets.append(
                "Largest tables by field count: " +
                ", ".join(f"{t['name']} ({len(t.get('fields', []))} fields)"
                          for t in top3))
            wr = [t for t in tables if t.get("relations")]
            if wr:
                bullets.append(
                    f"{len(wr)} table(s) define foreign-key references: " +
                    ", ".join(t["name"] for t in wr[:5]))
            wi = [t for t in tables if t.get("indexes")]
            if wi:
                bullets.append(
                    f"{len(wi)} table(s) define custom index(es) for query optimisation.")
        if classes:
            top3 = sorted(classes, key=lambda c: len(c.get("methods", [])),
                          reverse=True)[:3]
            bullets.append(
                "Most complex classes: " +
                ", ".join(f"{c['name']} ({len(c.get('methods', []))} methods)"
                          for c in top3))
            ext = [c for c in classes
                   if c.get("properties", {}).get("Extends")]
            if ext:
                bullets.append(
                    f"{len(ext)} class(es) use inheritance (Extends): " +
                    ", ".join(c["name"] + " extends " + c["properties"]["Extends"]
                              for c in ext[:5]))
        if forms:
            bullets.append(
                f"UI layer: {len(forms)} form(s) -- " +
                ", ".join(f["name"] for f in forms[:6]) +
                (" ..." if len(forms) > 6 else ""))
        if not bullets:
            bullets.append(
                "No major structural patterns detected.  "
                "This package appears to be a minimal customisation.")
        return bullets

    # ── Table of contents ─────────────────────────────────────────────────

    def _build_toc(self):
        self.story += [
            Paragraph("Table of Contents", self.styles["H1"]),
            HRFlowable(width="100%", thickness=2, color=self.COLOR_SECONDARY),
            Spacer(1, 6),
        ]
        for item in [
            "1.  Executive Summary",
            "2.  Tables (Fields, Indexes, References, Delete Actions, Methods)",
            "3.  Classes &amp; Methods",
            "4.  Forms",
            "5.  Queries",
            "6.  Enumerations",
            "7.  Extended Data Types",
            "8.  Other AOT Objects",
            "Appendix A -- Full X++ Source Code",
        ]:
            self.story.append(
                Paragraph(f"&nbsp;&nbsp;&nbsp;&nbsp;{item}", self.styles["Body"]))
        self.story.append(PageBreak())

    # ── Section header helper ─────────────────────────────────────────────

    def _section_header(self, title):
        self.story += [
            Paragraph(title, self.styles["H1"]),
            HRFlowable(width="100%", thickness=2, color=self.COLOR_SECONDARY),
            Spacer(1, 6),
        ]

    # ── Tables section ────────────────────────────────────────────────────

    def _build_tables_section(self):
        tables = self.parser.objects.get("TABLE", [])
        if not tables:
            return
        self._section_header("Tables")
        self.story.append(Paragraph(
            f"This section documents the <b>{len(tables)}</b> table(s) defined "
            f"in this XPO, including fields, data types, indexes, foreign-key "
            f"references, delete actions, and methods.",
            self.styles["Body"]))
        self.story.append(Spacer(1, 8))

        for tbl in tables:
            elems = [Paragraph(f"Table: {tbl['name']}", self.styles["H2"])]

            # Table properties
            props = {k: v for k, v in tbl.get("properties", {}).items() if v}
            if props:
                elems.append(self._make_table(
                    [["Property", "Value"]] +
                    [[k, str(v)] for k, v in props.items()],
                    col_widths=[2 * inch, 5 * inch]))
                elems.append(Spacer(1, 6))

            # Fields
            if tbl.get("fields"):
                elems.append(Paragraph(
                    f"Fields ({len(tbl['fields'])})", self.styles["H3"]))
                elems.append(self._make_table(
                    [["Field Name", "Data Type", "EDT / Enum", "Label", "Mandatory"]] +
                    [[f["name"],
                      f["type"],
                      f.get("edt") or f.get("enum_type") or "",
                      f.get("label", ""),
                      f.get("mandatory", "No")]
                     for f in tbl["fields"]],
                    col_widths=[1.6 * inch, 0.8 * inch, 1.6 * inch, 2.0 * inch, 0.7 * inch]))
                elems.append(Spacer(1, 6))

            # Indexes
            if tbl.get("indexes"):
                elems.append(Paragraph(
                    f"Indexes ({len(tbl['indexes'])})", self.styles["H3"]))
                elems.append(self._make_table(
                    [["Index Name", "Fields", "Unique", "Alternate Key"]] +
                    [[ix["name"],
                      ", ".join(ix["fields"]),
                      "Yes" if ix["unique"] else "No",
                      "Yes" if ix.get("alternate_key") else "No"]
                     for ix in tbl["indexes"]],
                    col_widths=[1.8 * inch, 3.0 * inch, 0.8 * inch, 1.1 * inch]))
                elems.append(Spacer(1, 6))

            # Foreign-key references
            if tbl.get("relations"):
                elems.append(Paragraph(
                    f"Foreign-Key References ({len(tbl['relations'])})",
                    self.styles["H3"]))
                elems.append(self._make_table(
                    [["Reference Name", "Related Table", "Cardinality", "Field Mapping"]] +
                    [[r["name"],
                      r["table"],
                      r.get("cardinality", ""),
                      " | ".join(r.get("field_mappings", [])[:3])]
                     for r in tbl["relations"]],
                    col_widths=[1.5 * inch, 1.5 * inch, 1.5 * inch, 2.5 * inch]))
                elems.append(Spacer(1, 6))

            # Delete actions
            if tbl.get("delete_actions"):
                elems.append(Paragraph(
                    f"Delete Actions ({len(tbl['delete_actions'])})",
                    self.styles["H3"]))
                elems.append(self._make_table(
                    [["Related Table", "Action"]] +
                    [[da["table"], da["action"]]
                     for da in tbl["delete_actions"]],
                    col_widths=[4 * inch, 3 * inch]))
                elems.append(Spacer(1, 6))

            # Methods summary
            if tbl.get("methods"):
                elems.append(Paragraph(
                    f"Table Methods ({len(tbl['methods'])}) -- full source in Appendix A",
                    self.styles["H3"]))
                elems.append(self._make_table(
                    [["Method Name", "Lines of X++"]] +
                    [[m["name"], str(m["lines"])] for m in tbl["methods"]],
                    col_widths=[5 * inch, 2 * inch]))

            self.story.append(KeepTogether(elems[:4]))
            for el in elems[4:]:
                self.story.append(el)
            self.story += [
                HRFlowable(width="100%", thickness=0.5, color=self.COLOR_BORDER),
                Spacer(1, 10),
            ]
        self.story.append(PageBreak())

    # ── Classes section ───────────────────────────────────────────────────

    def _build_classes_section(self):
        classes = self.parser.objects.get("CLASS", [])
        if not classes:
            return
        self._section_header("Classes &amp; Methods")
        for cls in classes:
            self.story.append(
                Paragraph(f"Class: {cls['name']}", self.styles["H2"]))
            props = {k: v for k, v in cls.get("properties", {}).items() if v}
            if props:
                self.story.append(self._make_table(
                    [["Property", "Value"]] +
                    [[k, str(v)] for k, v in props.items()],
                    col_widths=[2 * inch, 5 * inch]))
                self.story.append(Spacer(1, 6))
            if cls.get("methods"):
                self.story.append(self._make_table(
                    [["Method Name", "Lines of X++"]] +
                    [[m["name"], str(m["lines"])] for m in cls["methods"]],
                    col_widths=[5 * inch, 2 * inch]))
            self.story += [
                HRFlowable(width="100%", thickness=0.5, color=self.COLOR_BORDER),
                Spacer(1, 10),
            ]
        self.story.append(PageBreak())

    # ── Forms section ─────────────────────────────────────────────────────

    def _build_forms_section(self):
        forms = self.parser.objects.get("FORM", [])
        if not forms:
            return
        self._section_header("Forms")
        for frm in forms:
            self.story.append(
                Paragraph(f"Form: {frm['name']}", self.styles["H2"]))
            if frm.get("data_sources"):
                self.story.append(Paragraph(
                    f"<b>Data Sources:</b> {', '.join(frm['data_sources'])}",
                    self.styles["Body"]))
            props = {k: v for k, v in frm.get("properties", {}).items() if v}
            if props:
                self.story.append(self._make_table(
                    [["Property", "Value"]] +
                    [[k, str(v)] for k, v in props.items()],
                    col_widths=[2 * inch, 5 * inch]))
                self.story.append(Spacer(1, 4))
            if frm.get("methods"):
                self.story.append(self._make_table(
                    [["Method Name", "Lines of X++"]] +
                    [[m["name"], str(m["lines"])] for m in frm["methods"]],
                    col_widths=[5 * inch, 2 * inch]))
            self.story += [
                HRFlowable(width="100%", thickness=0.5, color=self.COLOR_BORDER),
                Spacer(1, 10),
            ]
        self.story.append(PageBreak())

    # ── Queries section ───────────────────────────────────────────────────

    def _build_queries_section(self):
        queries = self.parser.objects.get("QUERY", [])
        if not queries:
            return
        self._section_header("Queries")
        self.story.append(self._make_table(
            [["Query Name", "Title", "Data Sources"]] +
            [[q["name"],
              q.get("properties", {}).get("title") or "",
              ", ".join(q.get("data_sources", []))]
             for q in queries],
            col_widths=[2.3 * inch, 2.3 * inch, 2.4 * inch]))
        self.story.append(PageBreak())

    # ── Enums section ─────────────────────────────────────────────────────

    def _build_enums_section(self):
        enums = self.parser.objects.get("ENUM", [])
        if not enums:
            return
        self._section_header("Enumerations")
        for en in enums:
            self.story.append(
                Paragraph(f"Enum: {en['name']}", self.styles["H2"]))
            for k, v in {k: v for k, v in en.get("properties", {}).items()
                         if v}.items():
                self.story.append(Paragraph(
                    f"<b>{k.replace('_', ' ').title()}:</b> {v}",
                    self.styles["Body"]))
            if en.get("enums"):
                self.story.append(self._make_table(
                    [["Enum Value Name"]] +
                    [[ev["name"]] for ev in en["enums"]],
                    col_widths=[7 * inch]))
            self.story += [
                HRFlowable(width="100%", thickness=0.5, color=self.COLOR_BORDER),
                Spacer(1, 8),
            ]
        self.story.append(PageBreak())

    # ── EDTs section ──────────────────────────────────────────────────────

    def _build_edts_section(self):
        edts = (self.parser.objects.get("EDT", []) +
                self.parser.objects.get("EXTENDEDDATATYPE", []))
        if not edts:
            return
        self._section_header("Extended Data Types (EDT)")
        self.story.append(self._make_table(
            [["EDT Name", "Base Type", "Extends", "Label", "Help Text"]] +
            [[e["name"],
              e.get("properties", {}).get("base_type") or "",
              e.get("properties", {}).get("extends") or "",
              e.get("properties", {}).get("label") or "",
              (e.get("properties", {}).get("help_text") or "")[:80]]
             for e in edts],
            col_widths=[1.5 * inch, 1.1 * inch, 1.3 * inch, 1.5 * inch, 1.6 * inch]))
        self.story.append(PageBreak())

    # ── Other objects section ─────────────────────────────────────────────

    def _build_other_objects_section(self):
        skip = {"TABLE", "CLASS", "FORM", "QUERY", "ENUM", "EDT", "EXTENDEDDATATYPE"}
        others = {k: v for k, v in self.parser.objects.items()
                  if k not in skip and v}
        if not others:
            return
        self._section_header("Other AOT Objects")
        for ot, objs in sorted(others.items()):
            self.story.append(
                Paragraph(f"{ot.title()} ({len(objs)})", self.styles["H2"]))
            self.story.append(self._make_table(
                [["Name", "Key Properties"]] +
                [[o["name"],
                  "; ".join(f"{k}={v}"
                            for k, v in (o.get("properties") or {}).items()
                            if v)[:120]]
                 for o in objs],
                col_widths=[2.5 * inch, 4.5 * inch]))
            self.story.append(Spacer(1, 8))
        self.story.append(PageBreak())

    # ── Full source appendix ──────────────────────────────────────────────

    def _build_full_source_appendix(self):
        self._section_header("Appendix A -- Full X++ Source Code")
        self.story.append(Paragraph(
            "Complete X++ source code for every method found in the XPO, "
            "grouped by parent object.", self.styles["Body"]))
        self.story.append(Spacer(1, 8))

        for obj_type, objs in sorted(self.parser.objects.items()):
            for obj in objs:
                if not obj.get("methods"):
                    continue
                self.story.append(Paragraph(
                    f"{obj_type.title()}: {obj['name']}", self.styles["H2"]))
                for method in obj["methods"]:
                    self.story.append(Paragraph(
                        f"Method: <b>{method['name']}</b> "
                        f"<font color='#777777'>({method['lines']} lines)</font>",
                        self.styles["H3"]))
                    safe = (method["source"]
                            .replace("&", "&amp;")
                            .replace("<", "&lt;")
                            .replace(">", "&gt;"))
                    lines = safe.split("\n")
                    for i in range(0, len(lines), 60):
                        chunk = "\n".join(lines[i:i + 60])
                        self.story.append(Paragraph(
                            chunk.replace("\n", "<br/>").replace(" ", "&nbsp;"),
                            self.styles["Code"]))
                    self.story.append(Spacer(1, 6))
                self.story += [
                    HRFlowable(width="100%", thickness=0.5,
                               color=self.COLOR_BORDER),
                    Spacer(1, 6),
                ]

    # ── Table helper ──────────────────────────────────────────────────────

    def _make_table(self, data, col_widths=None):
        formatted = []
        for r_idx, row in enumerate(data):
            fmt_row = []
            for cell in row:
                text = str(cell) if cell is not None else ""
                text = (text.replace("&", "&amp;")
                            .replace("<", "&lt;")
                            .replace(">", "&gt;"))
                style = (self.styles["TableHeader"] if r_idx == 0
                         else self.styles["TableCell"])
                fmt_row.append(Paragraph(text, style))
            formatted.append(fmt_row)
        tbl = Table(formatted, colWidths=col_widths, repeatRows=1, hAlign="LEFT")
        tbl.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), self.COLOR_SECONDARY),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1),
             [self.COLOR_WHITE, self.COLOR_ACCENT]),
            ("TEXTCOLOR", (0, 0), (-1, 0), self.COLOR_WHITE),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ("LEFTPADDING", (0, 0), (-1, -1), 6),
            ("GRID", (0, 0), (-1, -1), 0.4, self.COLOR_BORDER),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ]))
        return tbl


# ──────────────────────────────────────────────────────────────────────────────
#  GUI APPLICATION
# ──────────────────────────────────────────────────────────────────────────────

class App(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Dynamics AX 2012 R3 -- XPO Extractor")
        self.geometry("660x440")
        self.resizable(False, False)
        self.configure(bg="#1F3864")
        self.xpo_path = None
        self.out_path = None
        self._build_ui()

    def _build_ui(self):
        tk.Label(self, text="Dynamics AX 2012 R3",
                 font=("Helvetica", 18, "bold"), bg="#1F3864",
                 fg="white").pack(pady=(22, 2))
        tk.Label(self, text="XPO File Extractor  &  PDF Report Generator",
                 font=("Helvetica", 11), bg="#1F3864",
                 fg="#D6E4F0").pack(pady=(0, 18))

        card = tk.Frame(self, bg="white", padx=22, pady=18)
        card.pack(fill="both", expand=True, padx=28, pady=(0, 22))
        card.columnconfigure(0, weight=1)

        tk.Label(card, text="XPO File:", font=("Helvetica", 10, "bold"),
                 bg="white").grid(row=0, column=0, sticky="w", pady=(0, 2))
        self.file_var = tk.StringVar(value="No file selected")
        tk.Label(card, textvariable=self.file_var, font=("Helvetica", 9),
                 bg="#F0F0F0", relief="groove", anchor="w", padx=6, pady=4,
                 width=50).grid(row=1, column=0, sticky="ew", pady=(0, 8))
        tk.Button(card, text="Browse...", command=self._browse,
                  font=("Helvetica", 10), bg="#2E75B6", fg="white",
                  activebackground="#1F3864", activeforeground="white",
                  relief="flat", padx=10, pady=5,
                  cursor="hand2").grid(row=1, column=1, padx=(8, 0),
                                       pady=(0, 8), sticky="e")

        tk.Label(card, text="Output PDF:", font=("Helvetica", 10, "bold"),
                 bg="white").grid(row=2, column=0, sticky="w", pady=(4, 2))
        self.out_var = tk.StringVar(value="Auto -- saved beside the XPO file")
        tk.Label(card, textvariable=self.out_var, font=("Helvetica", 9),
                 bg="#F0F0F0", relief="groove", anchor="w", padx=6, pady=4,
                 width=50).grid(row=3, column=0, sticky="ew", pady=(0, 8))
        tk.Button(card, text="Choose...", command=self._choose_output,
                  font=("Helvetica", 10), bg="#2E75B6", fg="white",
                  activebackground="#1F3864", activeforeground="white",
                  relief="flat", padx=10, pady=5,
                  cursor="hand2").grid(row=3, column=1, padx=(8, 0),
                                       pady=(0, 8), sticky="e")

        self.progress = ttk.Progressbar(card, mode="indeterminate", length=480)
        self.progress.grid(row=4, column=0, columnspan=2, pady=(6, 4),
                           sticky="ew")
        self.status_var = tk.StringVar(
            value="Ready -- select an XPO file to begin.")
        tk.Label(card, textvariable=self.status_var, font=("Helvetica", 9),
                 bg="white", fg="#444444").grid(row=5, column=0, columnspan=2,
                                                sticky="w", pady=(0, 4))

        self.go_btn = tk.Button(
            card, text="Extract & Generate PDF", command=self._run,
            font=("Helvetica", 12, "bold"), bg="#1F3864", fg="white",
            activebackground="#2E75B6", activeforeground="white",
            relief="flat", padx=16, pady=10, cursor="hand2")
        self.go_btn.grid(row=6, column=0, columnspan=2, pady=(10, 0),
                         sticky="ew")

    def _browse(self):
        path = filedialog.askopenfilename(
            title="Select Dynamics AX XPO File",
            filetypes=[("XPO Files", "*.xpo"), ("All Files", "*.*")])
        if path:
            self.xpo_path = path
            self.file_var.set(path)
            auto = os.path.splitext(path)[0] + "_XPO_Analysis.pdf"
            self.out_path = auto
            self.out_var.set(auto)
            self.status_var.set(f"Loaded: {os.path.basename(path)}")

    def _choose_output(self):
        path = filedialog.asksaveasfilename(
            title="Save PDF Report As", defaultextension=".pdf",
            filetypes=[("PDF Files", "*.pdf"), ("All Files", "*.*")])
        if path:
            self.out_path = path
            self.out_var.set(path)

    def _run(self):
        if not self.xpo_path:
            messagebox.showwarning(
                "No File Selected", "Please select an XPO file first.")
            return
        if not self.out_path:
            self.out_path = (os.path.splitext(self.xpo_path)[0] +
                             "_XPO_Analysis.pdf")
        self.go_btn.config(state="disabled")
        self.progress.start(10)
        self.status_var.set("Step 1/3 -- Parsing XPO file...")
        self.update()
        try:
            parser = XPOParser(self.xpo_path)
            parser.parse()
            total = sum(parser.summary_stats.values())
            self.status_var.set(
                f"Step 2/3 -- Analysed {total} objects. Building PDF...")
            self.update()
            builder = PDFBuilder(parser, self.out_path)
            builder.build()
            self.progress.stop()
            self.status_var.set(f"Done!  PDF saved: {self.out_path}")
            self.go_btn.config(state="normal")
            messagebox.showinfo(
                "Success",
                f"PDF report generated successfully!\n\n{self.out_path}")
            if sys.platform.startswith("win"):
                os.startfile(self.out_path)
            elif sys.platform == "darwin":
                os.system(f'open "{self.out_path}"')
            else:
                os.system(f'xdg-open "{self.out_path}"')
        except Exception as exc:
            self.progress.stop()
            self.go_btn.config(state="normal")
            self.status_var.set(f"Error: {exc}")
            messagebox.showerror(
                "Extraction Error",
                f"An error occurred:\n\n{exc}\n\n"
                f"Verify this is a valid AX 2012 R3 XPO export.")
            raise


if __name__ == "__main__":
    app = App()
    app.mainloop()
