"""Report download endpoints: Excel and PDF.

Reports are generated on-demand from data previously stored in the
in-memory report store (``reports.py``).  They are served as file
downloads and automatically expire after the TTL.
"""

from __future__ import annotations

import os
import tempfile
import time

import pandas as pd
from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
    Paragraph,
)

from reports import REPORT_TTL_SECONDS, get_report

router = APIRouter(tags=["reports"])


@router.get("/report/{report_id}/excel")
def download_excel(report_id: str):
    rdata = get_report(report_id)
    if not rdata:
        raise HTTPException(status_code=404, detail="Report not found or expired.")

    try:
        df = pd.DataFrame(rdata["data"])
        df = df.where(pd.notnull(df), None)

        tmp_dir = tempfile.gettempdir()
        tmp_path = os.path.join(tmp_dir, f"report_{report_id}.xlsx")

        with pd.ExcelWriter(tmp_path, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Report")
            worksheet = writer.sheets["Report"]
            for col_cells in worksheet.columns:
                max_length = max(
                    (len(str(cell.value)) if cell.value is not None else 0)
                    for cell in col_cells
                )
                col_letter = col_cells[0].column_letter
                worksheet.column_dimensions[col_letter].width = min(
                    max_length + 2, 50
                )

        return FileResponse(
            path=tmp_path,
            media_type=(
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            ),
            filename=f"library_report_{report_id[:8]}.xlsx",
        )
    except Exception as e:
        print(f"[EXCEL ERROR] {e}")
        raise HTTPException(
            status_code=500, detail=f"Excel generation failed: {e}",
        )


@router.get("/report/{report_id}/pdf")
def download_pdf(report_id: str):
    rdata = get_report(report_id)
    if not rdata:
        raise HTTPException(status_code=404, detail="Report not found or expired.")

    try:
        data = rdata["data"]
        question = rdata.get("question", "")
        df = pd.DataFrame(data)

        tmp_dir = tempfile.gettempdir()
        tmp_path = os.path.join(tmp_dir, f"report_{report_id}.pdf")

        doc = SimpleDocTemplate(
            tmp_path,
            pagesize=landscape(A4),
            rightMargin=30,
            leftMargin=30,
            topMargin=30,
            bottomMargin=30,
        )

        styles = getSampleStyleSheet()
        title_style = ParagraphStyle(
            "CustomTitle",
            parent=styles["Title"],
            fontSize=16,
            textColor=colors.HexColor("#1a5276"),
            spaceAfter=6,
        )
        sub_style = ParagraphStyle(
            "CustomSub",
            parent=styles["Normal"],
            fontSize=9,
            textColor=colors.grey,
            spaceAfter=12,
        )

        elements = []
        elements.append(Paragraph("SOUL 3.0 Library Report", title_style))
        elements.append(Paragraph(f"<b>Question:</b> {question}", sub_style))
        elements.append(
            Paragraph(
                f"<b>Generated:</b> {time.strftime('%d-%m-%Y %H:%M:%S')}  |  "
                f"<b>Rows:</b> {len(df)}",
                sub_style,
            )
        )
        elements.append(Spacer(1, 10))

        col_headers = [str(c) for c in df.columns]
        table_data = [col_headers]
        for _, row in df.iterrows():
            row_vals = []
            for v in row.tolist():
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    row_vals.append("")
                elif isinstance(v, pd.Timestamp):
                    row_vals.append(v.strftime("%d-%m-%Y %H:%M"))
                else:
                    s = str(v)
                    if len(s) > 80:
                        s = s[:77] + "..."
                    row_vals.append(s)
            table_data.append(row_vals)

        page_width = landscape(A4)[0] - 60
        n_cols = max(len(col_headers), 1)
        col_width = min(page_width / n_cols, 3 * inch)
        col_widths = [col_width] * n_cols

        table = Table(table_data, repeatRows=1, colWidths=col_widths)
        table.setStyle(
            TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1a5276")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, 0), 9),
                ("ALIGN", (0, 0), (-1, 0), "CENTER"),
                ("BOTTOMPADDING", (0, 0), (-1, 0), 8),
                ("BACKGROUND", (0, 1), (-1, -1), colors.HexColor("#f8f9fa")),
                (
                    "ROWBACKGROUNDS",
                    (0, 1),
                    (-1, -1),
                    [colors.white, colors.HexColor("#eef2f7")],
                ),
                ("FONTSIZE", (0, 1), (-1, -1), 8),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#bdc3c7")),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("LEFTPADDING", (0, 0), (-1, -1), 4),
                ("RIGHTPADDING", (0, 0), (-1, -1), 4),
            ])
        )

        elements.append(table)
        doc.build(elements)

        return FileResponse(
            path=tmp_path,
            media_type="application/pdf",
            filename=f"library_report_{report_id[:8]}.pdf",
        )
    except Exception as e:
        print(f"[PDF ERROR] {e}")
        raise HTTPException(
            status_code=500, detail=f"PDF generation failed: {e}",
        )


@router.get("/report/{report_id}/status")
def report_status(report_id: str):
    rdata = get_report(report_id)
    if not rdata:
        return {"valid": False, "detail": "Report not found or expired."}
    elapsed = time.time() - rdata["created_at"]
    remaining = max(0, REPORT_TTL_SECONDS - elapsed)
    return {
        "valid": True,
        "rows": len(rdata["data"]),
        "question": rdata["question"],
        "remaining_seconds": int(remaining),
    }
