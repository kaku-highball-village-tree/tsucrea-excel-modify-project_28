# -*- coding: utf-8 -*-
"""
make_manhour_to_sheet8_01_0002.py

役割:
  単一のジョブカン工数 CSV を入力として、
  工数_yyyy年mm月.tsv を同一フォルダに生成する。

実行例:
  python make_manhour_to_sheet8_01_0002.py manhour_xxxxxx.csv
"""

from __future__ import annotations

import csv
import os
import re
import sys
from pathlib import Path
from typing import List, Tuple


def write_error_text_utf8(pszErrorFilePath: str, pszText: str) -> None:
    with open(pszErrorFilePath, mode="a", encoding="utf-8") as objFile:
        objFile.write(pszText)


def get_target_year_month_from_filename(pszInputFilePath: str) -> Tuple[int, int]:
    pszBaseName: str = os.path.basename(pszInputFilePath)
    objMatch: re.Match[str] | None = re.search(r"(\d{2})\.(\d{1,2})\.csv$", pszBaseName)
    if objMatch is None:
        raise ValueError("入力ファイル名から対象年月を取得できません。")
    iYearTwoDigits: int = int(objMatch.group(1))
    iMonth: int = int(objMatch.group(2))
    iYear: int = 2000 + iYearTwoDigits
    return iYear, iMonth


def build_output_file_full_path(pszInputFileFullPath: str, pszOutputSuffix: str) -> str:
    pszDirectory: str = os.path.dirname(pszInputFileFullPath)
    pszBaseName: str = os.path.basename(pszInputFileFullPath)
    pszStem: str = os.path.splitext(pszBaseName)[0]
    pszOutputFileName: str = pszStem + pszOutputSuffix
    return os.path.join(pszDirectory, pszOutputFileName)


def normalize_time_h_mm_to_h_mm_ss(pszTimeText: str) -> str:
    pszText: str = (pszTimeText or "").strip()
    if pszText == "":
        return ""
    if pszText.count(":") == 2:
        return pszText
    if pszText.count(":") == 1:
        return pszText + ":00"
    return pszText


def convert_csv_to_tsv_file(pszInputCsvPath: str) -> str:
    if not os.path.exists(pszInputCsvPath):
        raise FileNotFoundError(f"Input CSV not found: {pszInputCsvPath}")

    pszOutputTsvPath: str = build_output_file_full_path(pszInputCsvPath, ".tsv")

    objRows: List[List[str]] = []
    arrEncodings: List[str] = ["utf-8-sig", "cp932"]
    objLastDecodeError: Exception | None = None

    for pszEncoding in arrEncodings:
        try:
            with open(
                pszInputCsvPath,
                mode="r",
                encoding=pszEncoding,
                newline="",
            ) as objInputFile:
                objReader: csv.reader = csv.reader(objInputFile)
                for objRow in objReader:
                    objRows.append(list(objRow))
            objLastDecodeError = None
            break
        except UnicodeDecodeError as objError:
            objLastDecodeError = objError
            objRows = []

    if objLastDecodeError is not None:
        raise objLastDecodeError

    if len(objRows) <= 1:
        with open(pszOutputTsvPath, mode="w", encoding="utf-8", newline="") as objOutputFile:
            objWriter: csv.writer = csv.writer(objOutputFile, delimiter="\t")
            for objRow in objRows:
                objWriter.writerow(objRow)
        return pszOutputTsvPath

    iTimeColumnIndexF: int = 5
    iTimeColumnIndexK: int = 10

    for iRowIndex in range(1, len(objRows)):
        objRow: List[str] = objRows[iRowIndex]
        if iTimeColumnIndexF < len(objRow):
            objRow[iTimeColumnIndexF] = normalize_time_h_mm_to_h_mm_ss(objRow[iTimeColumnIndexF])
        if iTimeColumnIndexK < len(objRow):
            objRow[iTimeColumnIndexK] = normalize_time_h_mm_to_h_mm_ss(objRow[iTimeColumnIndexK])
        objRows[iRowIndex] = objRow

    if len(objRows) >= 1 and len(objRows[0]) >= 1:
        pszHeaderFirstCell: str = objRows[0][0]
        if pszHeaderFirstCell.startswith("\ufeff"):
            pszHeaderFirstCell = pszHeaderFirstCell.lstrip("\ufeff")
        if (
            len(pszHeaderFirstCell) >= 2
            and pszHeaderFirstCell.startswith('"')
            and pszHeaderFirstCell.endswith('"')
        ):
            pszHeaderFirstCell = pszHeaderFirstCell[1:-1]
            pszHeaderFirstCell = pszHeaderFirstCell.replace('""', '"')
        if (
            len(pszHeaderFirstCell) >= 2
            and pszHeaderFirstCell.startswith('"')
            and pszHeaderFirstCell.endswith('"')
        ):
            pszHeaderFirstCell = pszHeaderFirstCell[1:-1]
        objRows[0][0] = pszHeaderFirstCell

    with open(pszOutputTsvPath, mode="w", encoding="utf-8", newline="") as objOutputFile:
        objWriter: csv.writer = csv.writer(objOutputFile, delimiter="\t")
        for objRow in objRows:
            objWriter.writerow(objRow)

    return pszOutputTsvPath


def process_single_input(pszInputManhourCsvPath: str) -> int:
    objInputPath: Path = Path(pszInputManhourCsvPath)
    objCandidatePaths: List[Path] = [objInputPath]

    objScriptDirectoryPath: Path = Path(__file__).resolve().parent
    objCandidatePaths.append(objScriptDirectoryPath / pszInputManhourCsvPath)

    objInputDirectoryPath: Path = Path.cwd() / "input"
    objCandidatePaths.append(objInputDirectoryPath / pszInputManhourCsvPath)

    if objInputPath.suffix.lower() == ".tsv":
        pszCsvFileName: str = objInputPath.with_suffix(".csv").name
        objCandidatePaths.append(objInputPath.with_suffix(".csv"))
        objCandidatePaths.append(objScriptDirectoryPath / pszCsvFileName)
        objCandidatePaths.append(objInputDirectoryPath / pszCsvFileName)

    objExistingPaths: List[Path] = [objPath for objPath in objCandidatePaths if objPath.exists()]
    if len(objExistingPaths) > 0:
        objInputPath = objExistingPaths[0]

    if not objInputPath.exists():
        pszErrorTextFilePath: str = str(Path.cwd() / "make_manhour_to_sheet8_01_0002_error.txt")
        write_error_text_utf8(
            pszErrorTextFilePath,
            f"Error: input file not found: {pszInputManhourCsvPath}\n"
            f"CurrentDirectory: {str(Path.cwd())}\n",
        )
        raise FileNotFoundError(f"Input file not found: {pszInputManhourCsvPath}")

    objBaseDirectoryPath: Path = objInputPath.resolve().parent

    pszStep1DefaultTsvPath: str = convert_csv_to_tsv_file(str(objInputPath))
    iFileYear, iFileMonth = get_target_year_month_from_filename(str(objInputPath))
    pszStep1TsvPath: str = str(
        objBaseDirectoryPath / f"工数_{iFileYear}年{iFileMonth:02d}月.tsv"
    )
    if pszStep1DefaultTsvPath != pszStep1TsvPath:
        os.replace(pszStep1DefaultTsvPath, pszStep1TsvPath)

    return 0


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: python make_manhour_to_sheet8_01_0002.py <input_manhour_csv>")
        return 1

    pszInputCsvPath: str = sys.argv[1]
    return process_single_input(pszInputCsvPath)


if __name__ == "__main__":
    raise SystemExit(main())
