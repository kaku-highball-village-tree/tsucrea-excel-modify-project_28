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

import argparse
import csv
import os
import re
import sys
from pathlib import Path
from typing import List, Tuple

import pandas as pd
from pandas import DataFrame


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
        if len(objRows[0]) >= 4 and objRows[0][3] == "所属グループ名":
            objRows[0][3] = "所属カンパニー名"

    with open(pszOutputTsvPath, mode="w", encoding="utf-8", newline="") as objOutputFile:
        objWriter: csv.writer = csv.writer(objOutputFile, delimiter="\t")
        for objRow in objRows:
            objWriter.writerow(objRow)

    return pszOutputTsvPath


def write_error_tsv(pszOutputFileFullPath: str, pszErrorMessage: str) -> None:
    pszDirectory: str = os.path.dirname(pszOutputFileFullPath)
    if len(pszDirectory) > 0:
        os.makedirs(pszDirectory, exist_ok=True)

    with open(pszOutputFileFullPath, "w", encoding="utf-8") as objFile:
        objFile.write(pszErrorMessage)
        if not pszErrorMessage.endswith("\n"):
            objFile.write("\n")


def build_removed_uninput_output_path(pszInputFileFullPath: str) -> str:
    pszDirectory: str = os.path.dirname(pszInputFileFullPath)
    pszBaseName: str = os.path.basename(pszInputFileFullPath)
    pszRootName: str
    pszExt: str
    pszRootName, pszExt = os.path.splitext(pszBaseName)

    pszOutputBaseName: str = pszRootName + "_step0001_removed_uninput.tsv"
    if len(pszDirectory) == 0:
        return pszOutputBaseName
    return os.path.join(pszDirectory, pszOutputBaseName)


def make_removed_uninput_tsv_from_manhour_tsv(pszInputFileFullPath: str) -> None:
    if not os.path.isfile(pszInputFileFullPath):
        pszDirectory: str = os.path.dirname(pszInputFileFullPath)
        pszBaseName: str = os.path.basename(pszInputFileFullPath)
        pszRootName: str
        pszExt: str
        pszRootName, pszExt = os.path.splitext(pszBaseName)
        pszErrorFileFullPath: str = os.path.join(
            pszDirectory,
            pszRootName + "_error.tsv",
        )

        write_error_tsv(
            pszErrorFileFullPath,
            "Error: input TSV file not found. Path = {0}".format(
                pszInputFileFullPath
            ),
        )
        return

    pszOutputFileFullPath: str = build_removed_uninput_output_path(pszInputFileFullPath)

    try:
        objDataFrame: DataFrame = pd.read_csv(
            pszInputFileFullPath,
            sep="\t",
            encoding="utf-8",
            dtype=str,
            keep_default_na=False,
            engine="python",
        )
    except Exception as objException:
        write_error_tsv(
            pszOutputFileFullPath,
            "Error: unexpected exception while reading TSV for removing '未入力'. "
            "Detail = {0}".format(objException),
        )
        return

    iColumnCount: int = objDataFrame.shape[1]
    if iColumnCount < 10:
        write_error_tsv(
            pszOutputFileFullPath,
            "Error: required columns G-J do not exist (need at least 10 columns). "
            "ColumnCount = {0}".format(iColumnCount),
        )
        return

    objColumnNameList: List[str] = list(objDataFrame.columns)
    pszColumnG: str = objColumnNameList[6]
    pszColumnH: str = objColumnNameList[7]
    pszColumnI: str = objColumnNameList[8]
    pszColumnJ: str = objColumnNameList[9]

    try:
        objSeriesHasUninputG = (
            objDataFrame[pszColumnG].fillna("").astype(str).str.strip() == "未入力"
        )
        objSeriesHasUninputH = (
            objDataFrame[pszColumnH].fillna("").astype(str).str.strip() == "未入力"
        )
        objSeriesHasUninputI = (
            objDataFrame[pszColumnI].fillna("").astype(str).str.strip() == "未入力"
        )
        objSeriesHasUninputJ = (
            objDataFrame[pszColumnJ].fillna("").astype(str).str.strip() == "未入力"
        )

        objSeriesHasUninputAny = (
            objSeriesHasUninputG
            | objSeriesHasUninputH
            | objSeriesHasUninputI
            | objSeriesHasUninputJ
        )

        objDataFrameFiltered: DataFrame = objDataFrame.loc[~objSeriesHasUninputAny].copy()
    except Exception as objException:
        write_error_tsv(
            pszOutputFileFullPath,
            "Error: unexpected exception while filtering rows with '未入力'. "
            "Detail = {0}".format(objException),
        )
        return

    try:
        objDataFrameFiltered.to_csv(
            pszOutputFileFullPath,
            sep="\t",
            index=False,
            encoding="utf-8",
            lineterminator="\n",
        )
    except Exception as objException:
        write_error_tsv(
            pszOutputFileFullPath,
            "Error: unexpected exception while writing TSV without '未入力' rows. "
            "Detail = {0}".format(objException),
        )
        return


def build_sorted_staff_code_output_path(pszInputFileFullPath: str) -> str:
    pszDirectory: str = os.path.dirname(pszInputFileFullPath)
    pszBaseName: str = os.path.basename(pszInputFileFullPath)
    pszRootName: str
    pszExt: str
    pszRootName, pszExt = os.path.splitext(pszBaseName)

    pszStep0001Suffix: str = "_step0001_removed_uninput"
    if pszRootName.endswith(pszStep0001Suffix):
        pszRootName = pszRootName[: -len(pszStep0001Suffix)]
    pszOutputBaseName: str = (
        pszRootName + "_step0002_removed_uninput_sorted_staff_code.tsv"
    )
    if len(pszDirectory) == 0:
        return pszOutputBaseName
    return os.path.join(pszDirectory, pszOutputBaseName)


def make_sorted_staff_code_tsv_from_manhour_tsv(pszInputFileFullPath: str) -> None:
    if not os.path.isfile(pszInputFileFullPath):
        raise FileNotFoundError(f"Input TSV not found: {pszInputFileFullPath}")

    pszOutputFileFullPath: str = build_sorted_staff_code_output_path(pszInputFileFullPath)

    try:
        objDataFrame: DataFrame = pd.read_csv(
            pszInputFileFullPath,
            sep="\t",
            dtype=str,
            encoding="utf-8",
            engine="python",
        )
    except Exception as objException:
        write_error_tsv(
            pszOutputFileFullPath,
            "Error: unexpected exception while reading manhour TSV for staff code sort. "
            "Detail = {0}".format(objException),
        )
        return

    iColumnCount: int = objDataFrame.shape[1]
    if iColumnCount < 2:
        write_error_tsv(
            pszOutputFileFullPath,
            "Error: staff code column (2nd column) does not exist. ColumnCount = {0}".format(
                iColumnCount
            ),
        )
        return

    objColumnNameList: List[str] = list(objDataFrame.columns)
    pszSortColumnName: str = objColumnNameList[1]

    try:
        objSorted: DataFrame = objDataFrame.copy()
        objSorted["__sort_staff_code__"] = pd.to_numeric(
            objSorted[pszSortColumnName],
            errors="coerce",
        )
        objSorted = objSorted.sort_values(
            by="__sort_staff_code__",
            ascending=True,
            kind="mergesort",
        ).drop(columns=["__sort_staff_code__"])
    except Exception as objException:
        write_error_tsv(
            pszOutputFileFullPath,
            "Error: unexpected exception while sorting by staff code. Detail = {0}".format(
                objException
            ),
        )
        return

    try:
        objSorted.to_csv(
            pszOutputFileFullPath,
            sep="\t",
            index=False,
            encoding="utf-8",
            lineterminator="\n",
        )
    except Exception as objException:
        write_error_tsv(
            pszOutputFileFullPath,
            "Error: unexpected exception while writing sorted staff-code TSV. Detail = {0}".format(
                objException
            ),
        )
        return


def step0003_normalize_company_name(pszCompanyName: str) -> str:
    objReplaceTargets: List[Tuple[str, str]] = [
        ("本部", "本部"),
        ("事業開発", "事業開発"),
        ("子会社", "子会社"),
        ("投資先", "投資先"),
        ("第１インキュ", "第一インキュ"),
        ("第２インキュ", "第二インキュ"),
        ("第３インキュ", "第三インキュ"),
        ("第４インキュ", "第四インキュ"),
        ("第1インキュ", "第一インキュ"),
        ("第2インキュ", "第二インキュ"),
        ("第3インキュ", "第三インキュ"),
        ("第4インキュ", "第四インキュ"),
    ]
    for pszPrefix, pszReplacement in objReplaceTargets:
        if pszCompanyName.startswith(pszPrefix):
            return pszReplacement
    return pszCompanyName


def build_step0003_company_normalized_output_path(pszInputFileFullPath: str) -> str:
    pszDirectory: str = os.path.dirname(pszInputFileFullPath)
    pszBaseName: str = os.path.basename(pszInputFileFullPath)
    pszRootName: str
    pszExt: str
    pszRootName, pszExt = os.path.splitext(pszBaseName)

    pszStep0002Suffix: str = "_step0002_removed_uninput_sorted_staff_code"
    if pszRootName.endswith(pszStep0002Suffix):
        pszRootName = pszRootName[: -len(pszStep0002Suffix)]
    pszOutputBaseName: str = pszRootName + "_step0004_normalized_company_name.tsv"
    if len(pszDirectory) == 0:
        return pszOutputBaseName
    return os.path.join(pszDirectory, pszOutputBaseName)


def make_company_normalized_tsv_from_step0002(pszInputFileFullPath: str) -> None:
    if not os.path.isfile(pszInputFileFullPath):
        raise FileNotFoundError(f"Input TSV not found: {pszInputFileFullPath}")

    pszOutputFileFullPath: str = build_step0003_company_normalized_output_path(
        pszInputFileFullPath
    )

    try:
        objDataFrame: DataFrame = pd.read_csv(
            pszInputFileFullPath,
            sep="\t",
            dtype=str,
            encoding="utf-8",
            keep_default_na=False,
            engine="python",
        )
    except Exception as objException:
        write_error_tsv(
            pszOutputFileFullPath,
            "Error: unexpected exception while reading step0002 TSV for company name normalization. "
            "Detail = {0}".format(objException),
        )
        return

    objColumnNameList: List[str] = list(objDataFrame.columns)
    objCandidateColumns: List[str] = [
        "計上カンパニー名",
        "計上カンパニー",
        "所属カンパニー",
        "所属カンパニー名",
    ]
    pszCompanyColumn: str | None = None
    for pszCandidate in objCandidateColumns:
        if pszCandidate in objColumnNameList:
            pszCompanyColumn = pszCandidate
            break

    if pszCompanyColumn is None:
        write_error_tsv(
            pszOutputFileFullPath,
            "Error: company name column not found. "
            "Expected one of {0}.".format(", ".join(objCandidateColumns)),
        )
        return

    try:
        objDataFrame[pszCompanyColumn] = (
            objDataFrame[pszCompanyColumn]
            .fillna("")
            .astype(str)
            .apply(step0003_normalize_company_name)
        )
    except Exception as objException:
        write_error_tsv(
            pszOutputFileFullPath,
            "Error: unexpected exception while normalizing company name column. "
            "Detail = {0}".format(objException),
        )
        return

    try:
        objDataFrame.to_csv(
            pszOutputFileFullPath,
            sep="\t",
            index=False,
            encoding="utf-8",
            lineterminator="\n",
        )
    except Exception as objException:
        write_error_tsv(
            pszOutputFileFullPath,
            "Error: unexpected exception while writing step0003 normalized TSV. "
            "Detail = {0}".format(objException),
        )
        return


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

    make_removed_uninput_tsv_from_manhour_tsv(pszStep1TsvPath)
    pszStep0001TsvPath: str = build_removed_uninput_output_path(pszStep1TsvPath)
    make_sorted_staff_code_tsv_from_manhour_tsv(pszStep0001TsvPath)
    pszStep0002TsvPath: str = build_sorted_staff_code_output_path(pszStep0001TsvPath)
    make_company_normalized_tsv_from_step0002(pszStep0002TsvPath)

    return 0


def main() -> int:
    objParser: argparse.ArgumentParser = argparse.ArgumentParser()
    objParser.add_argument(
        "pszInputManhourCsvPaths",
        nargs="+",
        help="Input Jobcan manhour CSV file paths",
    )
    objArgs: argparse.Namespace = objParser.parse_args()

    iExitCode: int = 0
    for pszInputManhourCsvPath in objArgs.pszInputManhourCsvPaths:
        try:
            iResult: int = process_single_input(pszInputManhourCsvPath)
        except Exception as objException:
            print(
                "Error: failed to process input file: {0}. Detail = {1}".format(
                    pszInputManhourCsvPath,
                    objException,
                )
            )
            iExitCode = 1
            continue
        if iResult != 0:
            iExitCode = 1

    return iExitCode


if __name__ == "__main__":
    raise SystemExit(main())
