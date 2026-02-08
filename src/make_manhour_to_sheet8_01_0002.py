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
    pszOutputBaseName: str = pszRootName + "_step0003_normalized_company_name.tsv"
    if len(pszDirectory) == 0:
        return pszOutputBaseName
    return os.path.join(pszDirectory, pszOutputBaseName)


def build_step0004_company_normalized_output_path(pszInputFileFullPath: str) -> str:
    pszDirectory: str = os.path.dirname(pszInputFileFullPath)
    pszBaseName: str = os.path.basename(pszInputFileFullPath)
    pszRootName: str
    pszExt: str
    pszRootName, pszExt = os.path.splitext(pszBaseName)

    pszStep0003Suffix: str = "_step0003_normalized_company_name"
    if pszRootName.endswith(pszStep0003Suffix):
        pszRootName = pszRootName[: -len(pszStep0003Suffix)]
    pszOutputBaseName: str = pszRootName + "_step0004_normalized_project_name.tsv"
    if len(pszDirectory) == 0:
        return pszOutputBaseName
    return os.path.join(pszDirectory, pszOutputBaseName)


def write_company_normalized_tsv(pszInputFileFullPath: str, pszOutputFileFullPath: str) -> None:
    if not os.path.isfile(pszInputFileFullPath):
        raise FileNotFoundError(f"Input TSV not found: {pszInputFileFullPath}")

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
            "Error: unexpected exception while reading TSV for company name normalization. "
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
            "Error: unexpected exception while writing normalized TSV. "
            "Detail = {0}".format(objException),
        )
        return


def make_company_normalized_tsv_from_step0002(pszInputFileFullPath: str) -> None:
    pszOutputFileFullPath: str = build_step0003_company_normalized_output_path(
        pszInputFileFullPath
    )
    write_company_normalized_tsv(pszInputFileFullPath, pszOutputFileFullPath)


def make_company_normalized_tsv_from_step0003(pszInputFileFullPath: str) -> None:
    pszOutputFileFullPath: str = build_step0004_company_normalized_output_path(
        pszInputFileFullPath
    )
    write_project_normalized_tsv(pszInputFileFullPath, pszOutputFileFullPath)


def step0004_normalize_project_code(pszProjectCode: str) -> str:
    return re.sub(r"[\s\u3000]+", "", pszProjectCode or "")


def step0004_normalize_project_name(pszProjectName: str) -> str:
    pszNormalized: str = (pszProjectName or "").replace(" ", "_").replace("　", "_")
    objMatchP: re.Match[str] | None = re.match(r"^(P\d{5})(.*)$", pszNormalized)
    if objMatchP is not None:
        pszCode: str = objMatchP.group(1)
        pszRest: str = objMatchP.group(2)
        if pszRest.startswith("【"):
            pszNormalized = pszCode + "_" + pszRest
    else:
        objMatchOther: re.Match[str] | None = re.match(r"^([A-OQ-Z]\d{3})(.*)$", pszNormalized)
        if objMatchOther is not None:
            pszCodeOther: str = objMatchOther.group(1)
            pszRestOther: str = objMatchOther.group(2)
            if pszRestOther.startswith("【"):
                pszNormalized = pszCodeOther + "_" + pszRestOther
    return pszNormalized


def write_project_normalized_tsv(pszInputFileFullPath: str, pszOutputFileFullPath: str) -> None:
    if not os.path.isfile(pszInputFileFullPath):
        raise FileNotFoundError(f"Input TSV not found: {pszInputFileFullPath}")

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
            "Error: unexpected exception while reading TSV for project normalization. "
            "Detail = {0}".format(objException),
        )
        return

    iColumnCount: int = objDataFrame.shape[1]
    if iColumnCount < 8:
        write_error_tsv(
            pszOutputFileFullPath,
            "Error: required columns G-H do not exist (need at least 8 columns). "
            "ColumnCount = {0}".format(iColumnCount),
        )
        return

    objColumnNameList: List[str] = list(objDataFrame.columns)
    pszColumnG: str = objColumnNameList[6]
    pszColumnH: str = objColumnNameList[7]

    try:
        objDataFrame[pszColumnG] = (
            objDataFrame[pszColumnG]
            .fillna("")
            .astype(str)
            .apply(step0004_normalize_project_code)
        )
        objDataFrame[pszColumnH] = (
            objDataFrame[pszColumnH]
            .fillna("")
            .astype(str)
            .apply(step0004_normalize_project_name)
        )
    except Exception as objException:
        write_error_tsv(
            pszOutputFileFullPath,
            "Error: unexpected exception while normalizing project code/name columns. "
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
            "Error: unexpected exception while writing normalized project TSV. "
            "Detail = {0}".format(objException),
        )
        return


def normalize_org_table_project_code_step0004(pszProjectCode: str) -> str:
    pszNormalized: str = step0004_normalize_project_name(pszProjectCode or "")
    return re.sub(r"[ \u3000]+", "_", pszNormalized)


def build_step0005_remove_ah_output_path(
    objBaseDirectoryPath: Path,
    iYear: int,
    iMonth: int,
) -> Path:
    return (
        objBaseDirectoryPath
        / f"工数_{iYear}年{iMonth:02d}月_step0005_remove_A_or_H_project.tsv"
    )


def build_step0006_company_replaced_output_path(
    objBaseDirectoryPath: Path,
    iYear: int,
    iMonth: int,
) -> Path:
    return (
        objBaseDirectoryPath
        / f"工数_{iYear}年{iMonth:02d}月_step0006_projects_replaced_by_管轄PJ表.tsv"
    )


def build_step0006_missing_project_output_path(
    objBaseDirectoryPath: Path,
    iYear: int,
    iMonth: int,
) -> Path:
    return (
        objBaseDirectoryPath
        / f"工数_{iYear}年{iMonth:02d}月_step0006_projects_missing_in_管轄PJ表.tsv"
    )


def read_org_table_company_mappings(pszOrgTableTsvPath: str) -> List[Tuple[str, str]]:
    if not os.path.isfile(pszOrgTableTsvPath):
        raise FileNotFoundError(f"Org table TSV not found: {pszOrgTableTsvPath}")

    objMappings: List[Tuple[str, str]] = []
    try:
        objOrgDataFrame: DataFrame = pd.read_csv(
            pszOrgTableTsvPath,
            sep="\t",
            dtype=str,
            encoding="utf-8",
            keep_default_na=False,
            engine="python",
        )
    except Exception as objException:
        raise RuntimeError(
            "Error: unexpected exception while reading 管轄PJ表.tsv. Detail = {0}".format(
                objException
            )
        ) from objException

    if objOrgDataFrame.shape[1] < 3:
        raise ValueError("Error: 管轄PJ表.tsv must have at least 3 columns.")

    objColumnNames: List[str] = list(objOrgDataFrame.columns)
    pszProjectColumn: str = objColumnNames[1]
    pszCompanyColumn: str = objColumnNames[2]

    for _, objRow in objOrgDataFrame.iterrows():
        pszProjectCode: str = str(objRow[pszProjectColumn] or "")
        pszCompanyName: str = str(objRow[pszCompanyColumn] or "")
        objMappings.append((pszProjectCode, pszCompanyName))

    return objMappings


def make_step0005_remove_ah_project_tsv(
    pszInputFileFullPath: str,
    pszOutputFileFullPath: str,
) -> None:
    if not os.path.isfile(pszInputFileFullPath):
        write_error_tsv(
            pszOutputFileFullPath,
            "Error: input TSV file not found for A/H removal. "
            "Path = {0}".format(pszInputFileFullPath),
        )
        return

    try:
        objDataFrameInput: DataFrame = pd.read_csv(
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
            "Error: unexpected exception while reading TSV for A/H removal. "
            "Detail = {0}".format(objException),
        )
        return

    if objDataFrameInput.shape[1] < 7:
        write_error_tsv(
            pszOutputFileFullPath,
            "Error: required column G does not exist (need at least 7 columns). "
            "ColumnCount = {0}".format(objDataFrameInput.shape[1]),
        )
        return

    objColumnNames: List[str] = list(objDataFrameInput.columns)
    pszProjectColumn: str = objColumnNames[6]

    objProjectSeries = objDataFrameInput[pszProjectColumn].fillna("").astype(str)
    objKeepMask = ~objProjectSeries.str.startswith(("A", "H"))
    objDataFrameOutput: DataFrame = objDataFrameInput.loc[objKeepMask].copy()

    try:
        objDataFrameOutput.to_csv(
            pszOutputFileFullPath,
            sep="\t",
            index=False,
            encoding="utf-8",
            lineterminator="\n",
        )
    except Exception as objException:
        write_error_tsv(
            pszOutputFileFullPath,
            "Error: unexpected exception while writing A/H removed TSV. "
            "Detail = {0}".format(objException),
        )
        return


def make_step0006_company_replaced_tsv_from_step0005(
    pszInputFileFullPath: str,
    pszOrgTableTsvPath: str,
    pszOutputFileFullPath: str,
    pszMissingOutputFileFullPath: str,
) -> None:
    if not os.path.isfile(pszInputFileFullPath):
        write_error_tsv(
            pszOutputFileFullPath,
            "Error: input TSV file not found for company replacement. "
            "Path = {0}".format(pszInputFileFullPath),
        )
        return

    try:
        objDataFrameInput: DataFrame = pd.read_csv(
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
            "Error: unexpected exception while reading TSV for company replacement. "
            "Detail = {0}".format(objException),
        )
        return

    if objDataFrameInput.shape[1] < 7:
        write_error_tsv(
            pszOutputFileFullPath,
            "Error: required columns D/G do not exist (need at least 7 columns). "
            "ColumnCount = {0}".format(objDataFrameInput.shape[1]),
        )
        return

    try:
        objMappings: List[Tuple[str, str]] = read_org_table_company_mappings(
            pszOrgTableTsvPath
        )
    except Exception as objException:
        write_error_tsv(
            pszOutputFileFullPath,
            "Error: unexpected exception while reading org table TSV. "
            "Detail = {0}".format(objException),
        )
        return

    objColumnNames: List[str] = list(objDataFrameInput.columns)
    pszCompanyColumn: str = objColumnNames[3]
    pszProjectColumn: str = objColumnNames[6]

    objCompanyValues: List[str] = []
    objMissingMask: List[bool] = []

    for _, objRow in objDataFrameInput.iterrows():
        pszProjectCode: str = str(objRow[pszProjectColumn] or "")
        pszNewCompany: str | None = None
        for pszOrgProjectCode, pszOrgCompanyName in objMappings:
            if pszOrgProjectCode != "" and pszOrgProjectCode.startswith(pszProjectCode):
                pszNewCompany = pszOrgCompanyName
                break
        if pszNewCompany is None:
            objCompanyValues.append(str(objRow[pszCompanyColumn] or ""))
            objMissingMask.append(True)
        else:
            objCompanyValues.append(pszNewCompany)
            objMissingMask.append(False)

    objDataFrameOutput: DataFrame = objDataFrameInput.copy()
    objDataFrameOutput[pszCompanyColumn] = objCompanyValues
    objMatchedDataFrame: DataFrame = objDataFrameOutput[[not is_missing for is_missing in objMissingMask]]
    objMissingDataFrame: DataFrame = objDataFrameInput[objMissingMask]

    try:
        objMatchedDataFrame.to_csv(
            pszOutputFileFullPath,
            sep="\t",
            index=False,
            encoding="utf-8",
            lineterminator="\n",
        )
    except Exception as objException:
        write_error_tsv(
            pszOutputFileFullPath,
            "Error: unexpected exception while writing company replaced TSV. "
            "Detail = {0}".format(objException),
        )
        return

    try:
        objMissingDataFrame.to_csv(
            pszMissingOutputFileFullPath,
            sep="\t",
            index=False,
            encoding="utf-8",
            lineterminator="\n",
        )
    except Exception as objException:
        write_error_tsv(
            pszMissingOutputFileFullPath,
            "Error: unexpected exception while writing missing project list TSV. "
            "Detail = {0}".format(objException),
        )
        return
def write_org_table_tsv_from_csv(objBaseDirectoryPath: Path) -> None:
    objScriptDirectoryPath: Path = Path(__file__).resolve().parent
    objOrgTableCsvPath: Path = objScriptDirectoryPath / "管轄PJ表.csv"
    if not objOrgTableCsvPath.exists():
        objOrgTableCsvPath = objBaseDirectoryPath / "管轄PJ表.csv"

    objOrgTableTsvPath: Path = objBaseDirectoryPath / "管轄PJ表.tsv"

    if not objOrgTableCsvPath.exists():
        write_error_tsv(
            str(objOrgTableTsvPath),
            "Error: 管轄PJ表.csv が見つかりません。Path = {0}".format(objOrgTableCsvPath),
        )
        return

    objRows: List[List[str]] = []
    arrEncodings: List[str] = ["utf-8-sig", "cp932"]
    objLastDecodeError: Exception | None = None

    for pszEncoding in arrEncodings:
        try:
            with open(
                objOrgTableCsvPath,
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
        write_error_tsv(
            str(objOrgTableTsvPath),
            "Error: unexpected exception while reading 管轄PJ表.csv. Detail = {0}".format(
                objLastDecodeError
            ),
        )
        return

    for iRowIndex, objRow in enumerate(objRows):
        if len(objRow) > 1:
            objRow[1] = normalize_org_table_project_code_step0004(objRow[1])
        objRows[iRowIndex] = objRow

    objOrgTableTsvPath.parent.mkdir(parents=True, exist_ok=True)
    with open(objOrgTableTsvPath, mode="w", encoding="utf-8", newline="") as objOutputFile:
        objWriter: csv.writer = csv.writer(objOutputFile, delimiter="\t")
        for objRow in objRows:
            objWriter.writerow(objRow)


def process_single_input(
    pszInputManhourCsvPath: str,
) -> tuple[int, Path | None, int | None, int | None, str | None]:
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
    pszStep0003TsvPath: str = build_step0003_company_normalized_output_path(
        pszStep0002TsvPath
    )
    make_company_normalized_tsv_from_step0003(pszStep0003TsvPath)
    pszStep0004TsvPath: str = build_step0004_company_normalized_output_path(
        pszStep0003TsvPath
    )

    return 0, objBaseDirectoryPath, iFileYear, iFileMonth, pszStep0004TsvPath


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
            iResult, objBaseDirectoryPath, iYear, iMonth, pszStep0004TsvPath = (
                process_single_input(pszInputManhourCsvPath)
            )
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
        elif (
            objBaseDirectoryPath is not None
            and iYear is not None
            and iMonth is not None
            and pszStep0004TsvPath is not None
        ):
            write_org_table_tsv_from_csv(objBaseDirectoryPath)
            objOrgTableTsvPath: Path = objBaseDirectoryPath / "管轄PJ表.tsv"
            objStep0005Path: Path = build_step0005_remove_ah_output_path(
                objBaseDirectoryPath,
                iYear,
                iMonth,
            )
            make_step0005_remove_ah_project_tsv(
                pszStep0004TsvPath,
                str(objStep0005Path),
            )
            objStep0006Path: Path = build_step0006_company_replaced_output_path(
                objBaseDirectoryPath,
                iYear,
                iMonth,
            )
            objStep0006MissingPath: Path = build_step0006_missing_project_output_path(
                objBaseDirectoryPath,
                iYear,
                iMonth,
            )
            make_step0006_company_replaced_tsv_from_step0005(
                str(objStep0005Path),
                str(objOrgTableTsvPath),
                str(objStep0006Path),
                str(objStep0006MissingPath),
            )

    return iExitCode


if __name__ == "__main__":
    raise SystemExit(main())
