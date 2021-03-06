#!/usr/bin/env python3
"""
cdc.py - Parse CDC CLIA Laboratory Data

https://www.cdc.gov/clia/LabSearch.html
"""

import argparse
import sys
import time
from argparse import RawDescriptionHelpFormatter

import pandas as pd


class AbortScriptException(Exception):
    """Abort the script and clean up before exiting."""


def parse_args():
    """Parse sys.argv and return args."""
    parser = argparse.ArgumentParser(
        formatter_class=RawDescriptionHelpFormatter,
        description="This tool compares Master lab data with new data CSV files",
        epilog="E.g.: ./cdc.py Master/Master.csv TestCaptures/data1.csv "
        "TestCaptures/data2.csv TestCaptures/data3.csv TestCaptures/data4.csv",
    )
    parser.add_argument(
        "master", help="A Master CLIA CDC CSV file to process",
    )
    parser.add_argument(
        "new_files",
        type=argparse.FileType("r"),
        nargs="+",
        help="A number of new CLIA CSV files to compare with Master",
    )
    parser.add_argument(
        "-e", "--extra", action="store_true", help="Display some extra data",
    )
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="Bypass safety rails - very dangerous",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="turn on verbose messages, commands and outputs",
    )

    return parser.parse_args()


def print_banner(description):
    """
    Display a bannerized print.

    E.g.     banner("Kubernetes Join")
    """
    banner = len(description)
    if banner > 200:
        banner = 200

    # First banner
    print("\n")
    for _ in range(banner):
        print("*", end="")

    # Add description
    print("\n%s" % description)

    # Final banner
    for _ in range(banner):
        print("*", end="")
    print("\n")


class Parsedata:
    """
    Common base class for parsing a Vertica Database File.

    Create a connection with a remote Flow Connector
    Execute VSQL queries
    SCP output and save in a global dataframe
    """

    pd.options.display.max_rows = 50
    pd.options.display.max_columns = None
    pd.options.display.width = None

    num_ran = 0

    def __init__(self, args):
        """Initialize all variables, basic time checking."""
        self.verbose = args.verbose
        self.force = args.force
        self.extra = args.extra
        self.master_csv = args.master
        self.new_files = args.new_files
        self.df_mas_lab_data = None  # Master Lab data
        self.df_new_lab_data = None  # Aggregated new Lab data
        self.columns = [
            "CLIA",
            "FACILITY_TYPE",
            "CERTIFICATE_TYPE",
            "LAB_NAME",
            "STREET",
            "CITY",
            "STATE",
            "ZIP",
            "PHONE",
        ]

    def df_diff(self, df1, df2, which=None):
        """Find rows which are different.

        which=left_only  - Which rows were only present in the first DataFrame?
        which=right_only - Which rows were only present in the second DataFrame?
        which=both       - Which rows were present in both DataFrames?
        which=None       - Which rows were not present in both DataFrames, but present in one of them?
        """
        comparison_df = df1.merge(df2, indicator=True, on=self.columns, how="outer")
        if which is None:
            diff_df = comparison_df[comparison_df["_merge"] != "both"].reset_index(
                drop=True
            )
        else:
            diff_df = comparison_df[comparison_df["_merge"] == which].reset_index(
                drop=True
            )

        return diff_df

    def get_files(self):
        """Use Python Pandas to create a dataset.

        Create two dataframes for the master and new data.
        Makes everything strings so merges and compares work.
        Set index to CLIA for merge later.
        Delete rows that are uneeded in the Master, that won't be in the new data.
        """

        # Grab master data - use existing header, remove unhappy columns

        self.df_mas_lab_data = pd.read_csv(
            self.master_csv, dtype=str, usecols=self.columns
        )

        # Delete rows, where column FACILITY_TYPE != Independent, Hospital,
        # Physician Office
        facility_type_keep_list = ["Independent", "Hospital", "Physician Office"]
        self.df_mas_lab_data = self.df_mas_lab_data[
            self.df_mas_lab_data["FACILITY_TYPE"].isin(facility_type_keep_list)
        ]

        # Make everything a string and remove trailing and leading whitespaces
        self.df_mas_lab_data = self.df_mas_lab_data.astype(str)
        self.df_mas_lab_data = self.df_mas_lab_data.applymap(
            lambda x: x.strip() if isinstance(x, str) else x
        )

        print_banner("Computing all the Data")
        print(f"{len(self.df_mas_lab_data)} original master CLIA labs...")

        # Grab other inputed files to make new data file to compare with
        self.df_new_lab_data = pd.concat(
            [
                pd.read_csv(file, names=self.columns, header=None, dtype=str, usecols=self.columns)
                for file in self.new_files
            ]
        )

        # Probably not needed for the new data but just in case:
        # Delete rows, where column FACILITY_TYPE != Independent, Hospital,
        # Physician Office
        self.df_new_lab_data = self.df_new_lab_data[
            self.df_new_lab_data["FACILITY_TYPE"].isin(facility_type_keep_list)
        ]

        # Make everything a string and remove trailing and leading whitespaces
        self.df_new_lab_data = self.df_new_lab_data.astype(str)
        self.df_new_lab_data = self.df_new_lab_data.applymap(
            lambda x: x.strip() if isinstance(x, str) else x
        )

        print(f"{len(self.df_new_lab_data)} inputted CLIA labs for comparison...")

    def process_data(self):
        """Process the data.

        Display data in various forms between the two data sets.

        Save the diff file and the new master.
        """

        if self.extra:
            print_banner("Old Master data")
            print(self.df_mas_lab_data)

            print_banner("New data combined from new data file(s)")
            print(self.df_new_lab_data)

        #
        # NEW CLIAS - Labs in new data not present in old Master
        #
        new_clias_df = self.df_diff(
            self.df_mas_lab_data, self.df_new_lab_data, which="right_only",
        )
        new_clias_df = new_clias_df.drop("_merge", 1)
        print(f"{len(new_clias_df)} are new CLIA Labs...")

        #
        # CLOSED CLIAS - Labs present in only in the master and not the new data
        #
        closed_clias_df = self.df_diff(
            self.df_mas_lab_data, self.df_new_lab_data, which="left_only",
        )
        closed_clias_df = closed_clias_df.drop("_merge", 1).reset_index(drop=True)
        print(f"{len(closed_clias_df)} are closed CLIA Labs...")

        #
        # UNCHANGED CLIAS - Labs were present in old Master and new data"
        #
        unchanged_clias_df = self.df_diff(
            self.df_mas_lab_data, self.df_new_lab_data, which="both"
        )
        unchanged_clias_df = unchanged_clias_df.drop("_merge", 1)
        print(f"{len(unchanged_clias_df)} are unchanged CLIA Labs...")

        #
        # New master = (unchanged + new)
        #

        # new_master + new data
        new_master_df = pd.concat(
            [unchanged_clias_df, new_clias_df], ignore_index=True
        ).reset_index(drop=True)

        # Save some info
        print_banner("Results saved to CSV files")

        timestr = time.strftime("%Y-%m-%d-%H:%M:%S")

        new_clia_data = f"Output/new_clia_data_{timestr}.csv"
        print(f"\nSaved new CLIA data to:        '{new_clia_data}'")
        new_clias_df.to_csv(new_clia_data)

        closed_clia_data = f"Output/closed_clia_data_{timestr}.csv"
        print(f"Saved closed CLIA data to:     '{closed_clia_data}'")
        closed_clias_df.to_csv(closed_clia_data)

        new_master = f"Output/new_master_clia_data_{timestr}.csv"
        print(f"Saved new master CLIA data to: '{new_master}'")
        new_master_df.to_csv(new_master)

        if self.extra:
            # Add some fun filtering
            print_banner("Labs in Alabama only")
            print(new_master_df[new_master_df["STATE"].str.match("AL")])

            print_banner("Labs with License 'Compliance'")
            print(
                new_master_df[new_master_df["CERTIFICATE_TYPE"].str.match("Compliance")]
            )

            print_banner("Labs in City 'Anchorage'")
            print(new_master_df[new_master_df["CITY"].str.match("Anchorage")])


def main():
    """Call everything."""
    args = parse_args()

    try:
        cdc_inst = Parsedata(args)

        cdc_inst.get_files()
        cdc_inst.process_data()

    except Exception:
        print("Exception caught:")
        print(sys.exc_info())
        raise


if __name__ == "__main__":
    main()
