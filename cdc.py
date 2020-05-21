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


def df_diff(df1, df2, comp_lst, which=None):
    """Find rows which are different.

    which=left_only  - Which rows were only present in the first DataFrame?
    which=right_only - Which rows were only present in the second DataFrame?
    which=both       - Which rows were present in both DataFrames?
    which=None       - Which rows were not present in both DataFrames, but present in one of them?
    """
    comparison_df = df1.merge(df2, indicator=True, on=comp_lst, how="outer",)
    if which is None:
        diff_df = comparison_df[comparison_df["_merge"] != "both"]
    else:
        diff_df = comparison_df[comparison_df["_merge"] == which]

    return diff_df


class Parsedata:
    """
    Common base class for parsing a Vertica Database File.

    Create a connection with a remote Flow Connector
    Execute VSQL queries
    SCP output and save in a global dataframe
    """

    pd.options.display.max_rows = 30
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
        self.df_master_lab_data = None
        self.df_new_lab_data = None
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
            "Contact",
            "Touch 1",
            "Touch 2",
            "Touch 3",
            "Touch 4",
            "Call Tag 1",
            "Call Tag 2",
        ]

    def get_files(self):
        """Use Python Pandas to create a dataset.

        Create two dataframes for the master and new data.
        Makes everything strings so merges and compares work.
        """

        # Grab master data - use existing header
        self.df_master_lab_data = pd.read_csv(
            self.master_csv, names=self.columns, header=0, dtype=str
        )
        self.df_master_lab_data = self.df_master_lab_data.astype(str)
        # Delete rows, where column FACILITY_TYPE != Independent, Hospital,
        # Physician Office
        facility_type_keep_list = ['Independent', 'Hospital', 'Physician Office']
        self.df_master_lab_data = self.df_master_lab_data[self.df_master_lab_data['FACILITY_TYPE'].isin(facility_type_keep_list)]
        self.df_master_lab_data = self.df_master_lab_data.reset_index(drop=True)
        print()
        print(f"Old master CLIA ({len(self.df_master_lab_data)}) data...")

        # Grab other inputed files to make new data file to compare with
        self.df_new_lab_data = pd.concat(
            [
                pd.read_csv(file, names=self.columns, header=None)
                for file in self.new_files
            ]
        )
        self.df_new_lab_data = self.df_new_lab_data.astype(str)
        print(f"Combine and save new data files ({len(self.df_new_lab_data)})...")

    def process_data(self):
        """Process the data.

        Display data in various forms between the two data sets.

        Save the diff file and the new master.
        """

        print("\nNumber of rows displayed restricted to '20'\n")
        if self.extra:
            print_banner("Old Master data")
            print(self.df_master_lab_data)

            print_banner("New data combined from new data file(s)")
            print(self.df_new_lab_data)

        #
        # NEW CLIAS
        #
        new_clias_df = df_diff(
            self.df_master_lab_data,
            self.df_new_lab_data,
            self.columns,
            which="right_only",
        )
        new_clias_df = new_clias_df.drop("_merge", 1).reset_index(drop=True)
        print_banner(
            f"New ({len(new_clias_df)}) CLIA (Labs in new data not present in old Master)"
        )
        print(new_clias_df)

        #
        # CLOSED CLIAS
        #
        closed_clias_df = df_diff(
            self.df_master_lab_data,
            self.df_new_lab_data,
            self.columns,
            which="left_only",
        )
        closed_clias_df = closed_clias_df.drop("_merge", 1).reset_index(drop=True)
        print_banner(
            f"Closed ({len(closed_clias_df)}) CLIA (Labs present in only in the master and not the new data)"
        )
        print(closed_clias_df)

        #
        # UNCHANGED CLIAS - present in both
        #
        unchanged_clias_df = df_diff(
            self.df_master_lab_data, self.df_new_lab_data, self.columns, which="both"
        )
        unchanged_clias_df = unchanged_clias_df.drop("_merge", 1)
        print_banner(
            f"Unchanged ({len(unchanged_clias_df)}) CLIA (Labs were present in old Master and new data"
        )
        print(unchanged_clias_df)

        #
        # New master = (unchanged + new)
        #

        # new_master + new data
        new_master_df = pd.concat(
            [unchanged_clias_df, new_clias_df], ignore_index=True
        ).reset_index(drop=True)
        print_banner(f"CLIA Master ({len(new_master_df)}) (unchanged + new)")

        print(new_master_df)

        # Save some info
        print_banner("Results saved to CSV files")

        timestr = time.strftime("%Y-%m-%d-%H:%M:%S")

        new_clia_data = f"Output/new_clia_data_{timestr}.csv"
        print(f"\nSaved new CLIA data to:        '{new_clia_data}'")
        new_clias_df.to_csv(new_clia_data)

        closed_clia_data = f"Output/closed_clia_data_{timestr}.csv"
        print(f"Saved closed CLIA data to:     '{closed_clia_data}'")
        closed_clias_df.to_csv(closed_clia_data)

        unchanged_clia_data = f"Output/unchanged_clia_data_{timestr}.csv"
        print(f"Saved unchanged CLIA data to:  '{unchanged_clia_data}'")
        unchanged_clias_df.to_csv(unchanged_clia_data)

        new_master = f"Output/new_master_clia_data_{timestr}.csv"
        print(f"Saved new master CLIA data to: '{new_master}'")
        new_master_df.to_csv(new_master)

        old_master = f"Output/old_master_clia_data_adj_{timestr}.csv"
        print(f"Saved old adjusted CLIA master data to:        '{old_master}'")
        self.df_master_lab_data.to_csv(old_master)

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
