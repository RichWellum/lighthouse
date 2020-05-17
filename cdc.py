#!/usr/bin/env python3
"""
cdc.py - Parse CDC CLIA Labatory Search

https://www.cdc.gov/clia/LabSearch.html
"""

import argparse
import sys
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
        "master", help="A master CDC CSV file to process",
    )
    parser.add_argument(
        "new_files",
        type=argparse.FileType("r"),
        nargs="+",
        help="Number of new data files",
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


def dataframe_difference(df1, df2, which=None):
    """Find rows which are different.

    which=left_only  - Which rows were only present in the first DataFrame?
    which=right_only - Which rows were only present in the second DataFrame?
    which=both       - Which rows were present in both DataFrames?
    which=None .     - Which rows were not present in both DataFrames, but present in one of them?
    """
    comparison_df = df1.merge(df2, indicator=True, how="outer")
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
        self.master_csv = args.master
        self.df_master_lab_data = None
        self.df_new_lab_data = None
        self.new_files = args.new_files

    def get_files(self):
        """Use Python Pandas to create a dataset.

        Create two dataframes for the master and new data.
        Makes everything strings so merges and compares work.
        """
        col_names = [
            "ID",
            "Type",
            "License",
            "Name",
            "Address",
            "City",
            "State",
            "Something",
            "Phone",
        ]

        # Grab master data
        self.df_master_lab_data = pd.read_csv(
            self.master_csv, names=col_names, header=None
        )
        self.df_master_lab_data = self.df_master_lab_data.astype(str)

        # Grab other inputed files to make new data file to compare with
        self.df_new_lab_data = pd.concat(
            [pd.read_csv(file, names=col_names, header=None) for file in self.new_files]
        )
        self.df_new_lab_data = self.df_new_lab_data.astype(str)

    def process_data(self):
        """Process the data.

        Display data in various forms between the two data sets.

        Save the diff file and the new master.
        """

        print_banner("Old Master data")
        print(self.df_master_lab_data)

        print_banner("New data combined from new data file(s)")
        print(self.df_new_lab_data)

        # This is the new Labs - save this off to a CSV
        print_banner(
            "(New not Master) These labs were present in only the combined new data"
        )
        new_lab_data_df = dataframe_difference(
            self.df_master_lab_data, self.df_new_lab_data, which="right_only"
        )
        print(new_lab_data_df)

        print_banner("(Master not new) These labs were present in only in the master")
        print(
            dataframe_difference(
                self.df_master_lab_data, self.df_new_lab_data, which="left_only"
            )
        )

        print_banner("(Duplicates) These labs were present in both sets of data")
        print(
            dataframe_difference(
                self.df_master_lab_data, self.df_new_lab_data, which="both"
            )
        )

        # This is the sanitized new combined or merged data
        # Save as the new master
        new_master_df = dataframe_difference(
            self.df_master_lab_data, self.df_new_lab_data
        )
        new_master_df = new_master_df.drop("_merge", 1)
        print_banner(
            "(Merged) These labs were present in only one set of data - this is the new Master"
        )
        print(new_master_df)

        # Final step compare new and old Masters - what has been removed from
        # old Master?
        removed_from_old_master = dataframe_difference(self.df_master_lab_data, new_master_df, which="left_only")
        print_banner("Labs removed from the old Master list")
        print(removed_from_old_master)

        # Add some fun filtering
        print_banner("Labs in Alabama only")
        print(new_master_df[new_master_df["State"].str.match("AL")])

        print_banner("Labs with License 'Compliance'")
        print(new_master_df[new_master_df["License"].str.match("Compliance")])

        print_banner("Labs in City 'Anchorage'")
        print(new_master_df[new_master_df["City"].str.match("Anchorage")])

        # Save some info
        print_banner("Lab Data saved to CSV files")

        print("\nSaved new lab data to 'Output/new_lab_data.csv'")
        new_lab_data_df.to_csv("Output/new_lab_data.csv")

        print("Saved new master lab data to 'Output/new_master_lab_data.csv'")
        new_master_df.to_csv("Output/new_master_lab_data.csv")

        print("Saved Labs removed from old Master to 'Output/removed_from_old_master_lab_data.csv'")
        removed_from_old_master.to_csv("Output/removed_from_old_master_lab_data.csv")

        print_banner(
            f"Total number of results in new master lab data: {len(new_master_df)}"
        )


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
