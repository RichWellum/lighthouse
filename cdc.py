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
        description="This tool compares two CSV files",
        epilog="E.g.: ./cdc.py Master.csv latest.csv",
    )
    parser.add_argument(
        "master", help="A master CDC CSV file to process",
    )
    parser.add_argument(
        "new_data", help="New data to compare",
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
        self.tol_pandas_df = ""
        self.master_csv = args.master
        self.new_data_csv = args.new_data
        self.df_master = None
        self.df_new_data = None

    def get_files(self):
        """Use Python Pandas to create a dataset.

        Get the CSV file, add headerss
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

        # Create two dataframes for the master and new data
        # Makes everything strings so merges and compares work
        self.df_master = pd.read_csv(self.master_csv, names=col_names, header=None)
        self.df_master = self.df_master.astype(str)
        self.df_new_data = pd.read_csv(self.new_data_csv, names=col_names, header=None)
        self.df_new_data = self.df_new_data.astype(str)

    def process_data(self):
        """Process the data.

        Display data in various forms between the two data sets.

        Save the diff file and the new master.
        """

        print_banner("Old Master data")
        print(self.df_master)

        print_banner("New data")
        print(self.df_new_data)

        # This is the new diff - save this off to a CSV
        print_banner("(New not Master) These rows were present in only the new data")
        new_data_df = dataframe_difference(
            self.df_master, self.df_new_data, which="right_only"
        )
        print(new_data_df)

        print_banner("(Master not new) These rows were present in only in the master")
        print(dataframe_difference(self.df_master, self.df_new_data, which="left_only"))

        print_banner("(Duplicates) These rows were present in both sets of data")
        print(dataframe_difference(self.df_master, self.df_new_data, which="both"))

        # This is the sanitized new combined or merged data
        # Save as the new master
        new_master_df = dataframe_difference(self.df_master, self.df_new_data)
        new_master_df = new_master_df.drop("_merge", 1)
        print("\nSaved new master to 'new_master.csv'")
        new_master_df.to_csv("new_master.csv")
        print_banner("(Merged) These rows were present in only one set of data")
        print(new_master_df)

        print("\nSaved diff to 'Output/diff.csv'")
        new_data_df.to_csv("Output/diff.csv")

        print("Saved new master to 'Output/new_master.csv'")
        new_master_df.to_csv("Output/new_master.csv")

        # Add some fun filtering
        print_banner("Clients in Alabama only")
        print(new_master_df[new_master_df["State"].str.match("AL")])

        print_banner("Clients with License 'Compliance'")
        print(new_master_df[new_master_df["License"].str.match("Compliance")])

        print_banner("Clients in City 'Anchorage'")
        print(new_master_df[new_master_df["City"].str.match("Anchorage")])

        print_banner(f"Total number of results: {len(new_master_df)}")


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
