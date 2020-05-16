#!/usr/local/bin/python3
"""
arpg.py - Parse a Stealthwatch (SW) Flow Collector (FC) Host Baseline record.

Automated Role Policy Generator (ARPG)

select v6_ntoa(ip_address), group_list, policy_group_list, archive_time,
day_of_week, ci, ti, fsi, total_traffic, data_loss, hi_traffic, lo_traffic,
new_flows, max_flows, new_dest_flows, max_dest_flows, syns_received, net_syns,
net_udps, net_icmp, exi, suspect_data_hoard, target_data_hoard, cnci, pvi,
udp_received, icmp_received, atk, anm from host_baseline where (group_list LIKE
'%,'||'1'||',%' ) and archive_time >= '2020-01-21';

The tool will aggregate all flows on a FC by default.

E.g.: ./arpg.py 10 -ip 10.208.108.101 -u root -p lan1cope

See *.rst files for more explanations and VSQL examples.

See Tests.rst for real examples.

Make sure passwordless ssh works on the remote FC.

This has been tested on python3.7.5, highly recomend running from a virtual
environment: virtualenv environment_name -p python3.7 source
environment_name/bin/activate pip install pandas... etc

Some of the filters are processed on the box (time range and flow id), some are
processed off-box by pandas. This is to achieve maximum efficiency.
"""

# # Rename aggregated flow_id to flow_id_hits
# self.pandas_df.rename(columns={'flow_id': 'hits'}, inplace=True)

# # Gather total bytes (client + server)
# self.pandas_df['Total'] = (self.pandas_df['client_bytes'] + self.pandas_df['server_bytes'])

# # Print total bytes in G (divide by 1e+9)
# self.pandas_df['Total (GB)'] = self.pandas_df['Total']
# self.pandas_df['Total (GB)'] /= (1024 * 1024 * 1024)

# # Add in bytes in M too
# self.pandas_df['Total (MB)'] = self.pandas_df['Total']
# self.pandas_df['Total (MB)'] /= (1024 * 1024)

# # Brief table headings
# self.pandas_df.rename(columns={
#     'server_ip_address': 'srv_ip',
#     'server_bytes': 'srv_bytes',
#     'client_packets': 'client_pkts',
#     'server_packets': 'svr_pkts'}, inplace=True)

# # Reorder columns for consistency
# self.pandas_df = self.pandas_df[['hits',
#                                  'client_bytes',
#                                  'client_pkts',
#                                  'srv_bytes',
#                                  'svr_pkts',
#                                  'Total',
#                                  'Total (MB)',
#                                  'Total (GB)']]

import argparse
import datetime
import os
import re
import socket
import subprocess
import sys
from argparse import RawDescriptionHelpFormatter
from os import path
from pathlib import Path

import dateutil.relativedelta
import pandas as pd
import yaml
from paramiko import SSHClient
from scp import SCPClient


class AbortScriptException(Exception):
    """Abort the script and clean up before exiting."""


def parse_args():
    """Parse sys.argv and return args."""
    parser = argparse.ArgumentParser(
        formatter_class=RawDescriptionHelpFormatter,
        description="This tool takes an input file, which is populated with\n"
        "a Vertica FC database query.\n\n"
        "Flow Collector credentials can be added by cli or\n"
        "populated by a ~/.arpg.yaml, in the format:\n\n"
        "ip: 10.208.108.101\nusername: root\npassword <mypassword>",
        epilog="E.g.: ./arpg.py -ip 10.90.67.28 --host_group 68",
    )
    parser.add_argument(
        "-ip",
        "--flow_collector_ip",
        type=str,
        default="None",
        help="IP Address of the Flow Collector to collect Bi-Flow from",
    )
    parser.add_argument(
        "-u",
        "--flow_collector_username",
        type=str,
        default="None",
        help="Username of the Flow Collector to collect Bi-Flow from",
    )
    parser.add_argument(
        "-p",
        "--flow_collector_password",
        type=str,
        default="None",
        help="Password of the Flow Collector to collect Bi-Flow from",
    )
    parser.add_argument(
        "-hg",
        "--host_group",
        type=str,
        default="%",
        help="Optionally add a Host Group ID to narrow down results "
        "This filter is processed on the Flow Collector",
    )
    parser.add_argument(
        "-at",
        "--archive_time",
        type=int,
        default=1,
        help="Period of time in months to pull from, default 1",
    )
    parser.add_argument(
        "-d", "--dry_run", action="store_true", help="Generate the VSQL commands only"
    )
    parser.add_argument(
        "-c",
        "--cache",
        action="store_true",
        help="Process cached data if found without getting new",
    )
    parser.add_argument(
        "-csv",
        "--csv",
        type=str,
        default="None",
        help="Process a passed in file of CSV data",
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
    parser.add_argument(
        "-r",
        "--raw",
        action="store_true",
        help="echo raw table data - super verbose",
    )

    return parser.parse_args()


def run_shell(cli, quiet=False):
    """
    Run a shell command and return the output.

    Print the output and errors if debug is enabled
    Not using logger.debug as a bit noisy for this info
    """
    if not quiet:
        print("...%s" % str(cli))

    process = subprocess.Popen(
        cli, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True
    )
    out, err = process.communicate()

    out = out.rstrip()
    err = err.rstrip()

    if str(out) != "0" and str(out) != "1" and out:
        print("  Shell STDOUT output:")
        print()
        print(out)
        print()
    if err:
        print("  Shell STDERR output:")
        print()
        print(err)
        print()

    return out


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


def get_human_readable(size, precision=2):
    """Get a readable byte count."""
    suffixes = ["B", "KB", "MB", "GB", "TB"]
    suffix_index = 0
    while size > 1024:
        suffix_index += 1  # increment the index of the suffix
        size = size / 1024.0  # apply the division
    return "%.*f %d" % (precision, size, suffixes[suffix_index])


def get_protocol_string(prot_id):
    """Convert a protocol id into its string."""
    # Create a lookup table by iterating over the values in the module
    table = {
        num: name[8:]
        for name, num in vars(socket).items()
        if name.startswith("IPPROTO")
    }
    pid = prot_id["protocol"]

    return table[pid]


def progress4(filename, size, sent, peername):
    """Define progress callback that prints the current percentage completed for the file."""
    sys.stdout.write(
        "(%s:%s) %s's progress: %.2f%%   \r"
        % (peername[0], peername[1], filename, float(sent) / float(size) * 100)
    )


def days_hours_minutes(time_date):
    """Convert to days, hours and minutes format."""
    return time_date.days, time_date.seconds // 3600, (time_date.seconds // 60) % 60


class Parsedata:
    """
    Common base class for parsing a Vertica Database File.

    Create a connection with a remote Flow Connector
    Execute VSQL queries
    SCP output and save in a global dataframe
    """

    # pd.options.display.max_rows = None
    pd.options.display.max_columns = None
    pd.options.display.width = None

    num_ran = 0

    def __init__(self, args):
        """Initialize all variables, basic time checking."""
        self.verbose = args.verbose
        self.raw = args.raw
        self.archive = args.archive_time
        self.host_group = args.host_group
        self.flow_collector_ip = "None"
        self.username = "None"
        self.password = "None"
        self.dry_run = args.dry_run
        self.force = args.force
        self.global_pandas_df = ""
        self.gntp_pandas_df = ""
        self.gatp_pandas_df = ""
        self.tol_pandas_df = ""
        self.command = ""
        self.cache = args.cache
        self.csv = args.csv

        # Calculate time a month from now
        utc_time_in_the_past = datetime.datetime.utcnow() - dateutil.relativedelta.relativedelta(
            months=args.archive_time
        )
        self.past_time_pretty = str(utc_time_in_the_past).split(".")[0]

        # Check for a base configuration and load FC credentials if it exists
        # Allow for CLI overwrite of each specific value
        home = str(Path.home())
        yaml_config = "{}/.arpg.yaml".format(home)

        if os.path.exists(yaml_config):
            # Load base configuration
            with open(yaml_config) as file:
                user_config = yaml.safe_load(file)
            self.flow_collector_ip = user_config["ip"]
            self.username = user_config["username"]
            self.password = user_config["password"]

        # Override base config with user inputs
        if args.flow_collector_ip != "None":
            self.flow_collector_ip = args.flow_collector_ip

        if args.flow_collector_username != "None":
            self.username = args.flow_collector_username

        if args.flow_collector_password != "None":
            self.password = args.flow_collector_password

        # If neither base config nor CLI has credentials then exit
        if (self.flow_collector_ip == "None" or self.username == "None" or self.password == "None"):
            print_banner(
                "Error: must supply Flow Collector IP, Username and Password, using "
                "CLI or ~/.arpg.yaml"
            )
            sys.exit()

        Parsedata.num_ran += 1

    def create_vsql_query(self):
        """Create the VSQL query string."""
        self.command = """/opt/vertica/bin/vsql -U dbadmin -w lan1cope -c "select v6_ntoa(ip_address) as ip_address, \
            archive_time, day_of_week, ci, ti, fsi, total_traffic, \
            data_loss, hi_traffic, lo_traffic, new_flows, max_flows, new_dest_flows, max_dest_flows, \
            syns_received, net_syns, net_udps, net_icmp, exi, suspect_data_hoard, target_data_hoard, \
            cnci, pvi, udp_received, icmp_received, atk, anm from host_baseline \
            where group_list LIKE '%,'||'{}'||',%' and archive_time >= '{}'" \
            -Aq -P footer=off -F ',' > /lancope/var/arpg
        """.format(
            self.host_group, self.past_time_pretty
        )

    def get_fc_data(self):
        """
        Connect to the Flow Connector and query the database.

        Compress the data and SCP the outcome locally to be processed
        """
        if self.cache:
            print_banner("Skip retrieving latest data, process cached data off-box")
            return

        # Process file instead of retrieving data, but cache always get overide
        if self.csv != "None":
            print_banner("Skip retrieving latest data, processing file {}".format(self.csv))
            return

        if self.dry_run:
            print_banner("Dry Run Begin")
            print(
                "...Will run this VSQL command on Flow "
                "Collector({}):\n".format(self.flow_collector_ip)
            )
            print(re.sub(" +", " ", self.command))
            return

        print(
            "...Local:  SSH connect to Flow Collector({})".format(
                self.flow_collector_ip
            )
        )
        ssh = SSHClient()
        ssh.load_system_host_keys()
        ssh.connect(
            self.flow_collector_ip,
            username=self.username,
            password=self.password,
            look_for_keys=False,
            allow_agent=False,
        )

        print(
            "...Remote: Run VSQL Query command on Flow "
            "Collector({}):".format(self.flow_collector_ip)
        )
        print(re.sub(" +", " ", self.command))

        # Run remote VSQL command
        _, stdout, _ = ssh.exec_command(self.command, timeout=None)
        # Wait for the command to terminate
        exit_status = stdout.channel.recv_exit_status()  # Blocking call
        if exit_status != 0:
            print("Error", exit_status)

        # Compress output file for scp efficiency
        print("...Remote: Compress data")
        _, stdout, _ = ssh.exec_command(
            "bzip2 -s -v -f /lancope/var/arpg", timeout=None
        )
        # Wait for the command to terminate
        exit_status = stdout.channel.recv_exit_status()  # Blocking call
        if exit_status != 0:
            print("Error", exit_status)

        # SCP file back home
        print("...Remote: SCP output /lancope/var/arpg.bz2 to /tmp/arpg.bz2")
        scp = SCPClient(ssh.get_transport(), progress4=progress4)
        scp.get("/lancope/var/arpg.bz2", "/tmp/arpg.bz2")

        # Clean up temp file
        print('...Remote: Clean up data')
        _, stdout, _ = ssh.exec_command('rm /lancope/var/arpg.bz2', timeout=None)
        exit_status = stdout.channel.recv_exit_status()  # Blocking call
        if exit_status != 0:
            print('Error', exit_status)

        scp.close()

    def process_file(self):
        """Use Python Pandas to create a dataset."""
        # Dry run doesn't process data
        if self.dry_run:
            print_banner("Dry run end")
            sys.exit()

        if self.cache:
            # Trying to read data from previous retrieval
            if path.exists("/tmp/arpg"):
                print("...Local:  Found and using cached data")
                self.global_pandas_df = pd.read_csv("/tmp/arpg")
            else:
                print("...Local:  Error - no previous data to use found")
                sys.exit()
        elif self.csv != "None":
            # Using data file passed in
            print("...Local:  Found and processing file '{}'".format(self.csv))
            self.global_pandas_df = pd.read_csv(self.csv)
        elif path.exists("/tmp/arpg.bz2"):
            # Using retrieved data
            print("...Local:  Found and processing new data")
            run_shell("bzip2 -d -f /tmp/arpg.bz2", True)
            self.global_pandas_df = pd.read_csv("/tmp/arpg")

    def display_raw_data(self):
        """
        Display the data with index and lists, raw debug only.

        Not used but interesting reference for a Pandas dataset
        """
        if self.raw:
            print_banner("Raw Pandas DF in series and rows")

            if self.global_pandas_df.empty:
                print("DataFrame is empty!")
            else:
                for (index_label, row_series) in self.global_pandas_df.iterrows():
                    print("Row Index label : ", index_label)
                    print("Row Content as Series : ", row_series.values)
            print("\n")

    def display_all_data(self):
        """Display the all the data in table form', raw only."""
        if self.raw:
            print_banner("All Host Baseline Data")

            if self.global_pandas_df.empty:
                print("No Host Baseline Data was found for these parameters!")
            else:
                print(self.global_pandas_df)
            print("\n")

    def get_never_trigger_point(self):
        """
        Display mean data grouped by IP ADDRESS. Remove Sundays and Saturdays.

        Remove rows 0 and 6 as sunday and saturday

        Means of each row, mean of each column (for all ips)

        mean (8+7) = ci mean (never trigger point)
        Output is: self.gntp_pandas_df
        """
        print("...Local:  Processing data ...")
        if self.host_group == "%":
            host_group_id = "All"

        print_banner("Host Group id={}".format(host_group_id))

        # Make a deep copy of the global pandas dataframe
        self.gntp_pandas_df = self.global_pandas_df.copy()

        # Remove rows where the day_of_week is Saturday(6) or Sunday(0)
        self.gntp_pandas_df = self.gntp_pandas_df[self.gntp_pandas_df.day_of_week != 0]
        self.gntp_pandas_df = self.gntp_pandas_df[self.gntp_pandas_df.day_of_week != 6]

        # Drop day_of_week column because we don't want the mean of that
        self.gntp_pandas_df = self.gntp_pandas_df.drop('day_of_week', axis=1)

        # Group ip_addresses and calculate the mean of all rows per IP
        self.gntp_pandas_df = self.gntp_pandas_df.groupby(['ip_address']).mean()
        if self.verbose:
            print_banner('Mean of all rows per IP')
            print(self.gntp_pandas_df)

        # Now get mean per column (or Never Trigger Point)
        self.gntp_pandas_df = self.gntp_pandas_df.mean(axis=0)
        print_banner('Never Trigger Point per data per IP')

        if not self.gntp_pandas_df.empty:
            print(self.gntp_pandas_df)
        else:
            print("No Host Baseline Data was found for these parameters!")
        print("\n")

    def get_always_trigger_point(self):
        """
        Display maximum of each data value grouped by IP ADDRESS.

        Remove Sundays and Saturdays.

        Remove rows 0 and 6 as sunday and saturday

        Output is: self.gatp_pandas_df
        """
        print("...Local:  Processing data ...")
        if self.host_group == "%":
            host_group_id = "All"

        print_banner("Host Group id={}".format(host_group_id))

        # Make a deep copy of the global pandas dataframe
        self.gatp_pandas_df = self.global_pandas_df.copy()

        # Remove rows where the day_of_week is Saturday(6) or Sunday(0)
        self.gatp_pandas_df = self.gatp_pandas_df[self.gatp_pandas_df.day_of_week != 0]
        self.gatp_pandas_df = self.gatp_pandas_df[self.gatp_pandas_df.day_of_week != 6]

        # Drop day_of_week column because we don't want the mean of that
        self.gatp_pandas_df = self.gatp_pandas_df.drop('day_of_week', axis=1)

        # Group ip_addresses and calculate the sum of all rows per IP
        self.gatp_pandas_df = self.gatp_pandas_df.groupby(['ip_address']).sum()
        if self.verbose:
            print_banner('Sum of all rows per IP')
            print(self.gatp_pandas_df)

        # Now get sum per column (or Never Trigger Point)
        self.gatp_pandas_df = self.gatp_pandas_df.sum(axis=0)
        print_banner('Always Trigger Point per data per IP')

        if not self.gatp_pandas_df.empty:
            print(self.gatp_pandas_df)
        else:
            print("No Host Baseline Data was found for these parameters!")
        print("\n")

    def get_tolerance(self):
        """
        Get the Tolerance.

        1. Raw table, take highest value of every individual columun per ip
        2. if highest is monday - two other mondays of data must exist (same day of week)
        3. for highest day of week, say Monday, for that week is there at least 3 other
        (values in a week) entries that week.
            a. Store all 3 values
        4. find average/standard dev of all mondays except the highest for that ip
        5. find average/standard dev of all values except hghest for that day of week
        6. a/bTake highest val and minus everage / standard dev for both day of week and
            highes val
        7. 3. x.3 for day of week(mon) (3a)
        8. 3 x .7 all days of week 3b
        9. Add 4 and 5 together (this is all ips)
        10. Average of 6 for all ips.
        11. Take 7 and compare to chart Jesse sent, and that becomes the T value

        Output is: self.tol_pandas_df
        """
        print("...Local:  Processing data ...")
        if self.host_group == "%":
            host_group_id = "All"

        print_banner("Host Group id={}".format(host_group_id))

        # Make a deep copy of the global pandas dataframe
        self.tol_pandas_df = self.global_pandas_df.copy()

        # Remove rows where the day_of_week is Saturday(6) or Sunday(0)
        # ToDo: Needed?
        self.tol_pandas_df = self.tol_pandas_df[self.tol_pandas_df.day_of_week != 0]
        self.tol_pandas_df = self.tol_pandas_df[self.tol_pandas_df.day_of_week != 6]

        # Drop day_of_week column because we don't want that
        self.tol_pandas_df = self.tol_pandas_df.drop('day_of_week', axis=1)

        # Group by ip_addresses
        self.tol_pandas_df = self.tol_pandas_df.groupby('ip_address')
        if self.verbose:
            print_banner('Group by IP')
            print(self.tol_pandas_df)

        # Get max value of each individual column per IP
        # Get a series containing maximum value of each column
        max_tol_pandas_df = self.tol_pandas_df.max(skipna=True)
        # max_tol_pandas_df = self.tol_pandas_df.max()
        print('Maximum value in each column not skipping NaN:')
        print(max_tol_pandas_df)

        # if not self.tol_pandas_df.empty:
        #     print(self.tol_pandas_df)
        # else:
        #     print("No Host Baseline Data was found for these parameters!")
        print("\n")


def main():
    """Call everything."""
    args = parse_args()

    try:
        arpg_inst = Parsedata(args)

        arpg_inst.create_vsql_query()
        arpg_inst.get_fc_data()
        arpg_inst.process_file()
        arpg_inst.display_all_data()
        arpg_inst.display_raw_data()
        # Process Data
        arpg_inst.get_never_trigger_point()
        arpg_inst.get_always_trigger_point()
        arpg_inst.get_tolerance()

    except Exception:
        print("Exception caught:")
        print(sys.exc_info())
        raise


if __name__ == "__main__":
    main()
