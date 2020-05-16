#!/usr/local/bin/python3
''' parse_biflow.py - Parse a Stealthwatch (SW) Flow Collector (FC) Bi-Flow record.

The objective of this tool is to be able to pull Bi-Flow records from a single
FC bounded by an Epoch (time from 'now').

The statistics being targeted are the client and server ip packets and bytes
aggregated for a particular Flow ID (FI). The records can be then compared to
other data sources like the StealtWatch Management Console (SMC).

For example from the SMC, you might look at the following flows:

SMC-> Analyse Flows -> Inside Host -> Query with 80/tcp

By finding all same FIs using this tool then aggregating up the bytes, one can
reconstruct a flow record that you would see in the SMC flow report. Of course
the user might have multiple FCs which the SMC would then deduplicate.

The tool will aggregate all flows on a FC by default.

E.g.: ./parse_biflow.py 10 -ip 10.208.108.101 -u root -p lan1cope

Or it will take a flow_id to focus on one flow:

E.g. ./parse_biflow.py 10 -ip 10.208.108.101 -u root -p lan1cope -f 3064829173

Other options include a date range and a client and or server ip address.

Also the FC logon parameters, -ip, -u, -p can be hidden in a base configuration
file, ~/parse_biflow.yaml in the format referenced in the
parse_biflow.yaml.example file in this directory. This is useful for demo's
when you want to keep your username and password hidden.

See *.rst files for more explanations and VSQL examples.

See Tests.rst for real examples.

Notes:

Make sure passwordless ssh works on the remote FC.

This has been tested on python3.7.5, highly recomend running from a virtual
environment:

virtualenv enviroment_name -p python3.7
source enviroment_name/bin/activate
pip install pandas... etc

Some of the filters are processed on the box (time range and flow id), some are
processed off-box by pandas. This is to achieve maximum efficiency.
'''

import argparse
import os.path
import re
import socket
import subprocess
import sys
from argparse import RawDescriptionHelpFormatter
from os import path
from pathlib import Path

import pandas as pd
import yaml
from paramiko import SSHClient
from scp import SCPClient
import datetime


class AbortScriptException(Exception):
    '''Abort the script and clean up before exiting.'''


def parse_args():
    '''Parse sys.argv and return args'''

    parser = argparse.ArgumentParser(
        formatter_class=RawDescriptionHelpFormatter,
        description='This tool takes an input file, which is populated with\n'
        'a Vertica FC database query.\n\n'
        'Flow Collector credentials can be added by cli or\n'
        'populated by a ~/parse_biflow.yaml, in the format:\n\n'
        'ip: 10.208.108.101\nusername: root\npassword <mypassword>',
        epilog='E.g.: ./parse_biflow.py 10 -ip 10.90.67.28 -ci 10.90.67.25 -si 216.239.35.12 -st '
        '"2019-11-01 21:41" -lt "2019-11-01 23:41" -fi 68')
    parser.add_argument('EPOCH',
                        help='Only pull records from the past duration in minutes, UTC TimeZone')
    parser.add_argument('-ip', '--flow_collector_ip', type=str, default='None',
                        help='IP Address of the Flow Collector to collect Bi-Flow from')
    parser.add_argument('-u', '--flow_collector_username', type=str, default='None',
                        help='Username of the Flow Collector to collect Bi-Flow from')
    parser.add_argument('-p', '--flow_collector_password', type=str, default='None',
                        help='Password of the Flow Collector to collect Bi-Flow from')
    parser.add_argument('-fi', '--flow_id', type=str, default='All',
                        help='Optionally add a Flow Collector Flow ID to narrow down results '
                        'This filter is processed on the Flow Collector')
    parser.add_argument('-ci', '--client_ip_address', type=str, default='All',
                        help='Optionally filter by client ip address to narrow down results')
    parser.add_argument('-si', '--server_ip_address', type=str, default='All',
                        help='Optionally filter by server ip address to narrow down results')
    parser.add_argument('-t', '--time', action='store_true',
                        help='Add last_time to columns, this will result in every flow with '
                        'more than one hit to get a unique row, but is useful for debugging')
    parser.add_argument('-st', '--start_time', type=str, default='All',
                        help='Optionally add a Vertica Start date in the format '
                        '"2019-10-03 12:49", this has to be included with a --last_time, UTC')
    parser.add_argument('-lt', '--last_time', type=str, default='All',
                        help='Optionally add a Vertica Last date in the format "2019-10-03 13:49" '
                        'this has to be included with a --start_time, UTC')
    parser.add_argument('-pk', '--peak', type=str, default='None',
                        help='Request to see any bytes sums over a peak value')
    parser.add_argument('-c', '--cache', action='store_true',
                        help='Process cached data if found without getting new')
    parser.add_argument('-d', '--dry_run', action='store_true',
                        help='Generate the VSQL commands only')
    parser.add_argument('-f', '--force', action='store_true',
                        help='Bypass safety rails - very dangerous')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='turn on verbose messages, commands and outputs')

    return parser.parse_args()


def run_shell(cmd, quiet=False):
  '''Run a shell command and return the output

  Print the output and errors if debug is enabled
  Not using logger.debug as a bit noisy for this info
  '''

  if not quiet:
    print('...%s' % str(cmd))

  p = subprocess.Popen(
      cmd,
      stdout=subprocess.PIPE,
      stderr=subprocess.PIPE,
      shell=True)
  out, err = p.communicate()

  out = out.rstrip()
  err = err.rstrip()

  if str(out) != '0' and str(out) != '1' and out:
    print('  Shell STDOUT output:')
    print()
    print(out)
    print()
  if err:
    print('  Shell STDERR output:')
    print()
    print(err)
    print()
  return(out)


def banner(description):
    '''
    Display a bannerized print
    E.g.     banner("Kubernetes Join")
    '''

    banner = len(description)
    if banner > 200:
        banner = 200

    # First banner
    print('\n')
    for c in range(banner):
        print('*', end='')

    # Add description
    print('\n%s' % description)

    # Final banner
    for c in range(banner):
        print('*', end='')
    print('\n')


def GetHumanReadable(size, precision=2):
    suffixes = ['B', 'KB', 'MB', 'GB', 'TB']
    suffixIndex = 0
    while size > 1024:
        suffixIndex += 1  # increment the index of the suffix
        size = size / 1024.0  # apply the division
    return "%.*f %d" % (precision, size, suffixes[suffixIndex])


def GetProtocolString(x):
  'Convert a protocol id into its string'
  # Create a lookup table by iterating over the values in the module
  table = {num: name[8:] for name, num in vars(socket).items() if name.startswith("IPPROTO")}
  pid = x['protocol']

  return (table[pid])


# Define progress callback that prints the current percentage completed for the file
def progress4(filename, size, sent, peername):
    sys.stdout.write("(%s:%s) %s\'s progress: %.2f%%   \r"
                     % (peername[0], peername[1], filename, float(sent) / float(size) * 100))


def days_hours_minutes(td):
  return td.days, td.seconds // 3600, (td.seconds // 60) % 60


class Parseflow:
  '''
  Common base class for parsing a Vertica Database File.

  Create a connection with a remote Flow Connector
  Execute VSQL queries
  SCP output and display tables
  Sum bytes and packets and display
  '''

  pd.options.display.max_rows = None
  pd.options.display.max_columns = None
  pd.options.display.width = None

  num_ran = 0

  def __init__(self, args):
    self.verbose = args.verbose
    self.start = args.start_time
    self.last = args.last_time
    self.fi = args.flow_id
    self.flow_date = "'{}' to: '{}'".format(str(self.start).replace("\n", " "),
                                            str(self.last).replace("\n", " "))
    self.ip = 'None'
    self.username = 'None'
    self.password = 'None'
    self.ci = args.client_ip_address
    self.si = args.server_ip_address
    self.t = args.time
    self.peak = args.peak
    self.cache = args.cache
    self.dry_run = args.dry_run
    self.force = args.force

    # Process mandatory Epoch and make sure boundary at 60 minutes is obeyed
    self.epoch = int(args.EPOCH)
    if self.epoch > 60 and not self.force:
      check_epoch = input("Warning Epoch '{}' is greater than '60' which can cause excessive "
                          "CPU and memory. Are you sure (Y/N)? ".format(self.epoch))
      if check_epoch.upper() in ["N", "NO"]:
        sys.exit()

    # Now check if last_time - start_time Epoch is greater than master Epoch
    if self.start != 'All' and self.last != 'All':
      FMT = '%Y-%m-%d %H:%M'
      tdelta = (datetime.datetime.strptime(
          self.last, FMT) - datetime.datetime.strptime(self.start, FMT))
      if self.verbose:
        print('last_time - start_time epoch {}m, Epoch {}m'.format(
            tdelta.total_seconds() // 60, self.epoch))

      if tdelta.total_seconds() // 60 > self.epoch and not self.force:
        check_epoch = input('Warning: (last_time - start_time) Epoch {}m is greater '
                            'than master Epoch {}m. Are you sure (Y/N)? '.format(
                                int(tdelta.total_seconds() // 60), self.epoch))
        if check_epoch.upper() in ["N", "NO"]:
          sys.exit()

    # Check for a base configuration and load FC credentials if it exists
    # Allow for CLI overwrite of each specific value
    home = str(Path.home())
    yaml_config = '{}/parse_biflow.yaml'.format(home)

    if os.path.exists(yaml_config):
      # Load base configuration
      with open(yaml_config) as file:
        user_config = yaml.load(file, Loader=yaml.FullLoader)
      self.ip = user_config['ip']
      self.username = user_config['username']
      self.password = user_config['password']

    # Override base config with user inputs
    if args.flow_collector_ip != 'None':
      self.ip = args.flow_collector_ip

    if args.flow_collector_username != 'None':
      self.username = args.flow_collector_username

    if args.flow_collector_password != 'None':
      self.password = args.flow_collector_password

    # If neither base config nor CLI has credentials then exit
    if self.ip == 'None' or self.username == 'None' or self.password == 'None':
      banner('Error: must supply Flow Collector IP, Username and Password, using '
             'CLI or ~/parse_biflow.yaml')
      sys.exit()

    Parseflow.num_ran += 1

  def createVSQLquery(self):
    '''Create the VSQL query string

    Process time and flow_id filters using VSQL (for efficiency) - otherwise
    could be pulling a very large file'''

    # Apply Flow Id and Time range filters on-box
    add_filter = ""
    if self.fi != 'All' and (self.start != 'All' and self.last != 'All'):
      # Time and Flow Id filter
      add_filter = " where start_time >= '{}' and last_time <= '{}' and id = '{}'".format(
          self.start, self.last, self.fi)
    elif self.fi != 'All' and (self.start == 'All' or self.last == 'All'):
      # Flow ID filter only
      add_filter = " where id = '{}'".format(self.fi)
    elif self.fi == 'All' and (self.start != 'All' and self.last != 'All'):
      # Time filter only
      add_filter = " where start_time >= '{}' and last_time <= '{}'".format(
          self.start, self.last, self.fi)

    self.command = """/opt/vertica/bin/vsql -U dbadmin -w lan1cope -c "select id as flow_id, \
      v6_ntoa(client_ip_address) as client_ip_address, v6_ntoa(server_ip_address) as \
      server_ip_address, server_port, client_port, client_bytes, client_packets, server_bytes, \
      server_packets, server_port, protocol, start_time, last_time from flow_stats \
      {}" -Aq -P footer=off -F ',' > /lancope/var/parse_biflow
    """.format(add_filter)

  def getTimeChange(self):
    '''User has requested in minutes a time range (Epoch) from now to the past

    Note the TimeZone on the FC is in UTC'''

    if 'All' not in self.flow_date:
      # User has requested a flow date using -ci and -si - give that precedence
      # Todo: this currently bypasses the 60 minute boundary check
      return

    now = datetime.datetime.utcnow()

    diff = datetime.timedelta(minutes=self.epoch)
    pretty_now = str(now).split(".")[0]
    epoch_time = now - diff
    pretty_epoch_time = str(epoch_time).split(".")[0]

    self.flow_date = "'{}' to: '{}'".format(pretty_epoch_time, pretty_now)
    self.start = pretty_epoch_time
    self.last = pretty_now

    if self.verbose:
      print("Time now is: {}".format(pretty_now))
      print("Ten minutes in the epoch is: {}".format(pretty_epoch_time))
      print('New flow date "{}"'.format(self.flow_date))
      print('Start Time={}, Last Time={}'.format(self.start, self.last))

  def getFCData(self):
    '''Connect to the Flow Connector and query the database

    Compress the data and SCP the outcome locally to be processed'''

    if self.cache:
      banner('Skip retrieving latest data, process cached data off-box')
      return

    if self.dry_run:
      banner('Dry Run Begin')
      print('...Will run this VSQL command on Flow Collector({}):\n'.format(self.ip))
      print(re.sub(' +', ' ', self.command))
      return

    print('...Local:  SSH connect to Flow Collector({})'.format(self.ip))
    ssh = SSHClient()
    ssh.load_system_host_keys()
    ssh.connect(self.ip,
                username=self.username,
                password=self.password,
                look_for_keys=False,
                allow_agent=False)

    print('...Remote: Run VSQL Query command on Flow Collector({}):'.format(self.ip))
    print(re.sub(' +', ' ', self.command))

    # Run remote VSQL command
    stdin, stdout, stderr = ssh.exec_command(self.command, timeout=None)
    # Wait for the command to terminate
    exit_status = stdout.channel.recv_exit_status()  # Blocking call
    if exit_status != 0:
      print('Error', exit_status)

    # Compress output file for scp efficiency
    print('...Remote: Compress data')
    stdin, stdout, stderr = ssh.exec_command(
        'bzip2 -s -v -f /lancope/var/parse_biflow', timeout=None)
    # Wait for the command to terminate
    exit_status = stdout.channel.recv_exit_status()  # Blocking call
    if exit_status != 0:
      print('Error', exit_status)

    # SCP file back home
    print('...Remote: SCP output /lancope/var/parse_biflow.bz2 to /tmp/parse_biflow.bz2')
    scp = SCPClient(ssh.get_transport(), progress4=progress4)
    scp.get('/lancope/var/parse_biflow.bz2', '/tmp/parse_biflow.bz2')

    # Clean up temp file
    print('...Remote: Clean up data')
    stdin, stdout, stderr = ssh.exec_command('rm /lancope/var/parse_biflow.bz2', timeout=None)
    exit_status = stdout.channel.recv_exit_status()  # Blocking call
    if exit_status != 0:
      print('Error', exit_status)
    scp.close()

  def processFile(self):
    ''''Use Python Pandas to create a dataset'''

    # Dry run doesn't process data
    if self.dry_run:
      banner('Dry run end')
      sys.exit()

    if self.cache:
      # Trying to read data from previous retrieval
      if path.exists('/tmp/parse_biflow'):
        print('...Local:  Found and using previous data in /tmp/parse_biflow')
      else:
        print('...Local:  Error - no previous data to use found')
        sys.exit()
    elif path.exists('/tmp/parse_biflow.bz2'):
      # Using retrieved data
      print('...Local:  Uncompress data file')
      run_shell('bzip2 -d -f /tmp/parse_biflow.bz2', True)

    # At this point it's just processing data
    self.df = pd.read_csv('/tmp/parse_biflow')

  def displayRawData(self):
    '''Display the data with index and lists, debug only

    Not used but interesting reference for a Pandas dataset'''

    return

    if self.verbose:
      banner('Raw Data Output')

      if self.df.empty:
        print('DataFrame is empty!')
      else:
        for (index_label, row_series) in self.df.iterrows():
          print('Row Index label : ', index_label)
          print('Row Content as Series : ', row_series.values)
      print('\n')

  def displayData(self):
    '''Display the all the data in table form', verbose only'''

    if self.verbose:
      banner('All Flow Data')

      if self.df.empty:
        print('No Bi-Flow Data was found for these parameters!')
      else:
        print(self.df)
      print('\n')

  def displayPckByteSum(self):
    '''Sum up the packets and bytes per Flow ID and display

    Process command filters and remove data accordingly

    Also add count of how many flow_ids hits there were'''

    print('...Local:  Processing data (aggregating counts, flow_ids and applying filters)...')

    #
    # Filter (remove) rows now, before results are grouped and aggregated
    #

    # Remove all but the client ip's required
    if self.ci != 'All':
      self.df = self.df[self.df.client_ip_address == self.ci]

    # Remove all but the server ip's required
    if self.si != 'All':
      self.df = self.df[self.df.server_ip_address == self.si]

    if self.t is True:
      # Create table with start and last times, but won't aggregate flow ids
      self.df = self.df.groupby(
          ['flow_id',
           'client_ip_address',
           'client_port',
           'server_ip_address',
           'server_port',
           'start_time',
           'last_time',
           'protocol']).agg({'client_bytes': 'sum',
                             'client_packets': 'sum',
                             'server_bytes': 'sum',
                             'server_packets': 'sum',
                             'flow_id': 'count'})
    else:
      # Create table with all the basics and aggregated flow ids
      self.df = self.df.groupby(
          ['flow_id',
           'client_ip_address',
           'client_port',
           'server_ip_address',
           'server_port',
           'protocol']).agg({'client_bytes': 'sum',
                             'client_packets': 'sum',
                             'server_bytes': 'sum',
                             'server_packets': 'sum',
                             'flow_id': 'count'})

    # Rename aggregated flow_id to flow_id_hits
    self.df.rename(columns={'flow_id': 'hits'}, inplace=True)

    # Gather total bytes (client + server)
    self.df['Total'] = (self.df['client_bytes'] + self.df['server_bytes'])

    # Print total bytes in G (divide by 1e+9)
    self.df['Total (GB)'] = self.df['Total']
    self.df['Total (GB)'] /= (1024 * 1024 * 1024)

    # Add in bytes in M too
    self.df['Total (MB)'] = self.df['Total']
    self.df['Total (MB)'] /= (1024 * 1024)

    # Brief table headings
    self.df.rename(columns={'server_ip_address': 'srv_ip',
                            'server_bytes': 'srv_bytes',
                            'client_packets': 'client_pkts',
                            'server_packets': 'svr_pkts'},
                   inplace=True)

    # Reorder columns for consistency
    self.df = self.df[['hits', 'client_bytes', 'client_pkts',
                       'srv_bytes', 'svr_pkts', 'Total',
                       'Total (MB)', 'Total (GB)']]

    if not self.df.empty:
      if self.peak != 'None':
        # Remove all but the rows whose total bytes are greater than the peak
        # Do this here when the bytes are aggregated
        self.df = self.df[self.df['Total'] >= int(self.peak)]
        if self.df.empty:
          # Check for empty post peak operation
          print('No Bi-Flow Data was found for Total Bytes Peak of "{}"'.format(self.peak))
          print('\n')
          return

      # Build a banner - only those options which were added
      ban = '*Aggregated Bi-Flow Data for FC={}'. format(self.ip)
      ban += ' *EPOCH={}m*'.format(self.epoch)
      if self.fi != 'All':
        ban += ' *Flow_ID={}*'.format(self.fi)
      if self.ci != 'All':
        ban += ' *Client_IP={}*'.format(self.ci)
      if self.si != 'All':
        ban += ' *Server_IP={}*'.format(self.si)
      if self.start != 'All' and self.last != 'All':
        ban += ' *Date="{}"*'.format(self.flow_date)
      if self.peak != 'None':
        ban += ' *Peak={}*'.format(self.peak)
      if self.t:
        ban += ' *Time={}*'.format(self.t)
      if self.dry_run:
        ban += ' *Dry Run={}*'.format(self.dry_run)
      if self.cache:
        ban += ' *Cached={}*'.format(self.cache)
      banner(ban)
      print(self.df)
    else:
      print('No Bi-Flow Data was found for these parameters!')

    print('\n')


def main():
  '''Main function.'''

  args = parse_args()

  try:
    parse = Parseflow(args)

    parse.getTimeChange()
    parse.createVSQLquery()
    parse.getFCData()
    parse.processFile()
    parse.displayRawData()
    parse.displayData()
    parse.displayPckByteSum()

  except Exception:
    print('Exception caught:')
    print(sys.exc_info())
    raise


if __name__ == '__main__':
    main()
