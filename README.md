# Lighthouse CDC parsing tools

This is a tool to make a comparison between a master file of CDC Labs,
retrieved and combined from: <https://www.cdc.gov/clia/LabSearch.html>, and a
second search that presumably contained new data, in the form of one or
multiple files of data.

The purpose is to document new CLIA labs and closed CLIA labs.

For options execute: ./cdc.py -html

The environment needs python3 and pandas modules to be installed.

Output is four files: closed_clia, new_clia, unchanged_clias and
new_master_clia - all in CSV format.

Example of running:

```bash
    ➜  lighthouse git:(master) ✗ ./cdc.py TestCaptures/master_50.csv TestCaptures/hospital_25_5_5.csv TestCaptures/independent_25_5_5.csv

    Old master CLIA (50) data...
    Combine and save new data files (50)...

    Number of rows displayed restricted to '20'



    **********************************************************
    New (10) CLIA (Labs in new data not present in old Master)
    **********************************************************

            CLIA     FACILITY_TYPE CERTIFICATE_TYPE                                    LAB_NAME                            STREET        CITY STATE    ZIP           PHONE Contact Touch 1 Touch 2 Touch 3 Touch 4 Call Tag 1 Call Tag 2
    0  01D2869487  Physician Office       Compliance                          Rich and Rondi lab             331 Parks Ave Suite B  Scottsboro    AL  35768  (256) 218-3022     nan     nan     nan     nan     nan        nan        nan
    1  01D#027938       Independent    Accreditation                              Trump Holdings           315 West Hickory Street   Sylacauga    AL  35150  (256) 401-4160     nan     nan     nan     nan     nan        nan        nan
    2  01C2154450       Independent       Compliance                             Surfboards R US              315 W Hickory Street   Sylacauga    AL  35150  (256) 239-7614     nan     nan     nan     nan     nan        nan        nan
    3  01A2159731       Independent       Compliance                             By some medison             117 N Chalkville Road  Trussville    AL  35173  (205) 848-2273     nan     nan     nan     nan     nan        nan        nan
    4  01S0301038       Independent       Compliance          University of Essex Medical Center              850 Peter Bryce Blvd  Tuscaloosa    AL  35401  (205) 348-1222     nan     nan     nan     nan     nan        nan        nan
    5  01W1060994       Independent       Compliance                  University of Wyoming labs      300 Towncenter Blvd  Suite D  Tuscaloosa    AL  35406  (205) 752-2367     nan     nan     nan     nan     nan        nan        nan
    6  01E2000676       Independent       Compliance                            Bob and Mary LTD    1406 Mcfarland Blvd  Suite 1-A  Tuscaloosa    AL  35406  (205) 759-4507     nan     nan     nan     nan     nan        nan        nan
    7  01Q2125016       Independent       Compliance  Laboratory Corporation Of America Holdings  1022 North 1st Street  Suite 500   Alabaster    AL  35007  (205) 621-8490     nan     nan     nan     nan     nan        nan        nan
    8  01E2340676  Physician Office    Accreditation                          Linda and Mike LTD    1406 Mcfarland Blvd  Suite 1-A  Whitstable    AL  35406  (205) 759-4507     nan     nan     nan     nan     nan        nan        nan
    9  01Q2129916       Independent       Compliance                        Jadon and Chloe Labs  1022 North 1st Street  Suite 500   Alabaster    TX  35007  (205) 621-8490     nan     nan     nan     nan     nan        nan        nan


    **************************************************************************
    Closed (10) CLIA (Labs present in only in the master and not the new data)
    **************************************************************************

            CLIA     FACILITY_TYPE CERTIFICATE_TYPE                                          LAB_NAME                               STREET          CITY         STATE     ZIP       PHONE Contact     Touch 1 Touch 2 Touch 3 Touch 4 Call Tag 1  Call Tag 2
    0  39D2163448  Physician Office              PPM                        Mhlc Ob/gyn At Lankenau Dh               3001 Garrett Road #c,    Drexel Hill  PENNSYLVANIA  19026-  6106427714     nan         nan     nan     nan     nan        nan         nan
    1  39D2163463  Physician Office              REG                   Main Line Fertility - Havertown  2010 West Chester Pike, Suite 350,      Havertown  PENNSYLVANIA  19083-  6108531112     nan         nan     nan     nan     nan        nan         nan
    2  39D2163471  Physician Office              REG  Allegheny Hlth Network Cancer Institute - Beaver                     81 Wagner Road,         Monaca  PENNSYLVANIA  15061-  4127703518     nan         nan     nan     nan     nan        nan         nan
    3  39D2163672  Physician Office              REG        The Occupational Health Center - East Side                  4950 Buffalo Road,           Erie  PENNSYLVANIA  16510-  8144527879     nan         nan     nan     nan     nan        nan         nan
    4  39D2163673  Physician Office              PPM               Mlhc Ob/gyn At Lankenau Springfield       925 Baltimore Pike, Suite 2b,    Springfield  PENNSYLVANIA  19064-  6106427714     nan         nan     nan     nan     nan        nan         nan
    5  45D2163312             Other              REG                                   Csl Plasma, Inc                 1900 S 23rd Street,        Mcallen         TEXAS  78503-  5619123048     nan         nan     nan     nan     nan        nan         nan
    6  45D2163579             Other              REG                  Medical City Er White Settlement           9650 White Settlement Rd,     Fort Worth         TEXAS  76108-  8173475877     nan         nan     nan     nan     nan        nan         nan
    7  45D2163656             Other              REG                                   Clear Choice Er           7105 Bartlett, Suite 101,         Laredo         TEXAS  78041-  9562065980     nan         nan     nan     nan     nan        nan         nan
    8  36D2163337       Independent              REG                Consultants In Laboratory Medicine            8768 Big Cypress Circle,       Sylvania          OHIO  43560-  4192912693     nan  4.2.19 LVM     nan     nan     nan       PTLD  Consulting
    9  39D2163461       Independent              REG                Main Line Fertility - Tower Health              301 South Seventh Ave,   West Reading  PENNSYLVANIA  19611-  4842582880     nan         nan     nan     nan     nan        nan         nan


    *****************************************************************
    Unchanged (40) CLIA (Labs were present in old Master and new data
    *****************************************************************

            CLIA     FACILITY_TYPE CERTIFICATE_TYPE                                     LAB_NAME                                             STREET              CITY           STATE     ZIP       PHONE                                        Contact     Touch 1 Touch 2 Touch 3 Touch 4 Call Tag 1  Call Tag 2
    0   33D2163782  Physician Office              REG                    Michele C Pauporte Md Llc                    120 East 86th Street - Area 2,          Manhattan        NEW YORK  10028-  2124271898                                            nan         nan     nan     nan     nan        nan         nan
    1   33D2163821  Physician Office              REG                   David Abayev Gynecology Pc                      104-20 Queens Blvd - Apt 1b,       Forest Hills        NEW YORK  11375-  7188802212                                            nan         nan     nan     nan     nan        nan         nan
    2   34D2163473             Other              PPM        Wake Forest University Baptist Health  Deac Clinic Attention Nicole Allen, Wake Fores...     Winston-Salem  NORTH CAROLINA  27101-  3367163322                                            nan         nan     nan     nan     nan        nan         nan
    3   36D2163335             Other              REG                              Csl Plasma, Inc                               28301 Chardon Road,   Willoughby Hills            OHIO  44092-  5619123048                                            nan         nan     nan     nan     nan       PTLD         nan
    4   36D2163176  Physician Office              PPM              Blanchard Valley Obstetrics And                                  1740 N Perry St,             Ottawa            OHIO  45875-  4195237000                                            nan         nan     nan     nan     nan        nan         nan
    ..         ...               ...              ...                                          ...                                                ...               ...             ...     ...         ...                                            ...         ...     ...     ...     ...        ...         ...
    43  52D2163824  Physician Office              REG          Heart And Vascular Of Wisconsin, Sc                       5045 W Grande Market Drive,           Appleton       WISCONSIN  54913-  9204194407                                            nan         nan     nan     nan     nan        nan         nan
    44  34D2163226       Independent              REG   Laboratory Corporation Of America Holdings                             864 Black Creek Road,          Four Oaks  NORTH CAROLINA  27524-  9199633148                                            nan         nan     nan     nan     nan        nan         nan
    45  34D2163230       Independent              REG   Laboratory Corporation Of America Holdings                              410 Canteberry Road,         Smithfield  NORTH CAROLINA  27577-  9199345149                                            nan         nan     nan     nan     nan        nan         nan
    46  34D2163231       Independent              REG  Laboratory Corporation Of American Holdings                               236 Butternut Lane,            Clayton  NORTH CAROLINA  27520-  9193591011                                            nan         nan     nan     nan     nan        nan         nan
    48  37D2163428       Independent              REG                            Neo Services, Llc                                 7020 S Utica Ave,              Tulsa        OKLAHOMA  74136-  9188956657  Physician Group Lab- kyang@neo-laboratory.com  4.2.19 LVM     nan     nan     nan       PTLD  Recruiting

    [40 rows x 16 columns]


    **********************************
    CLIA Master (50) (unchanged + new)
    **********************************

            CLIA     FACILITY_TYPE CERTIFICATE_TYPE                                    LAB_NAME                                             STREET              CITY           STATE     ZIP           PHONE Contact Touch 1 Touch 2 Touch 3 Touch 4 Call Tag 1 Call Tag 2
    0   33D2163782  Physician Office              REG                   Michele C Pauporte Md Llc                    120 East 86th Street - Area 2,          Manhattan        NEW YORK  10028-      2124271898     nan     nan     nan     nan     nan        nan        nan
    1   33D2163821  Physician Office              REG                  David Abayev Gynecology Pc                      104-20 Queens Blvd - Apt 1b,       Forest Hills        NEW YORK  11375-      7188802212     nan     nan     nan     nan     nan        nan        nan
    2   34D2163473             Other              PPM       Wake Forest University Baptist Health  Deac Clinic Attention Nicole Allen, Wake Fores...     Winston-Salem  NORTH CAROLINA  27101-      3367163322     nan     nan     nan     nan     nan        nan        nan
    3   36D2163335             Other              REG                             Csl Plasma, Inc                               28301 Chardon Road,   Willoughby Hills            OHIO  44092-      5619123048     nan     nan     nan     nan     nan       PTLD        nan
    4   36D2163176  Physician Office              PPM             Blanchard Valley Obstetrics And                                  1740 N Perry St,             Ottawa            OHIO  45875-      4195237000     nan     nan     nan     nan     nan        nan        nan
    ..         ...               ...              ...                                         ...                                                ...               ...             ...     ...             ...     ...     ...     ...     ...     ...        ...        ...
    45  01W1060994       Independent       Compliance                  University of Wyoming labs                       300 Towncenter Blvd  Suite D        Tuscaloosa              AL   35406  (205) 752-2367     nan     nan     nan     nan     nan        nan        nan
    46  01E2000676       Independent       Compliance                            Bob and Mary LTD                     1406 Mcfarland Blvd  Suite 1-A        Tuscaloosa              AL   35406  (205) 759-4507     nan     nan     nan     nan     nan        nan        nan
    47  01Q2125016       Independent       Compliance  Laboratory Corporation Of America Holdings                   1022 North 1st Street  Suite 500         Alabaster              AL   35007  (205) 621-8490     nan     nan     nan     nan     nan        nan        nan
    48  01E2340676  Physician Office    Accreditation                          Linda and Mike LTD                     1406 Mcfarland Blvd  Suite 1-A        Whitstable              AL   35406  (205) 759-4507     nan     nan     nan     nan     nan        nan        nan
    49  01Q2129916       Independent       Compliance                        Jadon and Chloe Labs                   1022 North 1st Street  Suite 500         Alabaster              TX   35007  (205) 621-8490     nan     nan     nan     nan     nan        nan        nan

    [50 rows x 16 columns]


    **************************
    Results saved to CSV files
    **************************


    Saved new CLIA data to:        'Output/new_clia_data_2020-05-18-13:45:31.csv'
    Saved closed CLIA data to:     'Output/closed_clia_data_2020-05-18-13:45:31.csv'
    Saved unchanged CLIA data to:  'Output/unchanged_clia_data_2020-05-18-13:45:31.csv'
    Saved new master CLIA data to: 'Output/new_master_clia_data_2020-05-18-13:45:31.csv'
    ➜  lighthouse git:(master) ✗
```
