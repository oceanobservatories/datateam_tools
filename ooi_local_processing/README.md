# OOI Local Data Processing
This repo provides an example of how to recreate the OOI data processing flow from raw, through parsed to processed data product on your local machine.

## Resources
Raw Data https://rawdata.oceanobservatories.org/files/   
Calibration Files https://github.com/ooi-integration/asset-management/tree/master/calibration   
Deployment Files https://github.com/ooi-integration/asset-management/tree/master/deployment   
Parsers https://github.com/oceanobservatories/mi-instrument   
Algorithms https://github.com/oceanobservatories/ion-functions  


## OSX/Anaconda Setup Instructions

Grab the parser code and set up your environment.

```
$ git clone https://github.com/oceanobservatories/mi-instrument.git
$ cd mi-instrument
$ conda create -n mi pip cython numpy docopt ipykernel netcdf4
$ source activate mi
$ pip install -r requirements.txt
$ pip install -e .
```

Set up jupyter notebook in your environment and make your environment kernel selectable.

```
$ python -m ipykernel install --user --name mi
```


In this repository is a raw data file from RS01SBPS-SF01A-CTDPFA102, which we will parse and process for this example. You can also find this file on the raw data archive at https://rawdata.oceanobservatories.org/files/RS01SBPS/SF01A/CTDPFA102/2017/10/

```
./raw_data/CTDPFA102_10.33.3.195_2101_20171005T0000_UTC.dat
```

Install some dependancies for the pygsw package

```
$ brew tap lukecampbell/homebrew-libgswteos
$ brew install libgswteos-10
$ brew test -v libgswteos-10
```

Grab the ion-functions (also known as Data Product Algorithms or Processors) and install the remaining packages to your environment.

```
$ git clone https://github.com/oceanobservatories/ion-functions.git
$ cd ion-functions
$ source activate mi
$ pip install -r requirements.txt
$ pip install -e .
```

Now launch jupyter notebook and open parse_process.ipynb. Make sure you select mi as your kernel.

```
$ jupyter notebook
```
