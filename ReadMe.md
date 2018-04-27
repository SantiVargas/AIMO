AIMO
===
This project facilitates the collection of data from Ripe Atlas.

Requirements
---

The required python depencies can be installed by running the following command:
```
pip install ripe.atlas.cousteau ripe.atlas.sagan tldextract
```

Runtime
---

The project can be run as a python module. Alternatively, the project can be run through the command line by use of config files. 

### Command Line
Simply run the following command:
```
./main.py config/config_file.cfg
```

### Config File
The config file has the following structure
```
[main]
api_key = 1234567890
domains_file = config/default_domains_list.txt

[probe]
requested = 1
type = country
value = US
```
