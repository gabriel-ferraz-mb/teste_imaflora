# Gabriel Ferraz - Imaflora
---

Set of scripts to download yearly deforestation data from TerraBrasilis Web Feature Service, treat it and insert into SQL database

---
### Installation
---

__1. Clone this repository__
```
git clone git@github.com:gabriel-ferraz-mb/test_imaflora.git
```

__2. Create environment and install dependencies__

Windows ([conda](https://docs.conda.io/projects/conda/en/latest/user-guide/install/windows.html)):
```
$ conda create -n your_env python
$ conda activate your_env
(your_env) $ python -m pip install --upgrade pip wheel
(your_env) $ python -m pip install -r requirements.txt
```

Linux:
```
$ python -m venv --clear --copies your_env
$ source your_env/bin/activate
(your_env) $ python -m pip install --upgrade pip wheel
(your_env) $ python -m pip install -r requirements.txt
```

__3. Create `.env` file__

Create a `config.env` file at the root of the project following the `.env-sample` variables.
Note that the created file __MUST__ be `config.env`

---
### Usage
---

#### ETL TerraBrasilis Data

Tool created to download defeorestation dataset available in TerraBrasilis Web Feature Service. To choose and filter the dataset follow these steps:

1. Choose a brazilian biome name within the list:
```
    a) amazon
    b) mata-atlantica
    c) cerrado
    d) pantanal
    e) pampa
```   
Note that the string __MUST__ be written in the exact same way as in this list. This variable is __MANDATORY__, if `None` is passed code will break.

2. Choose a starting date, that __MUST__ be written in format `YYYY-MM-DD`
3. Choose a end date, that __MUST__ be written in format `YYYY-MM-DD`

Starting date and end date are optional variables, if nothing is passed after biome request will be made for 2000-2024 entire period. 

```
$ cd teste_imaflora
$ python example_wfs_oficial.py <"biome"> <"start_date"> <"end_date">

"biome": biome chosen among the listed above
"start_date": initial date of filter
"start_date" : final date of filter

```

__Example__
```
$ cd test_imaflora
$ python example_wfs_oficial.py "cerrado" "2020-01-01" "2021-01-01>
```
This will retrieve, treat and load into SQL table every deforestation geometry detected for cerrado biome in the year of 2020

---
