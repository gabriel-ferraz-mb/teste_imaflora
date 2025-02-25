# Gabriel Ferraz - Imaflora
---

Script to download Data from TerraBrasilis Web Feature Service

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

Create a `.env` file at the root of the project following the `.env-sample` variables.

__4. Have `wfs_info` file__

Make sure `wfs_info.json` is in the same folder as the required scripts

---
### Usage
---

#### ETL TerraBrasilis Data

Tool created to download any geographic dataset available in TerraBrasilis Web Feature Service. To choose and filter the dataset follow these steps:

1. In `wfs_info.json` each element of the JSON is a valid workspace name. Choose any of the available paths.
2. Inside the chosen workspace path, the available layers within that workspace are listed in "layers".
   There, it is possible to see the columns structure of each layer. Choose any of the available layers listed bellow the chosen workspace
3. If the chosen dataset has a `year` column, it is possible to subset da data for desired time frame.

```
$ cd teste_imaflora
$ python run_etl_terrabrasilis.py <"workspace"> <"layer"> <"ano_inicio"> <"ano_fim">

"workspace": workspace name
"layer": layer name
"ano_inicio" : filter results greater or equal to a given year
"ano_fim" : filter results older or equal to a given year
```

If the code receives a non-existent workspace or workspace/layer, it will return a error message.
If the dataset has a `year` column, the start and end years area mandatory input as well. Otherwise, it is not required.

__Example__
```
$ cd test_imaflora
$ python run_etl_terrabrasilis.py "prodes-cerrado-nb" "yearly_deforestation" "2021" "2023"
```
---
