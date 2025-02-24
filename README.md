# Gabriel Ferraz - Imaflora
---

Script to download EMBARGOS IBAMA from Web Feature Service

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
 
---
### Usage
---

#### Download Embargo Ibama

Tool created to download Embargos Ibama.

```
$ cd test_imaflora
$ python test_imaflora.py <"ano_inicio"> <"ano_fim"> <"uf">

"ano_inicio" : filter results greater or equal to a year
"ano_fim" : filter results lesser or equal to a year
"ano_inicio" : filter results for a determined brazilian state code

The three variables are mandatory, if any of them is not passed the code will return an error message
```
__Example__
```
$ cd test_imaflora
$ python test_imaflora.py "2019" "2024" "SP"
```
---
