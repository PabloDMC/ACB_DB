# ACB_DB

![Python Version](https://img.shields.io/badge/python-3.10-blue)
![Status](https://img.shields.io/badge/status-active-brightgreen)

`acb_db` is a Python package designed to scrape and manage data from the official ACB (Spanish Basketball League) website. It allows users to keep an up-to-date database of matches, players, shots and other relevant data related to the ACB league. The data is stored in a PostgreSQL database, which can be easily updated by running the provided scripts.

## Features

The `acb_db` package offers the following key features:

- Scrapes data for Liga Endesa, Copa del Rey, and Supercopa.
- Processes raw data into structured formats for database insertion.
- Initializes the database and populates static and dynamic tables.
- Updates the database with new matchday data seamlessly.
- Modular and automated workflows for easy maintenance.

## Visual Representation

Below is a visual representation of the database schema.
![Database Schema](ACB_db_diagram.png)

## Requirements

- Python 3.10 (tested only on Python 3.10 and 3.12)
- pip (Python package manager)
- Google Chrome (tested with version 131) and compatible ChromeDriver
- Dependencies specified in `requirements.txt`. 

## Installation

### 1. Clone the repository

First, clone this repository to your local machine. Open a terminal and run:

```bash
git clone https://github.com/PabloDMC/ACB_DB.git
cd ACB_DB
```

### 2. Create and activate a virtual environment

It is recommended to create a virtual environment to avoid conflicts with globally installed dependencies.

```bash
python -m venv .venv
.venv\Scripts\activate
```

### 3. Install dependencies

Install the required dependencies listed in `requirements.txt`:

```bash
pip install -r requirements.txt
```

### 4. Configure the .env file
This project uses a `.env` file to manage sensitive configuration variables such as database credentials. Create a `.env` file in the root directory of the project with the following format:

```dotenv
DB_NAME='your_database_name'
DB_USER='your_database_user'
DB_PASSWORD='your_database_password'
DB_HOST='your_database_host'
DB_PORT='your_database_port'
```

#### Description of Variables:
* ``DB_NAME``: The name of the database to connect to.
* ``DB_USER``: The username with access privileges for the database.
* ``DB_PASSWORD``: The password associated with the database user.
* ``DB_HOST``: The host address of the database server (e.g., localhost or a remote IP).
* ``DB_PORT``: The port number where the database server is running (default for PostgreSQL is 5432).

After creating the `.env` file, the application will automatically load these variables during execution to configure the database connection.

## Usage

The `main.py` script serves as the primary entry point for managing the database. It orchestrates two main workflows:

### 1. Initialization (`inicial`)

This mode is used when setting up the database for the first time. It:

1. Scrapes historical data for:
   * Liga Endesa (2020-2021 to present).
   * Copa del Rey (2020-2021 to present).
   * Supercopa (2020-2021 to present).
2. Processes the raw scraped data to prepare it for the database. This includes:
   * Cleaning player and team names.
   * Structuring data into well-defined tables.
3. Creates the database schema as defined in `schema.sql`.
4. Populates static tables like:
`clubes`, `competiciones`, `temporadas`, `equipos`.
5. Populates dynamic tables like:
`jugadores`, `jugadores_equipos`, `jornadas`, `partidos`, `tiros`.

```bash
python main.py inicial
```

### 2. Update (actualizacion)

This mode is used to update the database with new matchday data. It:

1. Scrapes:
   * Current player rosters in the league.
   * Round data for:
      * A specified round (`--jornada`).
      * A specified competition (`--competicion`).
      * A specified season (`--temporada`).
2. Processes the raw data to:
   * Clean and normalize team and player names.
   * Prepare the data for database insertion.
3. Updates the following dynamic tables in the database:
`jugadores`, `jugadores_equipos`, `jornadas`, `partidos`, `tiros`.

#### Example Command:
To update the database with data from round 15 of Liga Endesa in the 2024-2025 season:

```bash
python main.py actualizacion -j 15 -c "liga_endesa" -t 2023
```
## Using as a Package

This project is configured as an installable package. You can install it locally by running the following command from the root of the project:

```bash
git clone https://github.com/PabloDMC/ACB_DB.git
cd ACB_DB
pip install .
```

Once installed, you can import and use the modules in other Python projects. Below is an example of how to use the package:

### Example: Importing from the Scraping and Presentation modules:

An example could be scraping the shots of a single player in a season to represent them in a shot chart. Below, the `ScraperACB` class from the `acb_db` package is imported to scrape the shots, and the `shot_chart` function is used to create the chart.

```python
# Instanciar clase.
from acb_db.Scraping.scraper import ScraperACB
musa = ScraperACB()

# Partidos de Dzanan Musa en la temporada 2021-2022.
partidos_musa = musa.obtener_id_partidos_jugador('30001170','2021')

# Se iteran los partidos que jugó Musa para obtener sus lanzamientos.
tiros_musa = []
for i in partidos_musa:
    tiros_musa.extend(musa.obtener_tiros_jugador('D. Musa',i))
musa.cerrar_driver()

# Conversión a dataframe.
import pandas as pd
df = pd.DataFrame(tiros_musa, columns=[
                    'nombre', 'x', 'y', 'cuarto', 'tiempo', 'equipo_local',
                    'resultado_local', 'resultado_visitante', 'equipo_visitante', 
                    'descripcion', 'anotado', 'id_partido',
                    'id_jornada', 'temporada', 'competicion', 'playoff'
                ])

# Transformacion lineal de las coordenadas de los tiros. 
import numpy as np
df = df.astype({'x':'float', 'y':'float'})
df['coord_x']=df['x']
df['coord_y']=df['y']
df['y'] = np.where((df['coord_x'] - 24.750) * 6 < 1400,
                      (df['coord_x'] - 24.750) * 6,
                      abs((df['coord_x'] - 24.750) * 6 - 2800))
df['x'] = np.where((df['coord_x'] - 24.750) * 6 < 1400,
                      (df['coord_y'] - 141.25) * 6,
                      -(df['coord_y'] - 141.25) * 6)

# Tiros realizados por Dzanan Musa durante su temporada de MVP (2021-2022)
from acb_db.Presentation.court import shot_chart
shot_chart(df[df['anotado']==True],df[df['anotado']==False])
```
## Dzanan Musa (2021-2022)
![Musa](musa2021.png)
