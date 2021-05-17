# Exploratory Learning Analytics Toolkit for Students (ELAS) - Backend

## Table of Contents

* [Project Info](#project-info)
* [General info](#general-info)
* [Project structure](#project-structure)
* [Technologies](#technologies)
* [Additional applications](#Additional-applications)
* [Setup](#setup)

## Project Info

This repository consists of the web server made with Flask for ELAS Lab Project. Each group will receive access to a
branch and should only work on project inside ```resources``` folder they are assigned to.

## Project structure

```
│   .gitignore
│   example.env                         # rename to .env
│   example.flaskenv                    # rename to .flaskenv
│   README.md
│   requirements.txt                    # necessary python libraries to install
│
├───application
│   │   extensions.py                   # initialization of libraries
│   │   main.py                         # application REST APIs
│   │   settings.py                     # global constants
│   │   __init__.py                     # application script file
│   │
│   └───resources                       # resources folder consists of all projects
│       │   __init__.py
│       │
│       ├───course_insights
│       │       course_insights.py      # Course Insights project REST APIs
│       │       __init__.py
│       │
│       ├───e3_selector
│       │       e3_selector.py          # E3 Selector project REST APIs
│       │       __init__.py
│       │
│       ├───intogen
│       │       intogen.py              # Intogen project REST APIs
│       │       __init__.py
│       │
│       └───spoa
│               spoa.py                 # SPOA project REST APIs
│               __init__.py
│
├───static
└───templates
```

## Technologies

Project is created with:

* [Python](https://www.python.org/downloads/release/python-387/) (v3.8.7)
* [Flask](https://flask.palletsprojects.com/en/1.1.x/) (v1.1.2)
* [MongoDB Community Server](https://www.mongodb.com/try/download/community) (v4.4.4)

## Additional applications

* [Postman](https://www.postman.com/downloads/)
* [Visual Studio Code](https://code.visualstudio.com/download)
  or [Pycharm Professional](https://www.jetbrains.com/de-de/pycharm/download/#section=windows)
* Mongo Compass (check the box to install when installing MongoDB Community Server)

## Setup

Step 1:- Download the latest version of Python from [Python official website](https://www.python.org/downloads/).

Step 2:- Download and install MongoDB Community Server
from [MongoDB official website](https://www.mongodb.com/try/download/community).

Step 3:- Install and activate virtualenv

Move inside to the project folder and open a terminal or cmd. Type the following commands to install virtualenv.

```
$ pip install virtualenv
$ pip install virtualenvwrapper-win
```

Step 4:- Create a virtual environment

```
$ virtualenv venv
```

Step 5:- Activate the virtual environment

```
$ .\venv\Scripts\activate
```

Step 6:- Install the python packages from the requirements file

```
$ pip install -r requirements.txt
```

Step 7:- Rename the file ```example.env``` to ```.env```.

Step 8:- Rename the file ```example.flaskenv``` to ```.flaskenv```.

Step 9:- Run the project either from a terminal or command prompt or powershell:

```
$ flask run
```

Web server should start running at localhost:5000.


*+++++