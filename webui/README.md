# Coral Web Server

A stand-alone web server which provides RESTful API for tracking Coral jobs.

## Prerequisites

* Linux or MacOS
* Python 2.7 or higher
```
# On Ubuntu
sudo apt-get install python
# On MacOS
brew install python
```
* Flask 0.12.0 or higher
```
pip install flask
```
* SQLAlchemy
```
pip install sqlalchemy
```
* SQLite
```
# On Ubuntu
sudo apt-get install sqlite3 libsqlite3-dev
# On MacOS
brew install sqlite
```

## How to run it

```
export FLASK_APP=/path/to/coral/webui/coralwebserver/__init__.py
flask run
```
The server runs on "localhost:5000".
