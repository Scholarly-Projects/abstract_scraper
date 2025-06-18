# Prerequisites
Python 3.8 or later installed

Check your Python version with:

bash
```
python --version
```
If you receive a "command not found" or similar error, install Python using the instructions below.

## 1. Installing Python

macOS
If you have Homebrew installed:

bash

```
brew install python
```
Alternatively, download the latest installer from:

https://www.python.org/downloads/mac-osx/

Windows

Download the latest Python installer from:

https://www.python.org/downloads/windows/

During installation, make sure to check the box labeled:

pgsql

Add Python to PATH

## 2. Organize Your Project Directory
Create and structure your working directory as follows:

bash

merge_abstracts_tool/
├── A/                        # Folder containing working_copy_raw_assets.json
├── B/                        # Folder containing working_copy_publications_abstracts_scraped.json
├── C/                        # Folder where output will be saved
├── script.py                 # The main Python script
└── setup.md                  # This setup file

Place your JSON input files in folders A/ and B/ respectively.

## 3. (Optional) Create and Activate a Virtual Environment
A virtual environment is recommended to manage dependencies separately from your global Python installation.

macOS / Linux
bash

```
python3 -m venv .venv
source .venv/bin/activate
```

Windows 

python -m venv .venv
source .venv/Scripts/activate

## 4. Install Required Dependencies

This script depends on the ijson package, which allows efficient streaming of large JSON files.

Install it with:

bash

```
pip install ijson
```

You can verify that it installed correctly by running:

bash

```
pip show ijson
```

## 5. Run the Script

Once dependencies are installed and your environment is activated, run the script with the following command:

bash

```
python script.py A/working_copy_raw_assets.json B/working_copy_publications_abstracts_scraped.json C/merged_output.json
```

The first argument is the path to the raw assets input file.

The second argument is the path to the abstracts file.

The third argument is the path to the desired output file.

## 6. Deactivate the Virtual Environment (When Finished)

Once you're done, you can deactivate the virtual environment with:

bash

```
deactivate
```
