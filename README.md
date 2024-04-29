# Sam's Thesis data processor

This is a simple data processor for Sam's thesis data. It reads the data from a CSV file, processes it and saves the results in a new CSV file.

## Installation

Since this is a python notebook you will need to have python installed. If you're using windows you can easily install python form the [Microsoft Store](https://apps.microsoft.com/detail/9ncvdn91xzqp?hl=en-us&gl=US). Otherwise you can download it from [here](https://www.python.org/downloads/).

After installing python you will need to install the dependencies prior to running the program. However first you will need to install poetry. You can do this by running the following commands:

```pwsh
# If you installed python using Microsoft Store, replace `python` with `python3` in the next line.
python -m pip install --user pipx
.\pipx.exe ensurepath
pipx install poetry
poetry install
```

Now that all dependencies are installed you can initiate a virtual environment by running the following command:

```pwsh
poetry shell
```

Normally you should see something like this:
![No text, just deal with it](resources/img/virtenv.png)
Keep note of the circled part, you will need it to select the kernel. In the right upper corner (assuming you're using Visual Studio Code) you will be able to select a kernel. Click on it and select the one with the same name as the circled part.
![No text, just deal with it](resources/img/select_kernel.png)

## Usage

### OBSE survery

in src/obse.ipynb you will find the code for the OBSE survey. You shouldn't really need to know what the code does, you just need to run it once so it creates the necesarry directories. After that you can place the CSV files in the resources/data directory and run the code again to process the data. The results will be saved in the resources/out directory
