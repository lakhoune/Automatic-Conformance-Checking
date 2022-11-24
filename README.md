# Automatic-Conformance-Checking
Automatic Conformance Checking insights in Celonis. Create a interactive visualization. 

## Install Streamlit
### Prerequisites
- Python 3.7 - Python 3.10
- PIP
## Dependencies

- Install numpy
- Install pandas
- Install streamlit
- Install ploty 
- Install seaborn
- Other visualization libraries 

## Install Streamlit on Windows
- Install Anaconda
- If you don't have Anaconda install yet, follow the steps provided on the Anaconda installation page.


- Install Streamlit

    ```sh
  pip install streamlit
    ```
    
    - Test that the installation worked:

    ```sh
    streamlit hello

    ```
    
    
## `pyinsights is not a module`

This means you need to `/path/to/pyinsights-folder` to `PYTHONPATH` environment variable

## `packaging is not a module`

Pycelonis relies on this package but does not seem to install it, so we install it manually

```sh
pip install packaging
