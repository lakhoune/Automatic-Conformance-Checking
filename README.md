# Automatic-Conformance-Checking

Automatic Conformance Checking insights in Celonis

## Dependencies

- Install pycelonis

    ```sh
    pip install --extra-index-url=https://pypi.celonis.cloud/ pycelonis=="1.7.3"
    ```

- Install dotenv

    ```sh
    pip install python-dotenv
    ```

## `pyinsights is not a module`

This means you need to `/path/to/pyinsights-folder` to `PYTHONPATH` environment variable

## `packaging is not a module`

Pycelonis relies on this package but does not seem to install it, so we install it manually

```sh
pip install packaging
```
