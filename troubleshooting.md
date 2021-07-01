# Troubleshooting

## libmagic package

```
ImportError: failed to find libmagic. Check your installation
```

Install libmagic [from conda](https://anaconda.org/conda-forge/libmagic)

```
conda install -c conda-forge libmagic
````

## pydateinfer module

```
ModuleNotFoundError: No module named 'pydateinfer'
```

Install infer date format library from git

```
pip install git+https://github.com/hi-primus/dateinfer.git
```

## url_parser module

```
ModuleNotFoundError: No module named 'url_parser'
```

Install url parser library from git

```
pip install git+https://github.com/hi-primus/url_parser.git
```