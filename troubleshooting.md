# Troubleshooting

## libmagic package

```
ImportError: failed to find libmagic. Check your installation
```

Install libmagic [from conda](https://anaconda.org/conda-forge/libmagic)

```
conda install -c conda-forge libmagic
```

## Getting MySQL up and running on macOS

1. Make sure your password encryption for “root” user is set on legacy, when you initialize your database on MySQL.prefPane 

2. Install MySQLdb. You can do this by running in a jupyter notebook cell:
```
conda install mysqlclient
```

## Getting MSSQL up and running on macOS

1. Run the following commmand in a jupyter notebokk cell:
```
pip install pyodbc
```

2. Then run the following commands on the terminal:
```
brew install unixodbc
```
(in my case, it appeared an error that got solved by running:)
```
sudo chown -R $(whoami) $(brew --prefix)/*
brew install unixodbc
```

```
brew tap microsoft/mssql-preview https://github.com/Microsoft/homebrew-mssql-preview

brew update

brew install mssql-tools
```
(again, in my case, it appeared another error that got solved by running:)
```
brew untap microsoft mssql-preview
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew update
brew install mssql-tools
```
