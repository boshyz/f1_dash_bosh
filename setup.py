from setuptools import setup, find_packages

setup(author = "Boshy Zhao",
      description= "A package to download F1 data using the Jolpica API and store it in a postgres db",
      name = "bosh_f1_jolpica",
      version = "0.1.0",
      packages = find_packages(include = ["bosh_f1_jolpica"]),
      install_requires = ["requests", "pandas>1.0", "numpy", "psycopg2"
                          , "datetime", "sqlalchemy", "dotenv"] ,                      
       python_requires = ">2.7"  )