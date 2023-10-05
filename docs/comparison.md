# Library size comparison


## Databricks Connect

Compressed:

```shell
$ cd $(mktemp -d) && pip3 wheel databricks-connect && echo "All wheels $(du -hs)" && echo "1Mb+ wheels: $(find . -type f -size +1M | xargs du -h | sort -h -r)" && cd -
All wheels:
60M

1Mb+ wheels: 
 23M	./pyarrow-13.0.0-cp311-cp311-macosx_11_0_arm64.whl
 13M	./numpy-1.26.0-cp311-cp311-macosx_11_0_arm64.whl
 10M	./pandas-2.1.1-cp311-cp311-macosx_11_0_arm64.whl
9.1M	./grpcio-1.59.0-cp311-cp311-macosx_10_10_universal2.whl
2.0M	./databricks_connect-14.0.1-py2.py3-none-any.whl
```

Uncompressed:

```shell
databricks-connect $ python3 -m venv venv
databricks-connect $ source ./venv/bin/activate
(venv) databricks-connect $ pip install databricks-connect
...
Successfully installed certifi-2023.7.22 charset-normalizer-3.3.0 databricks-connect-14.0.1 databricks-sdk-0.10.0 
googleapis-common-protos-1.60.0 grpcio-1.59.0 grpcio-status-1.59.0 idna-3.4 numpy-1.26.0 pandas-2.1.1 protobuf-4.24.4 
py4j-0.10.9.7 pyarrow-13.0.0 python-dateutil-2.8.2 pytz-2023.3.post1 requests-2.31.0 six-1.16.0 tzdata-2023.3 
urllib3-2.0.6
(venv) databricks-connect $ du -hs .
312M	.
(venv) databricks-connect $ du -ha . | sort -h -r | head
312M	./venv
312M	.
311M	./venv/lib/python3.11/site-packages
311M	./venv/lib/python3.11
311M	./venv/lib
 88M	./venv/lib/python3.11/site-packages/pyarrow
 71M	./venv/lib/python3.11/site-packages/pandas
 60M	./venv/lib/python3.11/site-packages/numpy
 41M	./venv/lib/python3.11/site-packages/pyarrow/libarrow.1300.dylib
 36M	./venv/lib/python3.11/site-packages/pandas/tests
(venv) databricks-connect $ find . -type f -size +1M | xargs du -h | sort -h -r
 41M	./venv/lib/python3.11/site-packages/pyarrow/libarrow.1300.dylib
 30M	./venv/lib/python3.11/site-packages/grpc/_cython/cygrpc.cpython-311-darwin.so
 22M	./venv/lib/python3.11/site-packages/numpy/.dylibs/libopenblas64_.0.dylib
 10M	./venv/lib/python3.11/site-packages/pyarrow/libarrow_flight.1300.dylib
3.7M	./venv/lib/python3.11/site-packages/pyarrow/libparquet.1300.dylib
3.5M	./venv/lib/python3.11/site-packages/numpy/.dylibs/libgfortran.5.dylib
3.3M	./venv/lib/python3.11/site-packages/pyarrow/lib.cpython-311-darwin.so
3.0M	./venv/lib/python3.11/site-packages/numpy/core/_multiarray_umath.cpython-311-darwin.so
2.6M	./venv/lib/python3.11/site-packages/pyarrow/libarrow_substrait.1300.dylib
2.5M	./venv/lib/python3.11/site-packages/pyarrow/libarrow_dataset.1300.dylib
2.0M	./venv/lib/python3.11/site-packages/pyarrow/libarrow_acero.1300.dylib
1.9M	./venv/lib/python3.11/site-packages/pyarrow/libarrow_python.dylib
1.9M	./venv/lib/python3.11/site-packages/pandas/_libs/join.cpython-311-darwin.so
1.7M	./venv/lib/python3.11/site-packages/pandas/_libs/groupby.cpython-311-darwin.so
1.7M	./venv/lib/python3.11/site-packages/pandas/_libs/algos.cpython-311-darwin.so
1.6M	./venv/lib/python3.11/site-packages/pandas/_libs/hashtable.cpython-311-darwin.so
1.1M	./venv/lib/python3.11/site-packages/pandas/_libs/interval.cpython-311-darwin.so
1.0M	./venv/lib/python3.11/site-packages/pyarrow/_flight.cpython-311-darwin.so
1.0M	./venv/lib/python3.11/site-packages/pyarrow/_compute.cpython-311-darwin.so
(venv) databricks-connect $ echo "Direct dependencies $(cat venv/lib/python3.11/site-packages/databricks_connect-14.0.1.dist-info/METADATA | grep Requires-Dist | wc -l)"
Direct dependencies       23
```


### Databricks SQL Connector

Compressed:

```shell
$ cd $(mktemp -d) && pip3 wheel databricks-sql-connector && echo "All wheels $(du -hs)" && echo "1Mb+ wheels: $(find . -type f -size +1M | xargs du -h | sort -h -r)" && cd -
All wheels:
51M

1Mb+ wheels: 
 23M	./pyarrow-13.0.0-cp311-cp311-macosx_11_0_arm64.whl
 13M	./numpy-1.26.0-cp311-cp311-macosx_11_0_arm64.whl
 10M	./pandas-2.1.1-cp311-cp311-macosx_11_0_arm64.whl
1.5M	./SQLAlchemy-1.4.49-cp311-cp311-macosx_10_9_universal2.whl
```

Uncompressed:

```shell
databricks-sql-connector $ python3 -m venv venv
➜  databricks-sql-connector $ source ./venv/bin/activate
(venv) databricks-sql-connector $ pip install databricks-sql-connector
... 
Successfully installed Mako-1.2.4 MarkupSafe-2.1.3 alembic-1.12.0 certifi-2023.7.22 charset-normalizer-3.3.0 
databricks-sql-connector-2.9.3 et-xmlfile-1.1.0 idna-3.4 lz4-4.3.2 numpy-1.26.0 oauthlib-3.2.2 openpyxl-3.1.2 
pandas-2.1.1 pyarrow-13.0.0 python-dateutil-2.8.2 pytz-2023.3.post1 requests-2.31.0 six-1.16.0 sqlalchemy-1.4.49 
thrift-0.16.0 typing-extensions-4.8.0 tzdata-2023.3 urllib3-2.0.6
(venv) databricks-sql-connector $ du -hs .
280M	.
(venv) databricks-sql-connector $ du -ha . | sort -h -r | head
280M	./venv/lib/python3.11/site-packages
280M	./venv/lib/python3.11
280M	./venv/lib
280M	./venv
280M	.
 88M	./venv/lib/python3.11/site-packages/pyarrow
 71M	./venv/lib/python3.11/site-packages/pandas
 60M	./venv/lib/python3.11/site-packages/numpy
 41M	./venv/lib/python3.11/site-packages/pyarrow/libarrow.1300.dylib
 36M	./venv/lib/python3.11/site-packages/pandas/tests
(venv) databricks-sql-connector $ find . -type f -size +1M | xargs du -h | sort -h -r
 41M	./venv/lib/python3.11/site-packages/pyarrow/libarrow.1300.dylib
 22M	./venv/lib/python3.11/site-packages/numpy/.dylibs/libopenblas64_.0.dylib
 10M	./venv/lib/python3.11/site-packages/pyarrow/libarrow_flight.1300.dylib
3.7M	./venv/lib/python3.11/site-packages/pyarrow/libparquet.1300.dylib
3.5M	./venv/lib/python3.11/site-packages/numpy/.dylibs/libgfortran.5.dylib
3.3M	./venv/lib/python3.11/site-packages/pyarrow/lib.cpython-311-darwin.so
3.0M	./venv/lib/python3.11/site-packages/numpy/core/_multiarray_umath.cpython-311-darwin.so
2.6M	./venv/lib/python3.11/site-packages/pyarrow/libarrow_substrait.1300.dylib
2.5M	./venv/lib/python3.11/site-packages/pyarrow/libarrow_dataset.1300.dylib
2.1M	./venv/lib/python3.11/site-packages/databricks/sql/thrift_api/TCLIService/__pycache__/ttypes.cpython-311.pyc
2.0M	./venv/lib/python3.11/site-packages/pyarrow/libarrow_acero.1300.dylib
2.0M	./venv/lib/python3.11/site-packages/databricks/sql/thrift_api/TCLIService/ttypes.py
1.9M	./venv/lib/python3.11/site-packages/pyarrow/libarrow_python.dylib
1.9M	./venv/lib/python3.11/site-packages/pandas/_libs/join.cpython-311-darwin.so
1.7M	./venv/lib/python3.11/site-packages/pandas/_libs/groupby.cpython-311-darwin.so
1.7M	./venv/lib/python3.11/site-packages/pandas/_libs/algos.cpython-311-darwin.so
1.6M	./venv/lib/python3.11/site-packages/pandas/_libs/hashtable.cpython-311-darwin.so
1.1M	./venv/lib/python3.11/site-packages/pandas/_libs/interval.cpython-311-darwin.so
1.0M	./venv/lib/python3.11/site-packages/pyarrow/_flight.cpython-311-darwin.so
1.0M	./venv/lib/python3.11/site-packages/pyarrow/_compute.cpython-311-darwin.so
(venv) databricks-sql-connector $ echo "Direct dependencies $(cat venv/lib/python3.11/site-packages/databricks_sql_connector-2.9.3.dist-info/METADATA | grep Requires-Dist | wc -l)"
Direct dependencies       14
```

## Databricks Labs LightSQL

Compressed:

```shell
$ cd $(mktemp -d) && pip3 wheel databricks-sdk && echo "All wheels $(du -hs)" && echo "1Mb+ wheels: $(find . -type f -size +1M | xargs du -h | sort -h -r)" && cd -
All wheels:
816K

1Mb+ wheels:
0
```

Uncompressed:

```shell
databricks-sdk $ python3 -m venv venv
databricks-sdk $ source ./venv/bin/activate
(venv) databricks-labs-lsql $  pip install databricks-sdk
...
Successfully installed certifi-2023.7.22 charset-normalizer-3.3.0 databricks-sdk-0.10.0 idna-3.4 requests-2.31.0 
urllib3-2.0.6
(venv) databricks-labs-lsql $  du -hs .
 30M	.
(venv) databricks-labs-lsql $  du -ha . | sort -h -r | head
 30M	./venv/lib/python3.11/site-packages
 30M	./venv/lib/python3.11
 30M	./venv/lib
 30M	./venv
 30M	.
 16M	./venv/lib/python3.11/site-packages/pip
 13M	./venv/lib/python3.11/site-packages/pip/_vendor
5.2M	./venv/lib/python3.11/site-packages/setuptools
4.4M	./venv/lib/python3.11/site-packages/databricks/sdk
4.4M	./venv/lib/python3.11/site-packages/databricks
```
