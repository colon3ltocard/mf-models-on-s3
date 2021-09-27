# a CLI to download MF models available on AWS open-data

See the official documentation for available models : https://mf-models-on-aws.org/en/doc/datasets/v2/

Usage.
If you want to preserve the prefix layout as a directory with subdir structure.
```
pip install -r requirements.txt
python main.py s3download arome-france-hd 2021-08-03
```

Note, on corporate machines behind a mitm SSL proxy you need to provide the corporate
root certificate using env var otherwise boto3
will fail with ssl errors:
```
export AWS_CA_BUNDLE=/home/frank/mf.crt
```

If you want to flatten all files in the current working directory.

```
python main.py s3download arome-france-hd 2021-08-03 --flatten
```

This will yield flatten files:

```
arome-france-hd_v2_2021-08-03_00_BRTMP_isobaric_0h.grib2.inv
arome-france-hd_v2_2021-08-03_00_BRTMP_isobaric_10h.grib2.inv
arome-france-hd_v2_2021-08-03_00_BRTMP_isobaric_11h.grib2.inv
arome-france-hd_v2_2021-08-03_00_BRTMP_isobaric_12h.grib2.inv
arome-france-hd_v2_2021-08-03_00_BRTMP_isobaric_13h.grib2.inv
arome-france-hd_v2_2021-08-03_00_BRTMP_isobaric_14h.grib2.inv
arome-france-hd_v2_2021-08-03_00_BRTMP_isobaric_15h.grib2.inv
```

# Upload to a local minio instance

The cli also features a s3upload subcommand.

First download some data.

```
python main.py s3download --flatten arome-france-hd 2021-08-05
```

Now, upload it to the local minio instance into pubbuck

```
python main.py s3upload http://localhost:9000 pubbuck
```

To force static names use the *--incremental-names* cli option

```
python main.py s3upload --incremental-names http://localhost:9000 pubbuck
```

# Upload to local webdav instance 

```
python main.py webdavupload --incremental-names http://localhost:32080 pubbuck
```
