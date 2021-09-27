"""
MF models on AWS scraping CLI
using boto3 (anonymous)
See doc https://mf-models-on-aws.org/en/doc/datasets/v2/
"""
import os
import requests
from pathlib import Path
from typing import Optional
import typer
import datetime
from joblib import Parallel, delayed
import boto3
from botocore.handlers import disable_signing


BUCKET = "mf-nwp-models"
app = typer.Typer()


def download(fkey, flatten):
    """
    Given a S3 key, donwload it from our bucket
    We disable signing to stay anonymous
    """
    s3 = boto3.resource("s3")
    s3.meta.client.meta.events.register("choose-signer.s3.*", disable_signing)
    bucket = s3.Bucket(f"{BUCKET}")
    os.makedirs(fkey.rsplit("/", 1)[0], exist_ok=True)
    print(f"Downloading {fkey}")
    bucket.download_file(fkey, fkey if not flatten else fkey.replace("/", "_"))


@app.command()
def s3download(
    model: str,
    run_date: datetime.datetime,
    flatten: bool = typer.Option(False, help="save all files in working dir"),
):
    """
    Download all the files of a given nwp model
    for a given run_date
    """

    typer.echo(f"Downloading model {model} for date {run_date}")
    prefix = f"{model}/v2/{run_date.strftime('%Y-%m-%d')}/"
    typer.echo(f"Prefix is {prefix}")
    s3 = boto3.resource("s3")
    s3.meta.client.meta.events.register("choose-signer.s3.*", disable_signing)
    bucket = s3.Bucket(f"{BUCKET}")
    Parallel(n_jobs=8, verbose=10)(
        delayed(download)(f.key, flatten) for f in bucket.objects.filter(Prefix=prefix)
    )


def upload_one(s3_host: str, bucket_name: str, filepath: Path, dst_name: str):
    """
    Uploads one file to the designated s3 storage into bucket.
    """
    s3 = boto3.resource("s3", endpoint_url=s3_host)
    s3.meta.client.meta.events.register("choose-signer.s3.*", disable_signing)
    bucket = s3.Bucket(f"{bucket_name}")
    print(f"Uploading {filepath}")
    bucket.upload_file(str(filepath), dst_name)


@app.command()
def s3upload(
    s3_host: str,
    bucket_name: str,
    glob_pattern: str = typer.Option("*.grib2", help="Glob pattern for upload"),
    incremental_names: bool = typer.Option(
        False,
        help="rename files incrementaly while keeping suffix: 1.grib2, 2.grib2, ...",
    ),
):
    """
    Uploads all the file matching glob pattern
    to the designated s3 host in given bucket.
    Uses anonymous upload so bucket must be public.
    """

    Parallel(n_jobs=8, verbose=10)(
        delayed(upload_one)(
            s3_host,
            bucket_name,
            filepath,
            f"{i}{filepath.suffix}" if incremental_names else str(filepath.name),
        )
        for i, filepath in enumerate(Path(".").glob(glob_pattern))
    )


def upload_one_dav(endpoint: str, filepath: Path, dst_name: str):
    """
    Uploads one file to the designated webdav endpoint
    """
    requests.put(f"{endpoint}/{dst_name}", open(filepath, "rb"))


@app.command()
def webdavupload(
    host: str,
    prefix: str,
    glob_pattern: str = typer.Option("*.grib2", help="Glob pattern for upload"),
    incremental_names: bool = typer.Option(
        False,
        help="rename files incrementaly while keeping suffix: 1.grib2, 2.grib2, ...",
    ),
):
    """
    Uploads all the file matching glob pattern
    to the designated webdav server host using PUT requests
    """

    Parallel(n_jobs=8, verbose=10)(
        delayed(upload_one_dav)(
            f"{host}/{prefix}",
            filepath,
            f"{i}{filepath.suffix}" if incremental_names else str(filepath.name),
        )
        for i, filepath in enumerate(Path(".").glob(glob_pattern))
    )


if __name__ == "__main__":
    app()
