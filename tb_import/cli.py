import typer
import requests
import json
import pandas as pd
from pathlib import Path
from typing import List
import time
from types import SimpleNamespace

app = typer.Typer()

options = {
    "entry_per_batch": 10,
    "time_per_batch_ms": 100,
    "verbose": False,
}

multi_dict = {
    'data': 'M_MeasurementValue',
    'key': 'M_MediumID',
    'timestamp': 'Unixtimestamp',
    'access_token': 'M_DevEUI',
}

multi_keys = SimpleNamespace(**multi_dict)


@app.command()
def csv(
    csvfile: Path = typer.Argument(..., help="Path to the csv file to import the data from"),
    access_token: str = typer.Option(..., envvar="ACCESS_TOKEN", help="The thingsboard access token of the device to import the data to"),
    baseurl: str = typer.Option("https://tesenso.io", help="The thingsboard baseurl with port"),
    separator: str = typer.Option(";", help="The separator of the csv file"),
    keys: List[str] = typer.Option(None, help="The keys to be imported as a timeseries. None means all"),
    unixtime: str = typer.Option("Unixtimestamp", help="The key to use as the unixtimestamp"),
    ms: bool = typer.Option(False, help="If the unixtimestamp should be interpreted as milliseconds"),
):
    """
    Add data from a csv file to a single device.
    Expects the csv file to have a header in the first row.
    """
    # Get basic variables
    url = baseurl.strip("/") + "/api/v1/" + access_token + "/telemetry"
    headers = {"Content-Type": "application/json"}
    df = pd.read_csv(csvfile, sep=separator)

    if options["verbose"]:
        typer.echo(f"Read csv file: {df}")

    # Filter unneeded keys
    if len(keys) != 0:
        drop_keys = [key for key in df.keys() if key not in keys and key != unixtime]
        df.drop(drop_keys, axis=1, inplace=True)
        if options["verbose"]:
            typer.echo(f"Drop keys: {drop_keys}]")

    # Fix the timestamp to milliseconds
    if not ms:
        if options["verbose"]:
            typer.echo(f"Adjust timestamp to milliseconds")
        df[unixtime] = df[unixtime].apply(lambda x: x*1000)

    if options["verbose"]:
        typer.echo(f"Used data: {df}")

    data_keys = [key for key in df.keys() if key != unixtime]
    typer.echo(f"Uploading data from {csvfile} to {url} with headers {headers} and keys {data_keys}")

    # loop through dataframe and upload once the size is reached
    count = 0
    list = []
    for index, row in df.iterrows():
        datapoint = {"ts": int(row[unixtime]), "values": row.drop(unixtime).to_dict()}
        list.append(datapoint)
        count = count + 1
        if count >= options["entry_per_batch"]:
            if options["verbose"]:
                typer.echo(f"Upload data: {list}")
            r = requests.post(url=url, headers=headers, data=json.dumps(list))
            if options["verbose"]:
                typer.echo(f"Status code: {r.status_code}")
            count = 0
            list = []
            time.sleep(options["time_per_batch_ms"]/1000)

    # upload last entries
    if len(list) > 0:
        if options["verbose"]:
            typer.echo(f"Upload data: {list}")
        r = requests.post(url=url, headers=headers, data=json.dumps(list))
        if options["verbose"]:
            typer.echo(f"Status code: {r.status_code}")

@app.command()
def multi(
    csvfile: Path = typer.Argument(..., help="Path to the csv file to import the data from"),
    baseurl: str = typer.Option("https://tesenso.io", help="The thingsboard baseurl with port"),
    separator: str = typer.Option(";", help="The separator of the csv file"),
):
    f"""
    Add data from a csv file to a single device.
    Expects the csv file to have a header in the first row.
    The csv file needs the following keys:
        {multi_keys.data}: The actual value to be imported into thingsboard
        {multi_keys.key}: The name of the telemetry key
        {multi_keys.timestamp}: A unix timestamp in seconds
        {multi_keys.access_token}: The thingsboard access token of the device
    """
    # Get basic variables
    headers = {"Content-Type": "application/json"}

    try:
        df = pd.read_csv(csvfile, sep=separator)
    except FileNotFoundError:
        typer.echo(f"Could not find csv file {csvfile}!")
        raise typer.Exit(1)

    if options["verbose"]:
        typer.echo(f"Read csv file: \n{df}")

    needed_keys = [multi_dict[key] for key in multi_dict.keys()]
    missing_keys = [key for key in needed_keys if key not in df.keys()]

    if len(missing_keys) > 0:
        typer.echo(f"Keys {missing_keys} are missing in {csvfile}!")
        raise typer.Exit(1)

    # separate dataframe into separate access_tokens
    tokens = df[multi_keys.access_token].unique()

    # loop over all tokens, filter dataframe and create payload with data
    for token in tokens:
        url = baseurl.strip("/") + "/api/v1/" + token + "/telemetry"
        data = df[df[multi_keys.access_token] == token]

        list = []
        count = 0
        for index, row in data.iterrows():
            typer.echo(row)
            datapoint = {"ts": int(row[multi_keys.timestamp])*1000, "values": {row[multi_keys.key]: row[multi_keys.data]}}
            list.append(datapoint)
            count = count + 1
            if count >= options["entry_per_batch"]:
                if options["verbose"]:
                    typer.echo(f"Upload data: {list}")
                r = requests.post(url=url, headers=headers, data=json.dumps(list))
                if options["verbose"]:
                    typer.echo(f"Status code: {r.status_code}")
                count = 0
                list = []
                time.sleep(options["time_per_batch_ms"]/1000)

        # upload last entries
        if len(list) > 0:
            if options["verbose"]:
                typer.echo(f"Upload data: {list}")
            r = requests.post(url=url, headers=headers, data=json.dumps(list))
            if options["verbose"]:
                typer.echo(f"Status code: {r.status_code}")

@app.callback()
def global_options(
    entry_per_batch: int = typer.Option(100, help="Amount of datapoints written in a single request"),
    time_per_batch_ms: int = typer.Option(100, help="Time to wait between requests in milliseconds"),
    verbose: bool = typer.Option(False, help="Print verbose output"),
):
    """
    A cli tool to import external data into thingsboard
    """
    options["entry_per_batch"] = entry_per_batch
    options["time_per_batch_ms"] = time_per_batch_ms
    options["verbose"] = verbose


def main():
    app()


if __name__ == "__main__":
    main()
